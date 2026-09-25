import asyncio
import logging
import os
import pathlib

from .models import Graph
from .plugin_manager import PluginManager
from .port_schema import edge_warning, validate_edge
from .registry import build_node_registry
from .blob_store import ensure_cleanup_task
from .media import MediaItem
from .stream import DROP_POLICIES, Pipe

logger = logging.getLogger("pystreamflow.engine")

# Scalar types that are meaningful straight out of a node's own config
# JSON (str/int/float/bool) - deliberately excludes dict/list, so an
# actually-structured 'value' (e.g. a nested object some node type might
# legitimately emit) is left as-is rather than half-unwrapped.
_ATTRIBUTE_SCALAR_TYPES = (str, int, float, bool)


def _clean_attribute_value(item):
    """Reduce a typical node-emitted item down to the single raw scalar
    an attribute wire is meant to deliver (e.g. a bare filepath string),
    instead of the metadata-wrapped dict most existing source nodes
    actually emit.

    This closes the gap flagged in the project's evaluation doc (finding
    3d.5): ``ListStringsNode``/``GeneratorInputNode`` emit
    ``{'value': ..., 'index': ...}``, not a bare value, so wiring one of
    those into e.g. ``FileInputNode.path`` via an ``attribute`` edge used
    to set that attribute to the whole dict rather than the string a
    workflow author actually wanted there. ``'value'`` is the de facto
    "primary payload" key across this node library (see the node types
    named above), so a dict carrying one is unwrapped to just that value
    for attribute edges specifically - other edge types (``data``,
    ``control``, ``endpoint``) are untouched and still deliver the full
    item, since a downstream node's own process() loop may legitimately
    want the metadata (e.g. the ``index``) that an attribute target does
    not.

    Only unwraps the single unambiguous case - a dict with a ``'value'``
    key holding a plain scalar. Anything else (no ``'value'`` key, a
    non-scalar value, a list, an already-bare scalar) passes through
    unchanged rather than guessing at a shape that isn't there.
    """
    if isinstance(item, MediaItem):
        # A media item is already the bare value - never unwrap or copy it
        # (media plan phase 1.3).
        return item
    if isinstance(item, dict) and isinstance(item.get('value'), _ATTRIBUTE_SCALAR_TYPES):
        return item['value']
    return item


def media_edge_maxsize() -> int:
    """Default queue bound for edges leaving a media output port
    (``PSF_MEDIA_EDGE_MAXSIZE``, default 8)."""
    return int(os.environ.get('PSF_MEDIA_EDGE_MAXSIZE', '8'))


def validate_buffer(buffer) -> str | None:
    """Check an edge's optional ``buffer`` config; returns an error
    message, or ``None`` if it's fine."""
    if buffer is None:
        return None
    if not isinstance(buffer, dict):
        return "buffer must be a mapping like {maxsize: 8, drop_policy: drop}"
    unknown = set(buffer) - {'maxsize', 'drop_policy'}
    if unknown:
        return f"unknown buffer setting(s): {', '.join(sorted(unknown))}"
    maxsize = buffer.get('maxsize', 0)
    if isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize < 0:
        return "buffer.maxsize must be a non-negative integer (0 = unbounded)"
    policy = buffer.get('drop_policy', 'block')
    if policy not in DROP_POLICIES:
        return f"buffer.drop_policy must be one of {', '.join(DROP_POLICIES)}"
    return None


def edge_pipe(src_node, source_port: str, buffer: dict | None = None) -> Pipe:
    """Build the Pipe for one edge (media plan phase 1.5).

    An explicit ``buffer`` from the workflow wins. Otherwise an edge
    leaving one of the source node class's ``MEDIA_OUTPUT_PORTS`` gets a
    bounded queue (``media_edge_maxsize()``) with that port's declared
    drop policy, so a slow consumer can't make a stream of video frames
    grow memory without limit. Every other edge stays unbounded, exactly
    as before.
    """
    if buffer:
        return Pipe(maxsize=int(buffer.get('maxsize', 0)), drop_policy=buffer.get('drop_policy', 'block'))
    policy = getattr(type(src_node), 'MEDIA_OUTPUT_PORTS', {}).get(source_port)
    if policy:
        return Pipe(maxsize=media_edge_maxsize(), drop_policy=policy)
    return Pipe()


async def _pump_attribute(pipe, target_node, attr_name):
    """Feed every value received on an 'attribute'-type edge straight into
    the target node's named config attribute (see BaseNode.set_attribute())
    for as long as the caller keeps this task alive.

    Deliberately a plain module-level function, not an Engine method - it
    never touched `self` in the first place (only `pipe`/`target_node`/
    `attr_name`), so it's reused as-is by api/server.py's live `POST
    /nodes/connect` endpoint for an ad-hoc attribute wire drawn directly on
    the canvas between two already-running nodes, not just by a full
    Engine-run Session's `_wire_edges()` below. Before that reuse existed,
    an ad-hoc attribute edge's pipe was wired with a plain `add_input()`
    (see server.py's connect_nodes()) - landing in `target_node.inputs`
    under an arbitrary attribute name that nothing in the target's own
    process() loop ever reads - so it silently did nothing at all until
    "Run" rebuilt the graph as a real Session, which is exactly this whole
    class of bug (see the "attribute changes including wiring changes"
    feedback this closes, alongside the wire-connect-only-at-Run fix in
    editor.js's syncWireToBackend()).
    """
    try:
        while True:
            value = await pipe.get()
            target_node.set_attribute(attr_name, _clean_attribute_value(value))
    except asyncio.CancelledError:
        pass


# Tracks every live, ad-hoc attribute-edge pump task started outside a full
# Engine-run Session - originally api/server.py's own module-level state
# (its POST /nodes/connect endpoint's direct counterpart of
# Engine._wire_edges()'s self._attribute_tasks list), moved here so it can
# be shared with mcp/server.py's connect_nodes/disconnect_nodes tools too.
# api/server.py and mcp/server.py run in the same process against the same
# shared `_nodes` registry (core/web_server.py) - api/server.py even mounts
# mcp/server.py's whole FastAPI app - so an attribute edge wired through
# one surface must be visible to (and cleaned up by) the other: a node
# created via the HTTP API but wired/deleted through the MCP tools, or vice
# versa, needs the *same* pump-task bookkeeping either way, not two
# independent dicts that can each leak the other's tasks. Keyed on the same
# (source_id, target_id, target_port) triple both surfaces already use to
# name an attribute edge on connect and disconnect.
_attribute_pump_tasks: dict[tuple[str, str, str], asyncio.Task] = {}


def _cancel_attribute_pump(key: tuple[str, str, str]) -> None:
    task = _attribute_pump_tasks.pop(key, None)
    if task is not None:
        task.cancel()


# Phase 1 of the wire-kind-unification design (see
# claude/design_unified_wire_kinds_plan.md): tracks the exact Pipe object
# each ad-hoc edge - one wired via POST/DELETE /nodes/connect (HTTP) or the
# connect_nodes/disconnect_nodes MCP tools, not a full Engine-run Session -
# added to its source node's output. Now that BaseNode.outputs can hold
# several live consumer pipes under one port name (see core/node.py's
# add_output()/remove_output()), disconnecting one specific edge needs to
# know *which* pipe it added so it can remove only that one and leave every
# other consumer of the same output port alone - `del src.outputs[name]`
# would otherwise silently kill every other edge fanned out from that same
# port too. Shared between api/server.py and mcp/server.py for the same
# reason _attribute_pump_tasks above is: both surfaces run in the same
# process against the same shared `_nodes` registry, so an edge wired
# through one must be disconnectable through the other. Keyed on the same
# 4-tuple both surfaces already use to name an edge on connect/disconnect -
# deliberately not including `type`, matching _attribute_pump_tasks' own
# 3-tuple key: within a single edge-kind system, one
# (source_id, source_port, target_id, target_port) combination names at
# most one live ad-hoc edge at a time.
_edge_pipes: dict[tuple[str, str, str, str], Pipe] = {}


class Engine:
    def __init__(self, graph: Graph, session_id: str | None = None):
        self.graph = graph
        # Stamped onto every node this Engine instantiates (see
        # _instantiate_nodes() below) so core/web_server.py's node
        # registry can tell which Session owns a given node id instead of
        # silently colliding when two Sessions' workflows share one - see
        # register_node()'s docstring in web_server.py for the bug this
        # is part of fixing. None for an Engine built without a Session
        # (matches every existing direct-construction call site/test).
        self.session_id = session_id
        self.node_instances: dict[str, object] = {}
        self.pipes: dict[tuple, object] = {}
        self.plugin_manager = PluginManager(pathlib.Path(__file__).parent.parent / 'plugins')
        self.plugin_manager.load_plugins()
        self._reported_errors: set[str] = set()
        self._attribute_tasks: list[asyncio.Task] = []

    def validate(self):
        # Check all nodes referenced in edges exist
        node_ids = {n.id for n in self.graph.nodes}
        node_types = {n.id: n.type for n in self.graph.nodes}
        # A known node type whose optional dependency isn't installed
        # (media plan phase 3 - e.g. the image nodes without Pillow) must
        # fail loudly here, not fall back to _instantiate_nodes()' silent
        # no-op node.
        from ..nodes import UNAVAILABLE_NODE_TYPES
        for n in self.graph.nodes:
            if n.type in UNAVAILABLE_NODE_TYPES:
                raise ValueError(f"Node {n.id}: {n.type} is not available - {UNAVAILABLE_NODE_TYPES[n.type]}")
        for e in self.graph.edges:
            if e.source not in node_ids:
                raise ValueError(f"Edge source {e.source} not found")
            if e.target not in node_ids:
                raise ValueError(f"Edge target {e.target} not found")
            # Port/edge-type validation lives here (not just in the HTTP
            # API's /workflows and /nodes/connect endpoints) so a workflow
            # YAML loaded directly - via the CLI, MCP server, or a Session
            # - gets the same protection as one saved through the node
            # editor. Before this, a wire whose port didn't match anything
            # a node actually read from (see core/port_schema.py's module
            # docstring) would wire up silently and just never carry any
            # data - the exact "looks connected, does nothing" bug this
            # closes.
            err = validate_edge(
                node_types[e.source], e.source_port, node_types[e.target], e.target_port, e.type,
            ) or validate_buffer(getattr(e, 'buffer', None))
            if err:
                raise ValueError(f"Invalid edge {e.source}->{e.target}: {err}")
            # Port-dtype mismatch (media plan phase 6b): advisory only.
            warning = edge_warning(node_types[e.source], e.source_port, node_types[e.target], e.target_port, e.type)
            if warning:
                logger.warning("workflow edge %s->%s: %s", e.source, e.target, warning)
        # Check for cycles via Kahn's algorithm
        in_degree = {n.id: 0 for n in self.graph.nodes}
        adj = {n.id: [] for n in self.graph.nodes}
        for e in self.graph.edges:
            adj[e.source].append(e.target)
            in_degree[e.target] += 1
        queue = [nid for nid,deg in in_degree.items() if deg==0]
        visited = 0
        while queue:
            nid = queue.pop()
            visited += 1
            for nb in adj[nid]:
                in_degree[nb] -= 1
                if in_degree[nb]==0:
                    queue.append(nb)
        if visited != len(self.graph.nodes):
            raise ValueError("Graph contains cycle")
        return True

    def _topological_order(self):
        in_degree = {n.id: 0 for n in self.graph.nodes}
        adj = {n.id: [] for n in self.graph.nodes}
        for e in self.graph.edges:
            adj[e.source].append(e.target)
            in_degree[e.target] += 1
        queue = [nid for nid,deg in in_degree.items() if deg==0]
        order = []
        while queue:
            nid = queue.pop(0)
            order.append(nid)
            for nb in adj[nid]:
                in_degree[nb] -= 1
                if in_degree[nb]==0:
                    queue.append(nb)
        return order

    def _instantiate_nodes(self):
        registry = build_node_registry()
        from ..core.web_server import register_node
        for node in self.graph.nodes:
            cls = registry.get(node.type) or self.plugin_manager.get_node_class(node.type)
            if not cls:
                from ..core.node import BaseNode
                logger.warning(
                    "unknown node type %r for node %r - falling back to a no-op node",
                    node.type, node.id,
                )
                class GenericNode(BaseNode):
                    async def process(self): pass
                cls = GenericNode
            inst = cls(node.id, node.config)
            # Stamp ownership before this Engine's own register_node()
            # call below, and before start()/init() run (which is where
            # a handful of node types - ScriptInputNode, WebOutputNode,
            # WebOutputJSONNode, LMStudioNode, WebInputNode,
            # UserInputNode - call register_node(self) a second time on
            # their own) - so every registration of this instance, from
            # whichever call site, is correctly attributed to this
            # session_id, not left as the default None.
            inst.session_id = self.session_id
            self.node_instances[node.id] = inst
            register_node(inst)

    def _wire_edges(self):
        for e in self.graph.edges:
            src_node = self.node_instances[e.source]
            tgt_node = self.node_instances[e.target]
            # One pipe per edge. This used to be keyed by the node pair
            # (source, target) alone, so two edges between the same two
            # nodes - e.g. VideoDecodeNode's out->in and audio->audio into
            # one VideoEncodeNode, or LMStudioNode's results and stats into
            # one sink - silently shared a single pipe: the target's two
            # input ports both read from it and items landed on whichever
            # port happened to read first.
            key = (e.source, e.source_port, e.target, e.target_port, e.type)
            if key not in self.pipes:
                self.pipes[key] = edge_pipe(src_node, e.source_port, getattr(e, 'buffer', None))
            pipe = self.pipes[key]
            # Phase 2 of the wire-kind-unification design: a 'raw' edge
            # wires exactly like a 'data' edge (same target-side add_input()
            # below) - the only difference is the delivery kind recorded on
            # the source's consumer list, which is what makes emit() run
            # the item through _clean_attribute_value() for this consumer
            # instead of delivering it as-is.
            src_node.add_output(e.source_port, pipe, kind='raw' if e.type == 'raw' else 'data')
            if e.type == 'attribute':
                # An attribute edge maps an upstream output directly onto
                # a named config attribute of the target node - e.g.
                # wiring a small settings node's output into a
                # FileInputNode's `path` - rather than into one of the
                # target's normal self.inputs pipes, which nothing in the
                # target's own process() loop would know to read under an
                # arbitrary attribute name. A dedicated background task
                # pumps values from the pipe straight into
                # BaseNode.set_attribute() instead of add_input().
                self._attribute_tasks.append(
                    asyncio.create_task(_pump_attribute(pipe, tgt_node, e.target_port)),
                )
                continue
            if e.type == 'control':
                # Give the source node a real, graph-derived target list
                # instead of trigger nodes only ever knowing about a
                # hand-typed `target_node_id` config string - see
                # core/trigger_targets.py's TriggerActionMixin, which
                # reads this and prefers it over that config field.
                src_node._graph_control_targets.append(e.target)
                # Always land a control edge's pipe on the target's single
                # reserved 'control' input (see BaseNode.add_input()'s
                # `name == 'control'` special case), never on
                # e.target_port. Regression fix: this used to call
                # `tgt_node.add_input(e.target_port, pipe)` just like a
                # data edge - but before the node editor grew a dedicated
                # control slot, the only port a UI-drawn control wire could
                # land on was one of the target's ordinary *data* ports
                # (typically 'in', the only slot most nodes had at all),
                # and add_input() does an unconditional `self.inputs[name]
                # = ...` dict assignment. So wiring a trigger onto a node's
                # 'in' port silently replaced that port's real data pipe
                # with the trigger's own output pipe - the target's
                # process() loop would then read whatever the trigger
                # emitted instead of its real upstream data, or (if the
                # data edge was wired second) the control wiring would
                # itself get clobbered right back - while a pure source
                # node with zero declared input ports (LogInputNode,
                # GeneratorInputNode, MQTTInputNode, TimerNode, ...) had no
                # input port at all a control wire could legally target in
                # the first place (see port_schema.py's `[]` input-port
                # overrides for that whole family). Landing every control
                # edge on the reserved 'control' name instead - which
                # every BaseNode subclass supports regardless of its
                # declared data ports, and which nothing else ever writes
                # to - fixes both: no data port is ever at risk, and a
                # control edge can target any node type. See
                # port_schema.py's validate_edge() for the matching
                # validation-side change (a control edge's target_port is
                # no longer checked against the target type's declared
                # input ports, the same way an attribute edge's isn't).
                tgt_node.add_input('control', pipe)
                continue
            tgt_node.add_input(e.target_port, pipe)

    async def run(self):
        self.validate()
        self._instantiate_nodes()
        self._wire_edges()
        # Expire/evict media blobs in the background while anything runs
        # (media plan phase 1.2). Shared by every Engine in the process.
        ensure_cleanup_task()

        # Start every node, in topological order, up front.
        #
        # This used to only explicitly start "source-like" nodes (no
        # inputs, or auto_start disabled) and rely on each downstream
        # node's wrapped input pipe to auto-start it the first time
        # something called .get() on it. That never actually happens for
        # any node with an input: the only code that ever reads a node's
        # own `self.inputs[...]` is that node's own process() loop - which
        # can't run until the node has started. So in practice, every node
        # with at least one input (i.e. everything except true sources)
        # never started at all when run through the engine; a pipeline of
        # more than one node was silently dead past its first node. Each
        # node's process() loop already polls its inputs with a timeout
        # and idles safely with nothing connected/nothing arrived yet, so
        # it's safe - and is what actually makes multi-node graphs work -
        # to just start all of them.
        # BaseNode.start() itself now absorbs an init() failure for its own
        # node (records it as that node's error/health state and returns
        # None instead of raising - see its own docstring) rather than
        # raising out of this loop, so one misconfigured node can no
        # longer abort every other node's startup here. This loop has no
        # try/except of its own precisely because it shouldn't need one
        # any more for that case; anything that still does escape
        # `start()` is a genuinely unexpected failure, and the `except
        # Exception` below (not just CancelledError) exists to make sure
        # even that doesn't leave already-started nodes running as
        # orphaned, unsupervised tasks.
        order = self._topological_order()
        for nid in order:
            await self.node_instances[nid].start()

        try:
            await self._supervise()
        except asyncio.CancelledError:
            await self.stop_all()
            raise
        except Exception:
            # Defense in depth alongside BaseNode.start()'s own init()
            # handling above: whatever the reason _supervise() itself
            # exits with an unexpected exception, every node this Engine
            # already started must still be stopped here - otherwise they
            # keep running forever as orphaned, no-longer-supervised
            # tasks, known to nothing (Session._run_loop()'s own except
            # block marks the session "error" and moves on, but never
            # cleaned up what's still running underneath it).
            logger.exception("Engine._supervise() raised an unhandled error; stopping all nodes")
            await self.stop_all()
            raise

    async def _supervise(self):
        """Run until cancelled, surfacing node task failures instead of
        letting them vanish as "Task exception was never retrieved"."""
        while True:
            await asyncio.sleep(1.0)
            for nid, inst in self.node_instances.items():
                task = getattr(inst, "_task", None)
                if task is not None and task.done() and not task.cancelled():
                    exc = task.exception()
                    if exc is not None and nid not in self._reported_errors:
                        self._reported_errors.add(nid)
                        logger.error(
                            "node %s (%s) stopped with an unhandled error: %s",
                            nid, type(inst).__name__, exc, exc_info=exc,
                        )

    async def stop_all(self):
        """Stop every node instantiated by this engine (used on shutdown)."""
        for t in self._attribute_tasks:
            t.cancel()
        if self._attribute_tasks:
            await asyncio.gather(*self._attribute_tasks, return_exceptions=True)
        await asyncio.gather(
            *(inst.stop() for inst in self.node_instances.values()),
            return_exceptions=True,
        )

    async def pause_all(self):
        """Pause every node instantiated by this engine.

        Unlike stop_all(), this does not touch node_instances/pipes or the
        run()/_supervise() task at all - it just suspends each node's
        process() loop via BaseNode.pause() (see core/node.py), preserving
        the node objects and their internal state so resume_all() can bring
        the *same* graph back to life instead of Session having to tear
        down and re-run Engine.run() from scratch (which would call
        _instantiate_nodes() again and silently lose all state - the exact
        "pause restarts instead of suspending" bug this Phase 1 pass fixed
        at the node level; Session had the identical bug one layer up).
        """
        await asyncio.gather(
            *(inst.pause() for inst in self.node_instances.values()),
            return_exceptions=True,
        )

    async def resume_all(self):
        """Resume every node paused by pause_all()."""
        await asyncio.gather(
            *(inst.resume() for inst in self.node_instances.values()),
            return_exceptions=True,
        )
