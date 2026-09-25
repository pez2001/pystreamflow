"""Per-node-type port schema: the real input/output port names each node
type uses, as a single declared source of truth.

Why this exists
----------------
`pystreamflow/api/ui.html`'s node editor assumes every node has a uniform,
numbered set of ports (``in0``, ``in1``, ... / ``out0``, ``out1``, ...) and
always sends those names when you drag a wire (see ``window.onmouseup``,
which hardcodes ``target_port = 'in0'``). But no node implementation in
``pystreamflow/nodes/`` actually keys its ``self.inputs``/``self.outputs``
dicts that way - they use plain names like ``'in'``/``'out'``, or several
distinct named ports (``RollingWindowBufferNode`` has ``in``, ``retain``,
``trigger`` inputs and ``history``, ``out`` outputs), or accept any name at
all (``AndNode``, ``MergeNode``, and the trigger nodes all just iterate
``self.inputs.values()`` - they don't care what a wired input pipe is
called). A wire drawn through the current UI therefore almost never lands
on the port name a node actually reads from - it's stored under a key
nothing looks up, so the connection is silently inert. This module is the
fix's foundation: one declared mapping of type name -> real port names,
so the backend can validate a wire against what a node type actually
supports (Phase 2) and the editor can eventually offer/label the right
ports instead of a fixed ``inN``/``outN`` scheme (Phase 3).

How it was derived
-------------------
By scanning every node class's own source for literal
``self.inputs.get('name')`` / ``self.inputs['name']`` / ``self.emit('name', ...)``
usage (concrete port names) and for ``self.inputs``/``self.outputs``
iteration over ``.values()``/``.items()``/keys (the "accepts/produces any
name" signal), then reviewing the result - the same "derive it once,
don't hand-maintain yet another copy" principle Phase 1 used for the node
type registry (``core/registry.py``). Nodes not listed in ``_OVERRIDES``
use ``DEFAULT_INPUT_PORTS``/``DEFAULT_OUTPUT_PORTS``, which is correct for
the large majority (over half) of node types - a single ``'in'`` input and
a single ``'out'`` output.
"""
from __future__ import annotations

# Sentinel meaning "accepts/produces any number of arbitrarily-named
# ports" - e.g. AndNode takes one pipe per wired input regardless of its
# port name, and ForkNode's outputs are created on the fly, one per wired
# edge, rather than from a fixed set declared up front.
DYNAMIC = "*"

DEFAULT_INPUT_PORTS: list[str] = ["in"]
DEFAULT_OUTPUT_PORTS: list[str] = ["out"]

PortList = "list[str] | str"  # list of names, or the DYNAMIC sentinel

# type name -> (input ports, output ports). Only entries that differ from
# the defaults above need to be listed here.
_OVERRIDES: dict[str, tuple[object, object]] = {
    # --- Pure sources: nothing reads self.inputs at all. ---
    "ConstantValueNode": ([], DEFAULT_OUTPUT_PORTS),
    "FileInputNode": ([], DEFAULT_OUTPUT_PORTS),
    # Media plan phase 2: reads one whole file per item, pure source.
    "MediaFileInputNode": ([], DEFAULT_OUTPUT_PORTS),
    # DirectoryInputNode: a pure source like FileInputNode, but with two
    # distinct, fixed output ports instead of one - discovered files and
    # discovered subdirectories are kept on separate ports (`files`/
    # `dirs`) rather than one mixed stream, so a downstream node can wire
    # to just the kind it cares about.
    "DirectoryInputNode": ([], ["files", "dirs"]),
    "GeneratorInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "JSONInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "ListStringsNode": ([], DEFAULT_OUTPUT_PORTS),
    "LogInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "MQTTInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "ProcessInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "PythonScriptInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "ScriptInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "ShellInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "SocketInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "TimerNode": ([], DEFAULT_OUTPUT_PORTS),
    "TimerTriggerNode": ([], DEFAULT_OUTPUT_PORTS),
    "TriggerPulseNode": ([], DEFAULT_OUTPUT_PORTS),
    "UrlInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "UserInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "WebInputNode": ([], DEFAULT_OUTPUT_PORTS),
    "ApiInputNode": ([], DEFAULT_OUTPUT_PORTS),
    # ApiOutputNode has a real 'in'/'out' pass-through pair like the
    # default schema, plus a second, distinct 'raw' output port - the
    # graph-level (wireable-on-canvas) counterpart to its /api/<uri>/raw
    # HTTP route. See nodes/output_api.py's class docstring for why both
    # 'out' and 'raw' exist as separate ports even though they currently
    # carry identical items.
    "ApiOutputNode": (DEFAULT_INPUT_PORTS, ["out", "raw"]),
    # --- Accept an input pipe under any port name (they iterate
    # self.inputs.values()/.items() rather than looking up one by name).
    # This covers both true multi-input nodes (And/Or/Nand/Nor/Xor/Xnor,
    # MergeNode, StackNode/queues, TableNode) and nodes that just peek at
    # "whatever single input is wired, if any" as an optional control/reset
    # signal (ClockNode's reset_on_input, the *-target trigger nodes'
    # "wait for an incoming pulse" input). Either way, no specific port
    # name is required, so DYNAMIC is the correct (and only correct)
    # answer for the input side. ---
    "AndNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "Base64DecodeNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "Base64EncodeNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "ClockNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "DisplayNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "FIFOQueueNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "HTMLScraperNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "LIFOQueueNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "MergeNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "NandNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "NorNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "OrNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "StackNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TableNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerDebounceNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerIfNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerOffNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerOnNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerPauseNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerThresholdNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "TriggerToggleNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "XnorNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    "XorNode": (DYNAMIC, DEFAULT_OUTPUT_PORTS),
    # --- Sinks: no outputs. ---
    "FileOutputNode": (DEFAULT_INPUT_PORTS, []),
    "MQTTOutputNode": (DEFAULT_INPUT_PORTS, []),
    # --- Fixed input, dynamic (config/edge-driven) outputs. ---
    # ForkNode's outputs are created on the fly, one per wired edge, under
    # whatever port name the wire names (see core/node.py's add_output()
    # and api/server.py's /nodes/connect, which calls
    # `src.add_output(req.source_port, pipe)` with no restriction on the
    # name for a DYNAMIC-output type) - so DYNAMIC already permits any
    # name, including the editor's paired 'out0'/'raw0', 'out1'/'raw1', ...
    # scheme (api/static/editor.js's rebuildPorts()/psfPairedRawOutputs)
    # that gives every one of Fork's normal duplicated outputs a matching
    # raw counterpart. No backend change is needed for that pairing:
    # process() below already emits identically to every name present in
    # self.outputs, so a wired 'rawN' port receives the exact same
    # duplicated item a wired 'outN' port does, purely because the UI
    # wired it there - Fork itself doesn't (and doesn't need to)
    # distinguish the two by name.
    "ForkNode": (DEFAULT_INPUT_PORTS, DYNAMIC),
    # --- Nodes with several distinct, fixed, non-default port names. ---
    "RollingWindowBufferNode": (["in", "retain", "trigger"], ["history", "out"]),
    "UserPromptNode": (["user"], ["out", "prompt"]),
    # SubgraphNode's real input/output port names are entirely
    # config-driven (config['input_bridges']/['output_bridges'], or the
    # legacy single config['input_port']/['output_port']) rather than a
    # fixed set declared by the class - it can have any number of each,
    # under any names, depending on how its embedded workflow's bridges
    # are configured (see nodes/subgraph.py). DYNAMIC is the correct
    # declaration for the same reason it is for AndNode/MergeNode/etc.:
    # there's no fixed, type-level list to validate a wire's port name
    # against, so any name is accepted and the node editor renders it
    # with the same dynamic add/remove-port UI those get.
    "SubgraphNode": (DYNAMIC, DYNAMIC),
    # --- New node types (feature request: split-by-value, MCP client,
    # cron, sync barrier, queue gate, LED activity - see each node's own
    # module docstring in pystreamflow/nodes/ for full behavior). ---
    # SplitByValueNode: fixed 'in', but its outputs are a config-driven
    # (`values`) list of numbered ports plus an always-present 'default' -
    # DYNAMIC for the same reason SubgraphNode's config-driven bridge
    # ports are: no fixed, type-level list to validate a wire's port name
    # against.
    "SplitByValueNode": (DEFAULT_INPUT_PORTS, DYNAMIC),
    # CronNode: a pure source like ClockNode/TimerNode - nothing reads
    # self.inputs at all.
    "CronNode": ([], DEFAULT_OUTPUT_PORTS),
    # SyncBarrierNode/LedActivityNode: both have a variable, always-paired
    # number of inputs and outputs (inN <-> outN) - DYNAMIC on both sides,
    # like AndNode/MergeNode's inputs and ForkNode's outputs, just DYNAMIC
    # for both directions on the same node at once.
    "SyncBarrierNode": (DYNAMIC, DYNAMIC),
    "LedActivityNode": (DYNAMIC, DYNAMIC),
    # RoundRobinNode: fixed 'in', but its outputs are a config-driven
    # (`count`) list of numbered ports (out0, out1, ...) - same DYNAMIC
    # rationale as SplitByValueNode just above: no fixed, type-level list
    # to validate a wire's port name against, since the real count only
    # exists in this node's own config.
    "RoundRobinNode": (DEFAULT_INPUT_PORTS, DYNAMIC),
    # QueueGateNode: two distinct, fixed, non-default input ports ('in'
    # the data to queue, 'pop' the release trigger whose own content is
    # discarded) and one normal output.
    "QueueGateNode": (["in", "pop"], DEFAULT_OUTPUT_PORTS),
    # MCPClientNode: plain single 'in'/'out' - the default schema already
    # covers it, listed here only for discoverability alongside the rest
    # of this update's new node types.
    "MCPClientNode": (DEFAULT_INPUT_PORTS, DEFAULT_OUTPUT_PORTS),
    # LMStudioNode: feature request - a downstream graph used to get only
    # one 'out' port carrying either a {'prompt','completion'} or a
    # {'prompt','error'} dict and had to branch on shape. Split into
    # fixed, distinct ports: 'out' (everything, unchanged in spirit from
    # before), 'prompt' (echoes what was sent, fires on every request
    # whether it succeeds or not), 'reasoning' (a reasoning-capable
    # model's chain-of-thought, when the response actually has one),
    # 'results' (just the final answer text), 'errors' (just the error
    # string on failure) and 'stats' (token usage + latency, when the
    # server reports it). See nodes/llm_lmstudio.py's process() for
    # exactly when each one fires.
    "LMStudioNode": (DEFAULT_INPUT_PORTS, ["out", "prompt", "reasoning", "results", "errors", "stats"]),
    # HttpPostNode: feature request - "add a node to post data to
    # external webservers". Fixed, distinct ports mirroring LMStudioNode's
    # own split above: 'out' (everything, one item either way), 'request'
    # (echoes what was sent, fires unconditionally), 'response' (just the
    # response body, only on a 2xx), 'errors' (just the error string,
    # only on failure - connection error or a non-2xx status), 'stats'
    # (status_code/latency_s/url, only on success). See
    # nodes/http_post_node.py's process() for exactly when each fires.
    "HttpPostNode": (DEFAULT_INPUT_PORTS, ["out", "request", "response", "errors", "stats"]),
}

VALID_EDGE_TYPES = {"data", "control", "endpoint", "attribute", "raw"}


def raw_port_name(name: str) -> str:
    """The legacy "unencapsulated tap" counterpart port name a real output
    port ``name`` used to get automatically paired with, back when ``raw``
    meant a second, permanently-duplicated port rather than a per-wire edge
    kind - ``'out'`` -> ``'raw'``, anything else -> ``'raw_' + name``.

    Phase 2 of the wire-kind-unification design (see
    ``claude/design_unified_wire_kinds_plan.md``) retired the schema/
    runtime machinery that used to generate these pairs automatically
    (``_add_raw_pairs()``/``BaseNode._auto_pair_raw_output()``) in favor of
    a real ``"raw"`` edge type chosen per-wire - a downstream consumer now
    gets the unwrapped value by wiring a ``type: "raw"`` edge onto the
    node's one real output port, not by wiring a plain ``data`` edge onto
    a separate ``raw``-named port. This function is kept around purely as
    a **migration-only helper**: ``core/persistence.py``'s
    ``load_workflow()`` uses it (via ``legacy_raw_base_port()`` below) to
    recognize and rewrite a workflow YAML saved under the old scheme, so
    it keeps loading and running correctly with no hand-editing required.
    Not used by ``get_port_schema()``/``validate_edge()``/runtime wiring
    any more - those all deal in real, singular port names only now.

    Left untouched: ApiOutputNode's own hand-declared ``raw`` port (a
    real, permanent, distinct port - never something this function
    generated) and ForkNode's own numbered ``outN``/``rawN`` pairing
    (dynamic outputs, paired by editor.js at wire-time, a separate
    mechanism this design never touched).
    """
    return 'raw' if name == 'out' else f'raw_{name}'


def legacy_raw_base_port(name: str) -> str | None:
    """Inverse of ``raw_port_name()``: if ``name`` looks like a port the
    old ``_add_raw_pairs()`` machinery would have generated (bare
    ``'raw'``, or ``'raw_<something>'``), return the real base port name
    it was paired with (``'raw'`` -> ``'out'``, ``'raw_history'`` ->
    ``'history'``); otherwise ``None``.

    Migration-only, used by ``core/persistence.py``'s ``load_workflow()``.
    Deliberately does **not** match ForkNode's numbered ``'raw0'``/
    ``'raw1'``-style ports (no underscore) - those belong to a separate,
    still-intact mechanism (see ``raw_port_name()``'s docstring) and must
    never be rewritten by this migration.
    """
    if name == 'raw':
        return 'out'
    if name.startswith('raw_'):
        return name[len('raw_'):]
    return None


def get_port_schema(type_name: str) -> dict[str, object]:
    """Return ``{"inputs": [...] | DYNAMIC, "outputs": [...] | DYNAMIC}``
    for a node type name. Unknown type names get the defaults too -
    callers that need to distinguish "unknown type" should check the node
    registry (``core.registry.build_node_registry()``) themselves first.

    Phase 2 of the wire-kind-unification design (see
    ``claude/design_unified_wire_kinds_plan.md``): this used to run every
    fixed output list through ``_add_raw_pairs()``, so e.g.
    ``ConstantValueNode`` reported ``["out", "raw"]`` here even though
    ``_OVERRIDES`` only ever declared ``"out"``. That auto-pairing is
    retired - a node type's real output ports are exactly what
    ``_OVERRIDES``/the defaults say, no more, no less. A node that wants a
    genuinely separate, permanent second output port still declares it
    explicitly in ``_OVERRIDES`` (see ``ApiOutputNode``'s ``["out",
    "raw"]`` - a real hand-declared pair, not something this function
    generates).
    """
    ins, outs = _OVERRIDES.get(type_name, (DEFAULT_INPUT_PORTS, DEFAULT_OUTPUT_PORTS))
    return {"inputs": ins, "outputs": list(outs) if isinstance(outs, list) else outs}


def all_port_schemas() -> dict[str, dict[str, object]]:
    """Port schema for every node type known to the shared registry."""
    from .registry import build_node_registry

    return {name: get_port_schema(name) for name in build_node_registry()}


def _port_allowed(port: str, declared: object) -> bool:
    return declared == DYNAMIC or port in declared


def validate_edge(
    source_type: str,
    source_port: str,
    target_type: str,
    target_port: str,
    edge_type: str = "data",
) -> str | None:
    """Check one edge against the port schemas of its endpoints.

    Returns ``None`` if the edge is fine, or a human-readable error string
    if not. Used by both ``Engine.validate()`` (so a bad workflow YAML -
    loaded straight from a file via the CLI/MCP, bypassing the API
    entirely - fails fast with a clear message) and the HTTP API's
    ``/workflows`` and ``/nodes/connect`` endpoints (so a bad wire from
    the UI is rejected up front instead of silently doing nothing, which
    is what happened before this validation existed - see finding 4.3 and
    the Phase 2 write-up in the project's evaluation doc).
    """
    if edge_type not in VALID_EDGE_TYPES:
        return f"unknown edge type {edge_type!r} (expected one of {sorted(VALID_EDGE_TYPES)})"

    src_schema = get_port_schema(source_type)
    if not _port_allowed(source_port, src_schema["outputs"]):
        return (
            f"{source_type!r} has no output port {source_port!r} "
            f"(its output ports are {src_schema['outputs']!r})"
        )

    if edge_type == "raw" and src_schema["outputs"] == DYNAMIC:
        # Phase 2 of the wire-kind-unification design: "raw" is legal on
        # any *fixed*, declared output port - the same condition the old
        # _add_raw_pairs() machinery used to gate its auto-pairing on
        # (DYNAMIC-output types are excluded there too). Without this
        # check, a DYNAMIC schema's blanket "any port name is allowed"
        # rule (see _port_allowed() above) would let a 'raw'-type edge
        # through for ForkNode - but ForkNode already has its own,
        # separate outN/rawN pairing mechanism (paired by name at wire
        # time in editor.js), untouched by this design, so a generic
        # per-wire 'raw' kind has no meaning there.
        return (
            f"{source_type!r} has a dynamic output schema - a 'raw' edge "
            f"only applies to a fixed, declared output port"
        )

    if edge_type in ("attribute", "control"):
        # An attribute edge's target_port names a config attribute on the
        # *target node instance* (e.g. "path" on a particular
        # FileInputNode), not one of its class-level input ports - there's
        # no fixed, per-type list to check it against the way there is for
        # data/endpoint edges (see BaseNode.set_attribute()), so only the
        # source side is validated for this edge type.
        #
        # A control edge's target_port is, likewise, not actually one of
        # the target's declared data input ports any more:
        # Engine._wire_edges() always lands a control edge on the target's
        # single reserved 'control' pipe (BaseNode.add_input()'s
        # `name == 'control'` special case) regardless of what
        # target_port the edge specifies - see the matching comment there.
        # That reserved pipe exists on every BaseNode subclass no matter
        # what its declared input ports are, including pure source types
        # with none at all (LogInputNode, GeneratorInputNode, ...), so
        # checking target_port against `tgt_schema["inputs"]` here would
        # incorrectly reject a perfectly valid control edge to any node
        # whose data-port list doesn't happen to contain whatever name the
        # UI/YAML used (most commonly 'in', kept as the node editor's
        # default for backward compatibility with control edges drawn
        # before it grew a dedicated control slot).
        return None

    tgt_schema = get_port_schema(target_type)
    if not _port_allowed(target_port, tgt_schema["inputs"]):
        return (
            f"{target_type!r} has no input port {target_port!r} "
            f"(its input ports are {tgt_schema['inputs']!r})"
        )

    return None
