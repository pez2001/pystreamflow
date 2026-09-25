import asyncio
import pathlib

import yaml

from ..core.models import Edge, Graph, Node
from ..core.node import BaseNode
from ..core.persistence import load_workflow
from ..core.plugin_manager import PluginManager
from ..core.stream import Pipe


class SubgraphNode(BaseNode):
    async def init(self):
        self.workflow_path = self.config.get('workflow_path')
        self.workflow_yaml = self.config.get('workflow_yaml')

        if self.workflow_path:
            self.graph = load_workflow(self.workflow_path)
        elif self.workflow_yaml:
            data = yaml.safe_load(self.workflow_yaml)
            g = Graph()
            for n in data.get('nodes', []):
                g.add_node(Node(**n))
            for e in data.get('edges', []):
                g.add_edge(Edge(**e))
            self.graph = g
        else:
            raise ValueError('SubgraphNode requires workflow_path or workflow_yaml')

        self.internal_nodes = {}
        self.internal_pipes = {}

        # This used to be a fourth hand-maintained copy of the type-name ->
        # class mapping (Engine, the HTTP API and the MCP server each had
        # their own too - see core/registry.py), missing several newer
        # node types (UserInputNode, the trigger_advanced.py classes,
        # SubgraphNode itself for nested subgraphs, ...) that the other
        # three copies did know about. Now built from the same single
        # source of truth as everywhere else.
        from ..core.registry import build_node_registry
        registry = build_node_registry()
        pm = PluginManager(pathlib.Path(__file__).parent.parent.parent / 'plugins')
        pm.load_plugins()

        for node in self.graph.nodes:
            cls = registry.get(node.type) or pm.get_node_class(node.type)
            if not cls:
                from ..core.node import BaseNode as BN
                class GenericNode(BN):
                    async def process(self):
                        while self._running:
                            await asyncio.sleep(1)
                cls = GenericNode
            inst = cls(node.id, node.config)
            self.internal_nodes[node.id] = inst

        for e in self.graph.edges:
            pipe = self.internal_pipes.setdefault((e.source, e.target), Pipe())
            src_node = self.internal_nodes[e.source]
            tgt_node = self.internal_nodes[e.target]
            src_node.add_output(e.source_port, pipe)
            tgt_node.add_input(e.target_port, pipe)

        incoming = {e.target for e in self.graph.edges}
        outgoing = {e.source for e in self.graph.edges}

        # ------------------------------------------------------------
        # Multi-input/output bridge support.
        #
        # A "bridge" maps one of this SubgraphNode's own external ports
        # to one internal node's port: {external_port, node_id, port}
        # (`port` is the internal node's port name; if omitted it's
        # auto-detected exactly the way the original single-bridge
        # implementation did - the internal node's first existing port,
        # or 'in'/'out' if it has none yet).
        #
        # `input_bridges`/`output_bridges` (new) is a list of such dicts
        # and is the only way to get more than one subgraph input/output.
        # When absent, this falls back byte-for-byte to the original
        # single `input_node_id`/`output_node_id`/`input_port`/
        # `output_port` config keys (still read below), so every existing
        # workflow YAML that only ever used one input and one output
        # keeps working completely unchanged - this is additive, not a
        # replacement, of the original shape.
        # ------------------------------------------------------------
        raw_input_bridges = self.config.get('input_bridges')
        if not raw_input_bridges:
            input_node_id = self.config.get('input_node_id')
            if not input_node_id:
                entry_candidates = [n.id for n in self.graph.nodes if n.id not in incoming]
                input_node_id = entry_candidates[0] if entry_candidates else (
                    self.graph.nodes[0].id if self.graph.nodes else None
                )
            raw_input_bridges = [{
                'external_port': self.config.get('input_port', 'in'),
                'node_id': input_node_id,
            }] if input_node_id else []

        raw_output_bridges = self.config.get('output_bridges')
        if not raw_output_bridges:
            output_node_id = self.config.get('output_node_id')
            if not output_node_id:
                exit_candidates = [n.id for n in self.graph.nodes if n.id not in outgoing]
                output_node_id = exit_candidates[0] if exit_candidates else (
                    self.graph.nodes[-1].id if self.graph.nodes else None
                )
            raw_output_bridges = [{
                'external_port': self.config.get('output_port', 'out'),
                'node_id': output_node_id,
            }] if output_node_id else []

        # Each entry: (external_port_name, bridge_pipe, internal_node).
        self._input_bridges: list[tuple[str, Pipe, object]] = []
        self._output_bridges: list[tuple[str, Pipe, object]] = []

        for b in raw_input_bridges:
            entry_node = self.internal_nodes.get(b.get('node_id'))
            if not entry_node:
                continue
            bridge_in = Pipe()
            target_port = b.get('port')
            if target_port:
                entry_node.add_input(target_port, bridge_in)
            elif not entry_node.inputs:
                entry_node.add_input('in', bridge_in)
            else:
                # Regression fix (kept from the original single-bridge
                # code): connect to the entry node's first *real* input
                # port rather than calling add_input() again, which would
                # add a brand new port alongside whatever real edge is
                # already wired inside the subgraph instead of replacing
                # its feed.
                first_port = next(iter(entry_node.inputs))
                entry_node.inputs[first_port] = bridge_in
            self._input_bridges.append((b.get('external_port', 'in'), bridge_in, entry_node))

        for b in raw_output_bridges:
            exit_node = self.internal_nodes.get(b.get('node_id'))
            if not exit_node:
                continue
            bridge_out = Pipe()
            source_port = b.get('port')
            if source_port:
                exit_node.add_output(source_port, bridge_out)
            elif not exit_node.outputs:
                exit_node.add_output('out', bridge_out)
            else:
                # Phase 1 fan-out change: BaseNode.outputs now holds a list
                # of (Pipe, kind) tuples per port name (Phase 2 added the
                # `kind` element - see core/node.py), but the intent here is
                # unchanged from before either change - replace the exit
                # node's existing internal wiring on its first real output
                # port with this bridge pipe (mirroring the input-bridge
                # branch above), not add a second consumer alongside it.
                # Internal subgraph bridging always carries plain data, so
                # the kind is hardcoded to 'data' here.
                first_out = next(iter(exit_node.outputs))
                exit_node.outputs[first_out] = [(bridge_out, 'data')]
            self._output_bridges.append((b.get('external_port', 'out'), bridge_out, exit_node))

        # Backward-compat single-bridge attributes - some callers (tests,
        # reflection/debug endpoints) may still read these directly.
        self.input_port = self._input_bridges[0][0] if self._input_bridges else self.config.get('input_port', 'in')
        self.output_port = self._output_bridges[0][0] if self._output_bridges else self.config.get('output_port', 'out')
        self.entry_node = self._input_bridges[0][2] if self._input_bridges else None
        self.exit_node = self._output_bridges[0][2] if self._output_bridges else None
        self.bridge_in = self._input_bridges[0][1] if self._input_bridges else None
        self.bridge_out = self._output_bridges[0][1] if self._output_bridges else None

        self._internal_tasks = []

    async def process(self):
        # start internal nodes
        for node in self.internal_nodes.values():
            if not node._running:
                self._internal_tasks.append(asyncio.create_task(node.start()))

        # One background task per input bridge, each independently
        # forwarding this SubgraphNode's external input pipe for that
        # bridge's external port into the internal bridge pipe feeding
        # its entry node - so multiple inputs are fed concurrently rather
        # than the single sequential external_pipe the original
        # single-bridge implementation polled.
        pump_tasks = [
            asyncio.create_task(self._pump_input(external_port, bridge_pipe))
            for external_port, bridge_pipe, _entry_node in self._input_bridges
        ]

        try:
            # Regression fix vs. the original implementation: output
            # forwarding here no longer depends on an input pipe ever
            # having been wired/found first (the original code only ever
            # reached its output-polling logic after successfully
            # obtaining `external_pipe`, so a SubgraphNode with no
            # external input wired at all - a purely output-producing
            # subgraph - never forwarded any of its internal output
            # either, even though its internal nodes were running fine).
            # Every output bridge is polled here unconditionally for as
            # long as this node runs.
            while self._running:
                for external_port, bridge_pipe, _exit_node in self._output_bridges:
                    try:
                        out_item = bridge_pipe.get_nowait()
                        self.emit(external_port, out_item)
                    except asyncio.QueueEmpty:
                        pass
                await asyncio.sleep(0.01)
        finally:
            for t in pump_tasks:
                t.cancel()

    async def _pump_input(self, external_port: str, bridge_pipe: Pipe):
        external_pipe = self.inputs.get(external_port)
        if not external_pipe:
            # wait for connection
            while self._running:
                await asyncio.sleep(0.5)
                external_pipe = self.inputs.get(external_port)
                if external_pipe:
                    break
            if not external_pipe:
                return
        while self._running:
            try:
                item = await asyncio.wait_for(external_pipe.get(), timeout=0.5)
                await bridge_pipe.put(item)
            except asyncio.TimeoutError:
                continue

    async def stop(self):
        await super().stop()
        for t in self._internal_tasks:
            t.cancel()
