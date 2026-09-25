"""
Regression tests for Phase 2 (+ bundled Phase 3 migration) of the
wire-kind-unification design (see
claude/design_unified_wire_kinds_plan.md): "raw" as a real per-wire edge
*kind*, delivered on a node's one real output port, instead of the old
scheme where every fixed output port was automatically duplicated into a
second, permanently-wired "raw"/"raw_<name>" port
(``core/port_schema.py``'s now-deleted ``_add_raw_pairs()`` /
``BaseNode``'s now-deleted ``_auto_pair_raw_output()``).

What changed, verified here against real node instances and a real
Engine-wired graph (never by inspection alone):

1. ``BaseNode.outputs`` now holds ``list[tuple[Pipe, str]]`` per port name
   - each wired consumer carries its own delivery ``kind`` ("data" or
   "raw"). ``emit()`` runs the item through ``core/engine.py``'s
   ``_clean_attribute_value()`` for a "raw" consumer (the same unwrap
   already used for "attribute" edges) and delivers it unchanged for a
   "data" consumer - two consumers on the *same* output anchor can now get
   two different views of the same emitted item.

2. ``validate_edge()`` rejects a "raw" edge whose source has a ``DYNAMIC``
   output schema (e.g. ForkNode) - "raw" only makes sense as a kind on a
   real, fixed, named output port.

3. ``Engine._wire_edges()`` passes ``kind='raw'`` through to
   ``add_output()`` exactly when the edge's declared type is "raw" - this
   is what makes the distinction reach real wiring, not just the
   lower-level ``BaseNode`` API.

4. ``core/persistence.py``'s ``_migrate_legacy_raw_edges()`` rewrites a
   workflow YAML saved under the old scheme (``source_port: 'raw'``/
   ``'raw_<name>'``, ``type: 'data'``) into the new scheme
   (``source_port: '<name>'``, ``type: 'raw'``) transparently on load, so
   an old save file still loads and still delivers the same unwrapped
   value it always did - and never touches an edge that's already
   pointed at a real, currently-declared output port (ApiOutputNode's own
   hand-declared 'raw' port; ForkNode's DYNAMIC-schema 'rawN' ports).

5. A real bug this retirement introduced and fixed in the same pass:
   ApiOutputNode's own 'raw' output port (a real, permanently
   hand-declared port, unrelated to the generic auto-pairing mechanism -
   see its ``_OVERRIDES`` entry) used to receive its item via the old
   generic auto-replay in ``BaseNode.emit()``, not an explicit emit call
   of its own. Retiring that generic auto-replay silently broke this
   node's 'raw' port (it stopped emitting anything at all) until
   ``nodes/output_api.py``'s ``process()`` was given back its own explicit
   second ``self.emit('raw', item)`` call.
"""
import asyncio

from pystreamflow.core.engine import Engine
from pystreamflow.core.models import Edge, Graph, Node
from pystreamflow.core.node import BaseNode
from pystreamflow.core.persistence import load_workflow, save_workflow
from pystreamflow.core.port_schema import legacy_raw_base_port, validate_edge
from pystreamflow.core.stream import Pipe


class _Source(BaseNode):
    async def process(self):
        await asyncio.sleep(3600)


async def test_raw_kind_unwraps_value_data_kind_keeps_full_item():
    # Two consumers on the *same* output anchor, one wired "data" and one
    # "raw" - this is the whole point of Phase 2: the distinction lives on
    # the wire/consumer, not on a second, separately-named port.
    n = _Source('n', {})
    data_pipe, raw_pipe = Pipe(), Pipe()
    n.add_output('out', data_pipe, kind='data')
    n.add_output('out', raw_pipe, kind='raw')
    n.emit('out', {'value': 'hello', 'index': 0})
    data_item = await asyncio.wait_for(data_pipe.get(), timeout=2.0)
    raw_item = await asyncio.wait_for(raw_pipe.get(), timeout=2.0)
    assert data_item == {'value': 'hello', 'index': 0}
    assert raw_item == 'hello'


async def test_raw_kind_passes_through_unchanged_when_not_the_value_shape():
    # _clean_attribute_value() only unwraps the one unambiguous shape (a
    # dict with a scalar 'value' key) - anything else passes through
    # unchanged, so a "raw" consumer of e.g. a bare string or a dict with
    # no 'value' key gets exactly what was emitted, same as "data" would.
    n = _Source('n', {})
    raw_pipe = Pipe()
    n.add_output('out', raw_pipe, kind='raw')
    n.emit('out', {'level': 'INFO', 'log': 'already unwrapped'})
    item = await asyncio.wait_for(raw_pipe.get(), timeout=2.0)
    assert item == {'level': 'INFO', 'log': 'already unwrapped'}


def test_validate_edge_rejects_raw_on_dynamic_output():
    # ForkNode's output schema is DYNAMIC (config-defined port names) - a
    # "raw" edge kind only makes sense against a real, fixed, declared
    # output port, so this must be rejected rather than silently accepted
    # and doing nothing useful at wire time.
    err = validate_edge('ForkNode', 'out0', 'LogOutputNode', 'in', 'raw')
    assert err is not None
    assert 'dynamic' in err.lower()


def test_validate_edge_accepts_raw_on_fixed_output():
    err = validate_edge('ListStringsNode', 'out', 'LogOutputNode', 'in', 'raw')
    assert err is None


async def test_engine_wires_raw_edge_and_delivers_unwrapped_value():
    # End-to-end through the real Engine wiring path (Engine._wire_edges()
    # -> BaseNode.add_output(..., kind=...)), not just the lower-level
    # BaseNode API tested above - this is what a real workflow YAML with a
    # 'raw'-type edge actually exercises.
    g = Graph()
    g.add_node(Node(id='src', type='ListStringsNode', config={'strings': ['hi'], 'interval': 3600, 'loop': False}))
    g.add_node(Node(id='sink', type='LogOutputNode', config={}))
    g.add_edge(Edge(source='src', target='sink', source_port='out', target_port='in', type='raw'))
    e = Engine(g)
    assert e.validate() is True
    # Engine.run() itself blocks forever (via _supervise()'s while True) -
    # mirroring how core/session_manager.py's Session actually drives it,
    # run it as a background task and cancel + await it in the finally,
    # which is what makes Engine.stop_all() actually run (see run()'s
    # CancelledError handler).
    task = asyncio.create_task(e.run())
    try:
        # Give the node startup + pipe scheduling a little real slack
        # rather than a single fixed sleep - polling briefly avoids
        # flakiness from event-loop scheduling latency without weakening
        # what's actually being asserted. The freshly-created task above
        # hasn't run its first step yet either, so node_instances itself
        # isn't populated until this loop's first await hands control
        # back to it.
        last = []
        for _ in range(20):
            await asyncio.sleep(0.1)
            sink = e.node_instances.get('sink')
            last = sink.get_last() if sink else []
            if last:
                break
        assert last, "sink never received anything"
        # LogOutputNode records {'level', 'log': <the item it received>} -
        # 'log' being the bare string 'hi' (not {'value': 'hi', 'index':
        # 0}) is the actual thing this test is verifying: the 'raw' edge
        # kind unwrapped ListStringsNode's {'value', 'index'}-shaped item
        # before it ever reached the sink.
        assert last[0]['item']['log'] == 'hi'
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def test_legacy_raw_edge_migrated_on_load(tmp_path):
    # A workflow YAML saved under the pre-Phase-2 convention
    # (source_port naming a legacy auto-paired port, type still 'data')
    # must load as the new scheme transparently.
    g = Graph()
    g.add_node(Node(id='src', type='ListStringsNode', config={}))
    g.add_node(Node(id='sink', type='LogOutputNode', config={}))
    g.add_edge(Edge(source='src', target='sink', source_port='raw', target_port='in', type='data'))
    path = str(tmp_path / 'legacy.yaml')
    save_workflow(g, path)
    g2 = load_workflow(path)
    assert len(g2.edges) == 1
    migrated = g2.edges[0]
    assert migrated.source_port == 'out'
    assert migrated.type == 'raw'


def test_legacy_raw_history_edge_migrated_to_correct_base_port(tmp_path):
    g = Graph()
    g.add_node(Node(id='buf', type='RollingWindowBufferNode', config={}))
    g.add_node(Node(id='sink', type='LogOutputNode', config={}))
    g.add_edge(Edge(source='buf', target='sink', source_port='raw_history', target_port='in', type='data'))
    path = str(tmp_path / 'legacy_history.yaml')
    save_workflow(g, path)
    g2 = load_workflow(path)
    migrated = g2.edges[0]
    assert migrated.source_port == 'history'
    assert migrated.type == 'raw'


def test_migration_leaves_api_output_nodes_real_raw_port_alone(tmp_path):
    # ApiOutputNode's 'raw' is a real, hand-declared output port (not
    # something the retired auto-pairing generated) - a data edge already
    # pointed at it must be left completely untouched by the migration,
    # since rewriting it to "type: raw" would double-unwrap an item that
    # was never wrapped in the first place for this node's own semantics.
    g = Graph()
    g.add_node(Node(id='src', type='ListStringsNode', config={}))
    g.add_node(Node(id='api', type='ApiOutputNode', config={'uri': 'migrationtest'}))
    g.add_edge(Edge(source='src', target='api', source_port='out', target_port='in', type='data'))
    g.add_edge(Edge(source='api', target='src', source_port='raw', target_port='in', type='data'))
    path = str(tmp_path / 'apiout.yaml')
    save_workflow(g, path)
    g2 = load_workflow(path)
    raw_edge = next(e for e in g2.edges if e.source == 'api')
    assert raw_edge.source_port == 'raw'
    assert raw_edge.type == 'data'


def test_migration_leaves_fork_nodes_dynamic_ports_alone(tmp_path):
    # ForkNode's output schema is DYNAMIC (config-defined 'outN'/'rawN'
    # port names) - a separate, untouched mechanism this design doesn't
    # touch. legacy_raw_base_port('raw0') doesn't even match (no
    # underscore), and the DYNAMIC-schema check is a second, independent
    # guard against ever rewriting one of these.
    assert legacy_raw_base_port('raw0') is None
    g = Graph()
    g.add_node(Node(id='fork', type='ForkNode', config={}))
    g.add_node(Node(id='sink', type='LogOutputNode', config={}))
    g.add_edge(Edge(source='fork', target='sink', source_port='raw0', target_port='in', type='data'))
    path = str(tmp_path / 'fork.yaml')
    save_workflow(g, path)
    g2 = load_workflow(path)
    edge = g2.edges[0]
    assert edge.source_port == 'raw0'
    assert edge.type == 'data'


async def test_api_output_node_raw_port_still_emits_after_retiring_auto_replay():
    # Direct regression test for the bug described in this file's module
    # docstring (point 5): ApiOutputNode's own hand-declared 'raw' port
    # used to be fed via BaseNode.emit()'s now-retired generic auto-replay
    # onto a second port, not an explicit emit call of its own. Wire both
    # of its real output ports and confirm both still receive the same
    # item on every process() iteration.
    from pystreamflow.nodes.output_api import ApiOutputNode
    n = ApiOutputNode('api-raw-regress', {'uri': 'phase2regress', 'sse': False, 'port': 0})
    out_pipe, raw_pipe = Pipe(), Pipe()
    n.add_output('out', out_pipe)
    n.add_output('raw', raw_pipe)
    await n.start()
    try:
        await n.in_pipe.put({'v': 7})
        out_item = await asyncio.wait_for(out_pipe.get(), timeout=2.0)
        raw_item = await asyncio.wait_for(raw_pipe.get(), timeout=2.0)
        assert out_item == {'v': 7}
        assert raw_item == {'v': 7}
    finally:
        await n.stop()
        from pystreamflow.core import web_server
        from pystreamflow.core.web_server import _nodes
        await web_server.stop_server()
        _nodes.pop('api-raw-regress', None)


if __name__ == '__main__':
    asyncio.run(test_raw_kind_unwraps_value_data_kind_keeps_full_item())
    asyncio.run(test_raw_kind_passes_through_unchanged_when_not_the_value_shape())
    test_validate_edge_rejects_raw_on_dynamic_output()
    test_validate_edge_accepts_raw_on_fixed_output()
    asyncio.run(test_engine_wires_raw_edge_and_delivers_unwrapped_value())
    print('phase 2 raw-edge-kind tests passed (run via pytest for the tmp_path-based ones)')
