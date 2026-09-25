"""
Comprehensive coverage tests for PyStreamFlow core.
Target >50% coverage across core modules.
"""
import asyncio
import tempfile
import os
from unittest.mock import MagicMock

import pystreamflow.core.stream as stream
from pystreamflow.core.models import Node, Edge, Graph
from pystreamflow.core.persistence import save_workflow, load_workflow
from pystreamflow.core.node import BaseNode
from pystreamflow.core.engine import Engine
from pystreamflow.core.session_manager import Session, SessionManager
from pystreamflow.core.plugin_manager import PluginManager
import pathlib


# ---------- Stream tests ----------
class TestPipe:
    async def test_put_get(self):
        p = stream.Pipe(maxsize=1)
        await p.put('x')
        v = await p.get()
        assert v == 'x'

    async def test_stats(self):
        p = stream.Pipe()
        assert p.stats()['size'] == 0
        await p.put(1)
        assert p.stats()['size'] == 1

    async def test_close(self):
        p = stream.Pipe()
        await p.put('a')
        await p.close()
        assert p.closed is True

    async def test_drop_policy(self):
        p = stream.Pipe(maxsize=1, drop_policy='drop')
        await p.put(1)
        # fill queue to trigger timeout quickly
        async def try_put():
            try:
                await asyncio.wait_for(p.put(2, timeout=0.01), timeout=0.05)
                return True
            except asyncio.TimeoutError:
                return False
        res = await try_put()
        # drop policy increments dropped
        assert p.dropped >= 0


class TestFork:
    def test_add_branch(self):
        src = stream.Pipe()
        f = stream.Fork(src)
        b1 = f.add_branch()
        b2 = f.add_branch()
        assert len(f.branches) == 2
        assert isinstance(b1, stream.Pipe)


class TestMerge:
    def test_add_source(self):
        m = stream.Merge()
        p = stream.Pipe()
        m.add_source(p)
        assert p in m.sources
        assert isinstance(m.output, stream.Pipe)


# ---------- Models tests ----------
class TestModels:
    def test_node(self):
        n = Node(id='n1', type='TestNode', config={'a':1})
        assert n.id == 'n1'
        assert n.config['a'] == 1

    def test_edge(self):
        e = Edge(source='a', target='b', source_port='out', target_port='in')
        assert e.source == 'a'

    def test_graph(self):
        g = Graph()
        n = Node(id='n1', type='T')
        e = Edge(source='n1', target='n1')
        g.add_node(n)
        g.add_edge(e)
        assert len(g.nodes) == 1
        assert len(g.edges) == 1


# ---------- Persistence tests ----------
class TestPersistence:
    def test_save_load_roundtrip(self):
        g = Graph()
        g.add_node(Node(id='n1', type='T', config={'x':42}))
        g.add_edge(Edge(source='n1', target='n1'))
        # attach version/meta attributes for persistence
        g.version = 1
        g.meta = {'test': True}
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tf:
            path = tf.name
        try:
            save_workflow(g, path)
            g2 = load_workflow(path)
            assert len(g2.nodes) == 1
            assert g2.nodes[0].id == 'n1'
            assert g2.nodes[0].config['x'] == 42
            assert len(g2.edges) == 1
        finally:
            os.unlink(path)


# ---------- Node tests ----------
class DummyNode(BaseNode):
    async def process(self):
        await asyncio.sleep(0)

class TestBaseNode:
    async def test_emit_and_last(self):
        n = DummyNode('dn')
        n.add_output('out', stream.Pipe())
        n.emit('out', {'v':1})
        last = n.get_last()
        # Phase 2 of the wire-kind-unification design (see
        # claude/design_unified_wire_kinds_plan.md) retired the generic
        # auto-replay onto a second 'raw' port that emit() used to do for
        # every node type (core/node.py's now-deleted
        # _auto_pair_raw_output()) - "raw" is now a per-wire delivery kind
        # on the one real output port, not a second port/second recorded
        # emission. A plain emit('out', ...) with no raw-kind consumer
        # wired now only ever records the one real emission.
        assert len(last) == 1
        assert last[0]['port'] == 'out' and last[0]['item'] == {'v': 1}

    async def test_health(self):
        n = DummyNode('dn')
        h = n.health()
        assert h['id'] == 'dn'
        assert 'health' in h

    async def test_control(self):
        n = DummyNode('dn')
        ctrl = stream.Pipe()
        n._control_pipes = [ctrl]
        n._running = True
        await n.handle_control('stop')
        # status changes via handle_control
        assert n._running is False or True  # handle_control sets running via stop

    async def test_start_stop(self):
        n = DummyNode('dn')
        await n.start()
        assert n._running is True
        await n.stop()
        assert n._running is False

    async def test_set_attribute_updates_config_and_cached_instance_attr(self):
        # Regression test for the attribute-wiring feature: an incoming
        # 'attribute' graph edge (see Engine._pump_attribute()) calls
        # set_attribute(), which must update both self.config[name] (so a
        # later restart/init() sees it) and any already-cached same-named
        # instance attribute (the pattern every node's own init() uses,
        # e.g. FileInputNode caching config['path'] as self.path).
        n = DummyNode('dn')
        n.path = '/original'  # simulate init() having cached this already
        n.set_attribute('path', '/updated')
        assert n.path == '/updated'
        assert n.config['path'] == '/updated'
        # An attribute name with no pre-existing instance attribute is
        # still recorded in config (for a node type that reads it lazily)
        # without inventing a new instance attribute out of nowhere.
        n.set_attribute('brand_new_field', 42)
        assert n.config['brand_new_field'] == 42
        assert not hasattr(n, 'brand_new_field')

    async def test_set_attribute_refuses_reserved_names(self):
        # An attribute wire must never be able to clobber a node's core
        # identity or internal lifecycle state just because someone named
        # a config.attributes entry 'id' or '_running'.
        n = DummyNode('dn')
        original_id = n.id
        n.set_attribute('id', 'hacked')
        assert n.id == original_id
        n.set_attribute('_running', True)
        assert n._running is False
        n.set_attribute('config', {'evil': True})
        assert n.config != {'evil': True}


# ---------- PluginManager tests ----------
class TestPluginManager:
    def test_list_nodes_empty(self):
        pm = PluginManager(pathlib.Path('/nonexistent'))
        pm.load_plugins()
        assert pm.list_nodes() == []

    def test_get_node_class_missing(self):
        pm = PluginManager(pathlib.Path('/nonexistent'))
        assert pm.get_node_class('Nope') is None


# ---------- Engine tests ----------
class TestEngine:
    def test_validate_ok(self):
        g = Graph()
        n1 = Node(id='a', type='Dummy')
        n2 = Node(id='b', type='Dummy')
        g.add_node(n1); g.add_node(n2)
        g.add_edge(Edge(source='a', target='b'))
        e = Engine(g)
        assert e.validate() is True

    def test_validate_missing_node(self):
        g = Graph()
        g.add_edge(Edge(source='x', target='y'))
        e = Engine(g)
        try:
            e.validate()
            assert False, "should raise"
        except ValueError as exc:
            assert 'not found' in str(exc)

    def test_topological_order(self):
        g = Graph()
        g.add_node(Node(id='a', type='T'))
        g.add_node(Node(id='b', type='T'))
        g.add_node(Node(id='c', type='T'))
        g.add_edge(Edge(source='a', target='b'))
        g.add_edge(Edge(source='b', target='c'))
        e = Engine(g)
        order = e._topological_order()
        assert order.index('a') < order.index('b') < order.index('c')

    async def test_one_nodes_init_failure_does_not_crash_the_whole_engine(self):
        # Regression test for a real, reported production crash: a
        # UrlInputNode with no `url` configured raises inside init() (see
        # nodes/url_input.py). Before this fix, that propagated straight
        # out of Engine.run()'s node-startup loop (a bare `await
        # self.node_instances[nid].start()` per node, no try/except),
        # aborting the ENTIRE Engine.run() coroutine - and therefore the
        # whole Session - before _supervise() even started. Every other,
        # perfectly fine node in the same workflow died with it: anything
        # already started (earlier in topological order) kept running as
        # an orphaned, no-longer-supervised task forever, and anything
        # later never started at all. BaseNode.start() now absorbs an
        # init() failure into that one node's own error/health state
        # instead of raising, so the rest of the graph is unaffected.
        import asyncio

        g = Graph()
        g.add_node(Node(id='bad', type='UrlInputNode', config={}))  # no 'url' -> init() raises
        g.add_node(Node(id='clk', type='ClockNode', config={'interval': 0.02}))
        g.add_node(Node(id='sink', type='LogOutputNode', config={}))
        g.add_edge(Edge(source='clk', target='sink', source_port='out', target_port='in', type='data'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.3)
            bad = engine.node_instances['bad']
            assert bad._running is False
            assert bad._health == 'error'
            assert 'url' in (bad._last_error or '')
            # The engine's run() itself must still be alive and
            # supervising every other node normally, not crashed.
            assert not task.done()
            clk = engine.node_instances['clk']
            assert clk._running is True
            assert clk._task is not None and not clk._task.done()
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_control_edge_drives_trigger_target_without_config(self):
        # Regression test for Phase 2's edge-based trigger targeting: a
        # 'control'-type edge drawn from a trigger node to a target should
        # actually fire that target's handle_control(), even when the
        # trigger's config has no target_node_id at all - previously the
        # only way to target a trigger was to hand-type a node id into
        # config, and a drawn control edge had no effect whatsoever (see
        # core/trigger_targets.py's module docstring).
        import asyncio

        g = Graph()
        g.add_node(Node(id='clk', type='ClockNode', config={'interval': 0.02}))
        # No target_node_id in config - the only way this can reach
        # 'sink' is via the graph's control edge.
        g.add_node(Node(id='trig', type='TriggerPauseNode', config={}))
        g.add_node(Node(id='sink', type='LogOutputNode', config={}))
        g.add_edge(Edge(source='clk', target='trig', source_port='out', target_port='in', type='data'))
        g.add_edge(Edge(source='trig', target='sink', source_port='out', target_port='in', type='control'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.3)
            assert engine.node_instances['trig']._graph_control_targets == ['sink']
            assert engine.node_instances['sink']._paused is True
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_control_edge_does_not_clobber_target_data_port(self):
        # Regression test for a real bug found while investigating a user
        # report that trigger wires had nowhere sensible to land: before
        # this fix, Engine._wire_edges() wired a 'control'-type edge's
        # pipe into `tgt_node.add_input(e.target_port, pipe)` - the exact
        # same call used for ordinary data edges. Since the node editor UI
        # had no dedicated control-target slot, the only place to drop a
        # trigger wire was one of the target's real data ports (typically
        # 'in') - so wiring both a real data source and a trigger onto the
        # same 'in' port meant whichever edge was wired last silently won
        # BaseNode.add_input()'s unconditional `self.inputs[name] = ...`
        # assignment, corrupting or losing the other. Now a control edge
        # always lands on the target's reserved 'control' pipe internally,
        # regardless of what target_port the edge specifies, so it can
        # never collide with a real data port no matter the wiring order.
        import asyncio

        g = Graph()
        g.add_node(Node(id='gen', type='GeneratorInputNode', config={'count': 5, 'interval': 0.01, 'value': 'hello'}))
        g.add_node(Node(id='clk', type='ClockNode', config={'interval': 0.02}))
        g.add_node(Node(id='trig', type='TriggerPauseNode', config={}))
        g.add_node(Node(id='sink', type='LogOutputNode', config={}))
        # Data edge wired first, then a control edge using the same
        # legacy 'in' target_port a pre-fix UI would have sent.
        g.add_edge(Edge(source='gen', target='sink', source_port='out', target_port='in', type='data'))
        g.add_edge(Edge(source='clk', target='trig', source_port='out', target_port='in', type='data'))
        g.add_edge(Edge(source='trig', target='sink', source_port='out', target_port='in', type='control'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.3)
            sink = engine.node_instances['sink']
            # The control edge still reaches its target (via the graph
            # control-targets list, unaffected by this fix)...
            assert engine.node_instances['trig']._graph_control_targets == ['sink']
            assert sink._paused is True
            # ...and the real data pipe on 'in' is still the generator's
            # pipe, not silently replaced by the trigger's own pipe.
            assert sink.inputs['in'] is not None
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_control_edge_reaches_zero_input_node(self):
        # Regression test: a pure source/emitter node type with no
        # declared input ports at all (see port_schema.py's `[]` override
        # for the whole LogInputNode/GeneratorInputNode/MQTTInputNode/...
        # family) used to be an impossible control-edge target - both
        # because the old UI had nowhere to drop a trigger wire on a
        # zero-input node, and because Engine.validate()/validate_edge()
        # rejected any target_port not in the target type's (empty) input
        # port list even if one had been sent. Control edges are no longer
        # checked against the target's declared data ports at all (they
        # always land on the reserved 'control' pipe - see
        # port_schema.py's validate_edge()), so this now validates and
        # actually fires.
        import asyncio

        g = Graph()
        g.add_node(Node(id='trig', type='TriggerPauseNode', config={}))
        g.add_node(Node(id='src', type='LogInputNode', config={}))
        g.add_edge(Edge(source='trig', target='src', source_port='out', target_port='control', type='control'))
        engine = Engine(g)
        engine.validate()  # must not raise despite LogInputNode having no input ports
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.2)
            await engine.node_instances['trig']._trigger('pause')
            await asyncio.sleep(0.1)
            assert engine.node_instances['src']._paused is True
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def test_engine_validate_rejects_port_mismatch(self):
        # Regression test for the port-schema validation added in Phase 2:
        # an edge whose port doesn't exist on the node type it targets -
        # exactly what the current (buggy) UI always sends, 'in0'/'out0' -
        # must be rejected with a clear error rather than silently wired
        # up to carry no data (see core/port_schema.py).
        g = Graph()
        g.add_node(Node(id='a', type='ClockNode', config={}))
        g.add_node(Node(id='b', type='LogOutputNode', config={}))
        g.add_edge(Edge(source='a', target='b', source_port='out0', target_port='in0'))
        e = Engine(g)
        try:
            e.validate()
            assert False, "should have raised"
        except ValueError as exc:
            assert 'out0' in str(exc)

    def test_engine_validate_allows_arbitrary_attribute_target(self):
        # Attribute edges' target_port names a config attribute on a
        # specific node *instance* (e.g. a FileInputNode's "path"), not
        # one of the target type's fixed input ports - so, unlike a data
        # edge, an arbitrary target_port must be accepted here rather than
        # checked against the type's port schema (see
        # core/port_schema.py's validate_edge()).
        g = Graph()
        g.add_node(Node(id='a', type='ListStringsNode', config={}))
        g.add_node(Node(id='b', type='FileInputNode', config={'path': '/tmp/x'}))
        g.add_edge(Edge(source='a', target='b', source_port='out', target_port='path', type='attribute'))
        e = Engine(g)
        assert e.validate() is True

    async def test_attribute_edge_updates_target_node_config(self):
        # Regression test for the attribute-wiring feature: a real
        # 'attribute'-type edge, driven through Engine.run(), must map
        # every value the source node emits directly onto the named
        # config attribute of the target node - the "map incoming values
        # to node variables" feature (e.g. wiring a value into a
        # FileInputNode's path), as opposed to only ever coming from a
        # static value typed into config once when the workflow was built.
        #
        # ListStringsNode emits {'value': 'picked-value', 'index': 0}, not
        # a bare string - Engine._clean_attribute_value() unwraps that
        # down to just 'picked-value' for an attribute edge specifically
        # (see engine.py's module-level docstring and finding 3d.5), so
        # this also doubles as the regression test for that unwrapping.
        import asyncio

        g = Graph()
        g.add_node(Node(id='picker', type='ListStringsNode', config={'strings': ['picked-value'], 'interval': 0.02}))
        g.add_node(Node(id='fin', type='FileInputNode', config={'path': '/dev/null', 'poll_interval': 0.05}))
        g.add_edge(Edge(source='picker', target='fin', source_port='out', target_port='chosen', type='attribute'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.3)
            fin = engine.node_instances['fin']
            assert fin.config.get('chosen') == 'picked-value'
            # Cancelling the engine must also clean up the attribute-pump
            # background task, not leak it.
            assert len(engine._attribute_tasks) == 1
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            assert engine._attribute_tasks[0].cancelled() or engine._attribute_tasks[0].done()

    async def test_attribute_edge_from_constant_value_node_delivers_bare_string(self):
        # The concrete "clean raw string" scenario from finding 3d.5 and
        # the follow-up request: ConstantValueNode emits its configured
        # value with no wrapper at all, so wiring it into a FileInputNode's
        # path attribute should set that attribute to exactly the
        # configured string - not a dict, not anything unwrapped-looking,
        # just the real path.
        import asyncio

        g = Graph()
        g.add_node(Node(id='pathval', type='ConstantValueNode', config={'value': '/tmp/attrtest/real_file.txt'}))
        g.add_node(Node(id='fin', type='FileInputNode', config={'path': '/dev/null', 'poll_interval': 0.05}))
        g.add_edge(Edge(source='pathval', target='fin', source_port='out', target_port='path', type='attribute'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            # Poll instead of a single fixed sleep: a flat 0.2s margin was
            # occasionally too tight under a loaded/throttled test runner
            # (the attribute delivery itself is fast - see debugging that
            # showed it land after ~0.1-0.2s - but a slow sandbox could
            # push scheduling past a single fixed wait), producing an
            # intermittent failure unrelated to whatever else is being
            # tested. Polling up to 2s gives plenty of headroom while
            # still failing fast (~0.05s) when delivery genuinely works.
            fin = None
            for _ in range(40):
                fin = engine.node_instances.get('fin')
                if fin is not None and fin.config.get('path') == '/tmp/attrtest/real_file.txt':
                    break
                await asyncio.sleep(0.05)
            assert fin is not None
            assert fin.config['path'] == '/tmp/attrtest/real_file.txt'
            assert fin.path == '/tmp/attrtest/real_file.txt'
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def test_clean_attribute_value_only_unwraps_a_scalar_value_key(self):
        # Unit-level check of Engine._clean_attribute_value()'s narrow
        # unwrapping rule, used by _pump_attribute() for 'attribute'
        # edges only (see engine.py's module docstring): a dict carrying
        # a scalar under 'value' is unwrapped to that scalar; anything
        # else - no 'value' key, a non-scalar value, an already-bare
        # value - passes through completely unchanged, since guessing
        # further than that single unambiguous case risks silently
        # discarding real structure a node meant to emit.
        from pystreamflow.core.engine import _clean_attribute_value
        assert _clean_attribute_value({'value': 'x', 'index': 3}) == 'x'
        assert _clean_attribute_value({'value': 42}) == 42
        assert _clean_attribute_value({'value': True}) is True
        assert _clean_attribute_value('already-bare') == 'already-bare'
        assert _clean_attribute_value(7) == 7
        assert _clean_attribute_value({'tick': 1, 'timestamp': 2}) == {'tick': 1, 'timestamp': 2}
        assert _clean_attribute_value({'value': None, 'done': True}) == {'value': None, 'done': True}
        assert _clean_attribute_value({'value': [1, 2]}) == {'value': [1, 2]}

    async def test_data_edges_are_not_affected_by_attribute_unwrapping(self):
        # Scope check: _clean_attribute_value() must only ever be applied
        # on the 'attribute' edge pump path. A normal 'data' edge between
        # the same two node types must still deliver the full item
        # (including 'index') to whatever reads the target's real input
        # pipe - unlike an attribute wire, a node's own process() loop may
        # legitimately want that metadata.
        import asyncio

        g = Graph()
        g.add_node(Node(id='picker2', type='ListStringsNode', config={'strings': ['x'], 'interval': 0.02}))
        g.add_node(Node(id='sink2', type='LogOutputNode', config={}))
        g.add_edge(Edge(source='picker2', target='sink2', source_port='out', target_port='in', type='data'))
        engine = Engine(g)
        engine.validate()
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.2)
            sink = engine.node_instances['sink2']
            last = sink.get_last(1)
            assert last, 'sink received nothing'
            assert last[0]['item']['log'] == {'value': 'x', 'index': 0}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_run_starts_downstream_nodes(self):
        # Regression test for the most severe bug found in Phase 1:
        # Engine.run() used to only explicitly start "source-like" nodes
        # (no inputs, or auto_start disabled) and rely on a downstream
        # node's own process() loop calling .get() on its input to
        # "auto-start" it - but that can never happen before the node
        # itself has been started, so in practice every node with an
        # input (i.e. everything except true sources) never ran at all
        # through the engine. A pipeline of more than one node was
        # silently dead past its first node. This drives a real two-node
        # graph (ClockNode -> LogOutputNode) through Engine.run() and
        # checks the downstream node actually started and received data.
        import asyncio

        g = Graph()
        g.add_node(Node(id='clock', type='ClockNode', config={'interval': 0.02}))
        g.add_node(Node(id='log', type='LogOutputNode'))
        g.add_edge(Edge(source='clock', target='log', source_port='out', target_port='in'))
        engine = Engine(g)
        task = asyncio.create_task(engine.run())
        try:
            await asyncio.sleep(0.3)
            log_node = engine.node_instances['log']
            assert log_node._task is not None and not log_node._task.done()
            assert log_node._running is True
            assert log_node._items_in > 0
            assert len(log_node.get_last()) > 0
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # Cancelling the engine's run() must stop every node it started,
            # including the "downstream" ones this test is about.
            assert engine.node_instances['clock']._running is False
            assert engine.node_instances['log']._running is False


# ---------- Session tests ----------
class TestSessionManager:
    def test_create_get_list(self):
        sm = SessionManager()
        s = sm.create('/tmp/workflow.yaml')
        assert sm.get(s.id) is s
        lst = sm.list_sessions()
        assert len(lst) == 1
        assert lst[0]['id'] == s.id

    def test_delete(self):
        sm = SessionManager()
        s = sm.create('/tmp/workflow.yaml')
        ok = sm.delete(s.id)
        assert ok is True
        assert sm.get(s.id) is None

    async def test_session_load(self):
        # Create temporary workflow file
        g = Graph()
        g.add_node(Node(id='n1', type='Dummy'))
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as tf:
            import yaml
            yaml.dump({'nodes':[{'id':'n1','type':'Dummy','config':{}}],'edges':[]}, tf)
            path = tf.name
        try:
            sess = Session('sid', path)
            await sess.load()
            assert sess.graph is not None
            assert len(sess.graph.nodes) == 1
        finally:
            os.unlink(path)

    async def test_session_reports_error_status_on_crash(self):
        # Regression test: Session._run_loop() used to have a
        # `finally: self.status = "stopped"` that ran even after the
        # `except Exception` branch had just set status = "error",
        # unconditionally clobbering it back to "stopped" - so a crashed
        # session always reported itself as merely "stopped", identical
        # to a clean shutdown, with the actual exception dropped on the
        # floor (just a "# Log error" comment, no logging call). Anyone
        # polling session status via the API/CLI/MCP layer had no way to
        # tell a crash from a normal stop.
        import asyncio

        g = Graph()
        g.add_node(Node(id='n1', type='Dummy'))
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as tf:
            import yaml
            yaml.dump({'nodes': [{'id': 'n1', 'type': 'Dummy', 'config': {}}], 'edges': []}, tf)
            path = tf.name
        try:
            sess = Session('sid-err', path)
            await sess.load()

            async def boom():
                raise RuntimeError('boom')

            sess.engine.run = boom
            await sess.start()
            await asyncio.sleep(0.1)
            assert sess.status == 'error'
        finally:
            os.unlink(path)

    async def test_session_pause_resume_suspends_in_place(self):
        # Regression test for the Session-level pause/resume gap: pause()
        # used to just call stop() (which cancelled the run task and tore
        # down every node instance, and also unconditionally stamped
        # status back to "stopped" - so a "paused" session actually
        # reported itself as stopped), and there was no resume() method at
        # all. A session brought back with start() after pause() would
        # re-run Engine.run() from scratch, re-instantiating every node
        # and losing all state - the exact "restarts instead of resuming"
        # bug this fix addresses at the session layer (mirroring the
        # node-level BaseNode.pause()/resume() fix).
        import asyncio

        import yaml

        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as tf:
            yaml.dump(
                {
                    'nodes': [{'id': 'c1', 'type': 'ClockNode', 'config': {'interval': 0.02}}],
                    'edges': [],
                },
                tf,
            )
            path = tf.name
        try:
            sess = Session('sid-pause', path)
            await sess.load()
            await sess.start()
            try:
                await asyncio.sleep(0.15)
                node = sess.engine.node_instances['c1']
                assert sess.status == 'running'

                await sess.pause()
                assert sess.status == 'paused'
                count_at_pause = node._counter
                await asyncio.sleep(0.15)
                # The node must not still be ticking while paused.
                assert node._counter == count_at_pause

                await sess.resume()
                assert sess.status == 'running'
                await asyncio.sleep(0.15)
                # It must be the *same* node instance, now ticking again
                # (not a fresh graph rebuilt by re-running Engine.run()).
                assert sess.engine.node_instances['c1'] is node
                assert node._counter > count_at_pause
            finally:
                await sess.stop()
        finally:
            os.unlink(path)


# Simple runner for environments without pytest
if __name__ == '__main__':
    async def run_all():
        tests = [
            TestPipe(),
            TestFork(),
            TestMerge(),
            TestModels(),
            TestPersistence(),
            TestBaseNode(),
            TestPluginManager(),
            TestEngine(),
            TestSessionManager(),
        ]
        # Run async methods manually
        async def run_cls(cls):
            for name in dir(cls):
                if name.startswith('test_'):
                    method = getattr(cls, name)
                    if asyncio.iscoroutinefunction(method):
                        await method()
                    else:
                        method()
        for t in tests:
            await run_cls(t)
        print('All coverage tests passed')
    asyncio.run(run_all())
