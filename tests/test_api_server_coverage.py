"""
Additional coverage for pystreamflow/api/server.py - previously 51%
covered (156 of 321 statements missed). test_api_coverage.py already
covers /health, /version, /nodes, /parse_yaml, /sessions, /node-schema,
/config-schema, the /workflows bad-port rejection and one full
workflow-through-session end-to-end run; this file rounds out the many
node-CRUD, connect/disconnect, plugin, static-page and session/reflection
endpoints that weren't touched yet.
"""
import asyncio
import uuid

import pytest
import yaml
from fastapi.testclient import TestClient

from conftest import TEST_API_KEY
from pystreamflow.api.server import app
from pystreamflow.core.session_manager import session_manager
from pystreamflow.core.web_server import _nodes

_AUTH_HEADERS = {"Authorization": f"Bearer {TEST_API_KEY}"}
client = TestClient(app, headers=_AUTH_HEADERS)


def _unique_id(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:8]}'


# ---------- Static/HTML pages ----------

def test_root_serves_ui_html():
    r = client.get('/')
    assert r.status_code == 200
    assert 'text/html' in r.headers['content-type']

def test_ui_route_serves_ui_html():
    r = client.get('/ui')
    assert r.status_code == 200

def test_ui_html_cache_busts_editor_js_with_a_content_hash():
    # Regression for a real bug: StaticFiles serves /static/editor.js with
    # no Cache-Control header, and ui.html used to reference it as a bare
    # '/static/editor.js' - most browsers apply heuristic caching to that
    # (roughly proportional to file age), so a browser that had ever loaded
    # the UI before could keep serving a stale editor.js indefinitely after
    # any update to it, even across a full reload. root()/ui() now stamp
    # the URL with a hash of the file's current bytes (via
    # _versioned_static_html()) so the URL itself changes whenever the
    # content does, making a stale cached copy impossible to serve.
    import hashlib
    from pystreamflow.api.server import _static_dir

    expected = hashlib.sha256((_static_dir / 'editor.js').read_bytes()).hexdigest()[:10]
    for path in ('/', '/ui'):
        r = client.get(path)
        assert r.status_code == 200
        assert f'/static/editor.js?v={expected}' in r.text

def test_showcase_route():
    r = client.get('/showcase')
    assert r.status_code == 200

def test_example_workflow_yaml_route():
    r = client.get('/example_workflow.yaml')
    assert r.status_code == 200
    assert 'nodes' in r.text

def test_docs_route_serves_existing_file():
    r = client.get('/docs/index.md')
    assert r.status_code == 200

def test_docs_route_falls_back_to_index_for_missing_file():
    r = client.get('/docs/this-does-not-exist.md')
    assert r.status_code == 200
    # Falls back to docs/index.md's real content rather than 404ing.


# ---------- Node CRUD ----------

def test_create_node_via_api():
    node_id = _unique_id('api-node')
    try:
        r = client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
        assert r.status_code == 200
        assert r.json() == {'status': 'created', 'node_id': node_id}
        assert node_id in _nodes
    finally:
        _nodes.pop(node_id, None)


def test_create_node_defaults_to_started_active_not_idle():
    # A node added to a live graph this way used to sit inert (created
    # and registered, but never started) until something separately
    # called POST /nodes/{id}/start - editor.js's own "▶ Start"
    # context-menu action existing at all was a symptom of this: dropping
    # a node onto a running graph should read as "add this to the flow",
    # not "stage it, inactive, for later". create_node now auto-starts
    # the node immediately (asyncio.create_task(node.start())), the same
    # pattern node_start() itself already used.
    node_id = _unique_id('api-autostart')
    try:
        r = client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {'interval': 0.05}})
        assert r.status_code == 200
        import time
        deadline = time.monotonic() + 2.0
        node = _nodes[node_id]
        while not node._running and time.monotonic() < deadline:
            time.sleep(0.02)
        assert node._running is True
    finally:
        _nodes.pop(node_id, None)

def test_create_node_rejects_unknown_type():
    r = client.post('/nodes', json={'node_id': 'x', 'node_type': 'NoSuchType', 'config': {}})
    assert 'error' in r.json()

def test_create_node_rejects_duplicate_id():
    node_id = _unique_id('api-dup')
    try:
        client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
        r = client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
        assert r.json() == {'error': 'node id already exists'}
    finally:
        _nodes.pop(node_id, None)

def test_node_config_get_and_put():
    node_id = _unique_id('api-cfg')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {'interval': 1}})
    try:
        got = client.get(f'/nodes/{node_id}/config')
        assert got.json()['config']['interval'] == 1

        put = client.put(f'/nodes/{node_id}/config', json={'config': {'interval': 2}})
        assert put.json() == {'status': 'updated', 'node': node_id}

        got2 = client.get(f'/nodes/{node_id}/config')
        assert got2.json()['config']['interval'] == 2
    finally:
        _nodes.pop(node_id, None)

def test_node_config_hidden_flag():
    node_id = _unique_id('api-hidden')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode',
                                 'config': {'_secret': 'x', 'interval': 1}})
    try:
        default = client.get(f'/nodes/{node_id}/config').json()
        assert '_secret' not in default['config']
        with_hidden = client.get(f'/nodes/{node_id}/config?hidden=true').json()
        assert with_hidden['config']['_secret'] == 'x'
    finally:
        _nodes.pop(node_id, None)

def test_node_last_and_stats():
    node_id = _unique_id('api-stats')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {'interval': 0.02}})
    try:
        client.post(f'/nodes/{node_id}/start')
        import time
        time.sleep(0.1)
        last = client.get(f'/nodes/{node_id}/last?n=5')
        assert last.json()['node'] == node_id
        stats = client.get(f'/nodes/{node_id}/stats')
        assert stats.json()['node'] == node_id
        assert 'health' in stats.json()
        client.post(f'/nodes/{node_id}/stop')
    finally:
        _nodes.pop(node_id, None)

def test_node_lifecycle_endpoints_not_found():
    for method, path in [
        ('get', '/nodes/nope/config'), ('put', '/nodes/nope/config'),
        ('get', '/nodes/nope/last'), ('get', '/nodes/nope/stats'),
        ('post', '/nodes/nope/start'), ('post', '/nodes/nope/stop'),
        ('post', '/nodes/nope/pause'), ('post', '/nodes/nope/step'),
        ('post', '/nodes/nope/emit'),
    ]:
        kwargs = {'json': {'config': {}}} if path.endswith('/config') and method == 'put' else {}
        r = getattr(client, method)(path, **kwargs)
        assert r.json() == {'error': 'node not found'}

def test_node_pause_and_step_endpoints():
    node_id = _unique_id('api-pause')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
    try:
        paused = client.post(f'/nodes/{node_id}/pause')
        assert paused.json()['status'] in ('paused', 'resumed')
        stepped = client.post(f'/nodes/{node_id}/step')
        assert stepped.json()['status'] == 'stepped'
    finally:
        _nodes.pop(node_id, None)

async def test_node_reset_endpoint_clears_accumulated_state():
    # POST /nodes/{id}/reset -> BaseNode.reset(): mirrors /step exactly
    # (same fire-and-forget control-message shape), but new. Uses a
    # StackNode since it overrides reset() to also clear its own
    # accumulated `stack`, not just the generic stats/error bookkeeping.
    node_id = _unique_id('api-reset')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'StackNode', 'config': {}})
    try:
        node = _nodes[node_id]
        node.stack.extend(['a', 'b', 'c'])
        node._error_count = 3
        node._items_in = 7
        r = client.post(f'/nodes/{node_id}/reset')
        assert r.json()['status'] == 'reset'
        await asyncio.sleep(0.2)
        assert node.stack == []
        assert node._error_count == 0
        assert node._items_in == 0
    finally:
        _nodes.pop(node_id, None)

def test_node_reset_endpoint_missing_node():
    r = client.post('/nodes/does-not-exist/reset')
    assert r.json() == {'error': 'node not found'}

def test_manual_emit_endpoint_replays_last_output():
    # The new title-bar "manual emit" button's backend: POST
    # /nodes/{id}/emit re-emits each output port's most recently emitted
    # item again, synchronously, and reports what it replayed.
    node_id = _unique_id('api-emit')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ConstantValueNode', 'config': {'value': 'hello'}})
    try:
        import time
        time.sleep(0.1)  # let it emit at least once on its own
        r = client.post(f'/nodes/{node_id}/emit')
        body = r.json()
        assert body['status'] == 'emitted'
        assert body['node'] == node_id
        assert body['replayed'].get('out') == 'hello'
    finally:
        _nodes.pop(node_id, None)

def test_manual_emit_endpoint_on_fresh_node_replays_nothing():
    node_id = _unique_id('api-emit-fresh')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'MergeNode', 'config': {}})
    try:
        r = client.post(f'/nodes/{node_id}/emit')
        assert r.json() == {'status': 'emitted', 'node': node_id, 'replayed': {}}
    finally:
        _nodes.pop(node_id, None)

def test_delete_node():
    node_id = _unique_id('api-del')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
    r = client.delete(f'/nodes/{node_id}')
    assert r.json() == {'status': 'deleted', 'node_id': node_id}
    assert node_id not in _nodes

def test_delete_node_not_found():
    r = client.delete('/nodes/does-not-exist')
    assert r.json() == {'error': 'node not found'}


# ---------- Connect / disconnect ----------

def test_connect_and_disconnect_nodes_via_api():
    src_id, tgt_id = _unique_id('api-src'), _unique_id('api-tgt')
    client.post('/nodes', json={'node_id': src_id, 'node_type': 'ClockNode', 'config': {}})
    client.post('/nodes', json={'node_id': tgt_id, 'node_type': 'LogOutputNode', 'config': {}})
    try:
        connected = client.post('/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in',
        })
        assert connected.json() == {'status': 'connected'}
        assert 'out' in _nodes[src_id].outputs

        disconnected = client.request('DELETE', '/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in',
        })
        assert disconnected.json() == {'status': 'disconnected'}
        assert 'out' not in _nodes[src_id].outputs
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)

async def test_connect_same_output_fans_out_to_two_targets_and_disconnect_leaves_the_other():
    # Phase 1 of the wire-kind-unification design (see
    # claude/design_unified_wire_kinds_plan.md): BaseNode.add_output() used
    # to be a plain overwriting `self.outputs[name] = pipe` - wiring a
    # second consumer from the same source_port silently stole the pipe
    # away from whichever consumer was wired first, with no error. Two
    # separate /nodes/connect calls from the same source_port to two
    # different targets must both stay live and both receive every
    # emitted item, and disconnecting one must not affect the other.
    #
    # Written as `async def`, calling the real route coroutines
    # (create_node/connect_nodes/disconnect_nodes) directly rather than
    # through client.post(...)/client.request(...), for the same reason
    # test_connect_attribute_edge_actually_pumps_values_live() above is:
    # TestClient without an explicit `with TestClient(app) as client:`
    # block spins up a brand-new throwaway event loop per call and tears
    # it down - force-cancelling every task still pending on it, LogOutputNode's
    # own process() loop included - the moment that one HTTP call returns.
    # This test needs both targets' process() loops to actually still be
    # running when src.emit() fires, well after the calls that created
    # them, so it can't go through client.post(...) for node creation.
    from pystreamflow.api.server import (
        ConnectReq,
        NodeCreateReq,
        connect_nodes,
        create_node,
        disconnect_nodes,
    )

    src_id = _unique_id('api-fanout-src')
    tgt1_id, tgt2_id = _unique_id('api-fanout-tgt1'), _unique_id('api-fanout-tgt2')
    # UserInputNode's process() loop is a quiet no-op (it only ever emits
    # via an HTTP submission or a manual emit) - deliberately chosen over
    # e.g. ClockNode so this test's own src.emit() calls are the only
    # things ever putting an item on 'out', with no real periodic tick
    # racing the assertions below.
    await create_node(NodeCreateReq(node_id=src_id, node_type='UserInputNode', config={}))
    await create_node(NodeCreateReq(node_id=tgt1_id, node_type='LogOutputNode', config={}))
    await create_node(NodeCreateReq(node_id=tgt2_id, node_type='LogOutputNode', config={}))
    try:
        for tgt_id in (tgt1_id, tgt2_id):
            result = await connect_nodes(ConnectReq(
                source_id=src_id, source_port='out', target_id=tgt_id, target_port='in',
            ))
            assert result == {'status': 'connected'}

        src, tgt1, tgt2 = _nodes[src_id], _nodes[tgt1_id], _nodes[tgt2_id]
        assert len(src.outputs['out']) == 2

        # Probe each target's own re-emission (LogOutputNode logs and
        # forwards whatever it receives on 'in' as {'level', 'log'} on
        # 'out') - a real downstream consumer of the fan-out, added the
        # same way any other wire would be, exercising add_output() again
        # rather than reaching into internals.
        from pystreamflow.core.stream import Pipe
        probe1, probe2 = Pipe(), Pipe()
        tgt1.add_output('out', probe1)
        tgt2.add_output('out', probe2)

        item = {'probe': 'fanout'}
        src.emit('out', item)
        got1 = await asyncio.wait_for(probe1.get(), timeout=2.0)
        got2 = await asyncio.wait_for(probe2.get(), timeout=2.0)
        assert got1['log'] == item
        assert got2['log'] == item

        # Disconnect just the first edge - the second must remain live and
        # keep receiving, and the first target must not receive anything
        # further.
        disconnect_result = await disconnect_nodes(ConnectReq(
            source_id=src_id, source_port='out', target_id=tgt1_id, target_port='in',
        ))
        assert disconnect_result == {'status': 'disconnected'}
        assert len(src.outputs['out']) == 1

        item2 = {'probe': 'fanout-2'}
        src.emit('out', item2)
        got2_again = await asyncio.wait_for(probe2.get(), timeout=2.0)
        assert got2_again['log'] == item2
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(probe1.get(), timeout=0.3)
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt1_id, None)
        _nodes.pop(tgt2_id, None)


def test_connect_nodes_missing_source_or_target():
    r = client.post('/nodes/connect', json={
        'source_id': 'nope', 'source_port': 'out', 'target_id': 'also-nope', 'target_port': 'in',
    })
    assert r.json() == {'error': 'source or target not found'}

def test_connect_nodes_rejects_bad_port():
    src_id, tgt_id = _unique_id('api-badport-src'), _unique_id('api-badport-tgt')
    client.post('/nodes', json={'node_id': src_id, 'node_type': 'ClockNode', 'config': {}})
    client.post('/nodes', json={'node_id': tgt_id, 'node_type': 'LogOutputNode', 'config': {}})
    try:
        r = client.post('/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out0',
            'target_id': tgt_id, 'target_port': 'in0',
        })
        assert 'error' in r.json()
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)

def test_connect_control_edge_records_graph_control_target():
    src_id, tgt_id = _unique_id('api-ctrl-src'), _unique_id('api-ctrl-tgt')
    client.post('/nodes', json={'node_id': src_id, 'node_type': 'TriggerOnNode', 'config': {}})
    client.post('/nodes', json={'node_id': tgt_id, 'node_type': 'ClockNode', 'config': {}})
    try:
        r = client.post('/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert r.json() == {'status': 'connected'}
        assert tgt_id in _nodes[src_id]._graph_control_targets

        client.request('DELETE', '/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert tgt_id not in _nodes[src_id]._graph_control_targets
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)


def test_connect_control_edge_lands_on_reserved_port_not_real_data_port():
    # Regression test for a real bug found alongside the same one in
    # Engine._wire_edges(): this endpoint used to call
    # `tgt.add_input(req.target_port, pipe)` unconditionally for a
    # 'control'-type edge too, so wiring a trigger's output onto a
    # target's real 'in' data port through this live-node API silently
    # replaced that port's real data pipe with the trigger's own output
    # pipe. A control edge must always land on the target's reserved
    # 'control' pipe instead, regardless of what target_port the request
    # specifies, and must never touch a real data port that's already
    # wired under that same target_port name.
    src_id, tgt_id = _unique_id('api-ctrl-src2'), _unique_id('api-ctrl-tgt2')
    client.post('/nodes', json={'node_id': src_id, 'node_type': 'TriggerOnNode', 'config': {}})
    client.post('/nodes', json={'node_id': tgt_id, 'node_type': 'LogOutputNode', 'config': {}})
    try:
        tgt = _nodes[tgt_id]
        # Simulate a real data edge already wired onto 'in'.
        from pystreamflow.core.stream import Pipe
        tgt.add_input('in', Pipe())
        real_data_pipe_wrapper_before = tgt.inputs['in']

        r = client.post('/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert r.json() == {'status': 'connected'}
        # The real data pipe on 'in' must be untouched...
        assert tgt.inputs['in'] is real_data_pipe_wrapper_before
        # ...and the control message pipe must have landed on the
        # reserved _control_pipes list BaseNode.add_input('control', ...)
        # special-cases - it never even enters tgt.inputs at all, which
        # is the strongest possible guarantee it can't collide with any
        # real data port regardless of name.
        assert len(tgt._control_pipes) == 1

        client.request('DELETE', '/nodes/connect', json={
            'source_id': src_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        # Disconnecting the control edge must not have touched 'in'...
        assert tgt.inputs['in'] is real_data_pipe_wrapper_before
        # ...and must have removed exactly that one control source.
        assert tgt._control_pipes == []
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)


def test_two_control_edges_onto_the_same_target_both_survive():
    # Regression test for the single-incoming-control-pipe-per-target
    # limitation flagged as an open question in
    # claude/design_unified_wire_kinds_plan.md: wiring a second control
    # edge onto a node that already has one used to silently steal
    # control away from the first source (BaseNode.add_input('control',
    # ...) was a plain overwrite) - the same bug class Phase 1 already
    # fixed on the data-output fan-out side. Two Trigger sources wired to
    # the same target must both keep working, and disconnecting one must
    # leave the other's control channel intact.
    src1_id, src2_id, tgt_id = (
        _unique_id('api-2ctrl-src1'), _unique_id('api-2ctrl-src2'), _unique_id('api-2ctrl-tgt'),
    )
    client.post('/nodes', json={'node_id': src1_id, 'node_type': 'TriggerOnNode', 'config': {}})
    client.post('/nodes', json={'node_id': src2_id, 'node_type': 'TriggerOffNode', 'config': {}})
    client.post('/nodes', json={'node_id': tgt_id, 'node_type': 'ClockNode', 'config': {}})
    try:
        r1 = client.post('/nodes/connect', json={
            'source_id': src1_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert r1.json() == {'status': 'connected'}
        r2 = client.post('/nodes/connect', json={
            'source_id': src2_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert r2.json() == {'status': 'connected'}
        tgt = _nodes[tgt_id]
        # Both sources must be real, distinct, live control inputs - not
        # one overwriting the other.
        assert len(tgt._control_pipes) == 2

        # Disconnecting src2's edge must remove only src2's pipe.
        client.request('DELETE', '/nodes/connect', json={
            'source_id': src2_id, 'source_port': 'out',
            'target_id': tgt_id, 'target_port': 'in', 'type': 'control',
        })
        assert len(tgt._control_pipes) == 1
    finally:
        _nodes.pop(src1_id, None)
        _nodes.pop(src2_id, None)
        _nodes.pop(tgt_id, None)


async def test_two_data_edges_into_the_same_input_both_survive_and_disconnect_leaves_the_other():
    # Regression test for the third side of the same single-slot bug
    # already fixed for outputs (Phase 1, "fan-out") and control wires
    # (the reset/sessions/auth round, "control-pipe fan-in"): a regular
    # *data* input port used to be a single overwritable slot too -
    # `tgt.add_input(req.target_port, pipe)` did a plain
    # `self.inputs[name] = <wrapped pipe>` - so a second /nodes/connect
    # call wiring a different source onto the same target_port silently
    # stole the connection from whichever source was wired first, with no
    # error. Two UserInputNode sources wired to the same LogOutputNode
    # target's 'in' port must both keep delivering, and disconnecting one
    # must leave the other's delivery intact.
    #
    # async def + calling the route coroutines directly, for the same
    # reason the output-fan-out test above does: this needs both a
    # background emit and the target's own process() loop still alive
    # well after the connect calls return, which TestClient's own
    # throwaway per-request event loop wouldn't preserve.
    from pystreamflow.api.server import (
        ConnectReq,
        NodeCreateReq,
        connect_nodes,
        create_node,
        disconnect_nodes,
    )
    from pystreamflow.core.stream import Pipe

    src1_id, src2_id, tgt_id = (
        _unique_id('api-fanin-src1'), _unique_id('api-fanin-src2'), _unique_id('api-fanin-tgt'),
    )
    await create_node(NodeCreateReq(node_id=src1_id, node_type='UserInputNode', config={}))
    await create_node(NodeCreateReq(node_id=src2_id, node_type='UserInputNode', config={}))
    await create_node(NodeCreateReq(node_id=tgt_id, node_type='LogOutputNode', config={}))
    try:
        for src_id in (src1_id, src2_id):
            result = await connect_nodes(ConnectReq(
                source_id=src_id, source_port='out', target_id=tgt_id, target_port='in',
            ))
            assert result == {'status': 'connected'}

        src1, src2, tgt = _nodes[src1_id], _nodes[src2_id], _nodes[tgt_id]
        assert len(tgt._input_sources['in']) == 2

        probe = Pipe()
        tgt.add_output('out', probe)

        src1.emit('out', {'value': 'from-src1'})
        src2.emit('out', {'value': 'from-src2'})
        got1 = await asyncio.wait_for(probe.get(), timeout=2.0)
        got2 = await asyncio.wait_for(probe.get(), timeout=2.0)
        assert {got1['log']['value'], got2['log']['value']} == {'from-src1', 'from-src2'}

        # Disconnect just src1's edge - src2's must remain live and keep
        # delivering, and src1's must no longer reach the target.
        disconnect_result = await disconnect_nodes(ConnectReq(
            source_id=src1_id, source_port='out', target_id=tgt_id, target_port='in',
        ))
        assert disconnect_result == {'status': 'disconnected'}
        assert len(tgt._input_sources['in']) == 1

        src2.emit('out', {'value': 'from-src2-again'})
        got_again = await asyncio.wait_for(probe.get(), timeout=2.0)
        assert got_again['log']['value'] == 'from-src2-again'

        src1.emit('out', {'value': 'should-not-arrive'})
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(probe.get(), timeout=0.3)
    finally:
        _nodes.pop(src1_id, None)
        _nodes.pop(src2_id, None)
        _nodes.pop(tgt_id, None)


async def test_connect_attribute_edge_actually_pumps_values_live():
    # Regression test for a real bug (feedback: "only by clicking the run
    # button some states are updated (attribute changes including wiring
    # changes)"): before this fix, connect_nodes() had no special handling
    # for an 'attribute'-type edge at all - it fell into the generic
    # `tgt.add_input(req.target_port, pipe)` branch, landing the pipe in
    # tgt.inputs under an arbitrary attribute name that nothing in the
    # target's own process() loop ever reads. A live attribute wire drawn
    # directly between two already-running ad-hoc nodes (as opposed to
    # one loaded as part of a whole Session's workflow, where
    # Engine._wire_edges() has always spawned a real pump task) therefore
    # had zero effect until "Run" threw the graph away and rebuilt it as a
    # Session. core.engine._pump_attribute() (refactored to a plain
    # module-level function specifically so this endpoint could reuse it)
    # is now spawned here the same way Engine._wire_edges() spawns it.
    #
    # Written as a direct `async def` test calling the endpoint coroutines
    # in-process, sharing this one test's event loop throughout, rather
    # than going through TestClient/`client.post(...)`: TestClient without
    # an explicit `with TestClient(app) as client:` block (which is how
    # `client` is constructed at module scope in this file) spins up a
    # brand-new throwaway event loop per call and tears it down - including
    # force-cancelling every asyncio task still pending on it - the moment
    # that one HTTP call returns (see starlette.testclient's
    # `_portal_factory`/`start_blocking_portal`). A real deployment has
    # exactly one long-lived event loop for the server's whole lifetime, so
    # that per-call teardown is a TestClient artifact, not a real-world
    # behavior - but it means any test that needs a background task (like
    # the attribute pump) to actually still be alive *after* the request
    # that created it returns cannot go through `client.post(...)` here.
    import asyncio

    from pystreamflow.api.server import (
        ConnectReq,
        NodeCreateReq,
        _attribute_pump_tasks,
        connect_nodes,
        create_node,
        disconnect_nodes,
    )

    src_id, tgt_id = _unique_id('api-attr-src'), _unique_id('api-attr-tgt')
    await create_node(NodeCreateReq(
        node_id=src_id, node_type='ConstantValueNode',
        config={'value': 'picked-path', 'repeat': True, 'interval': 0.02},
    ))
    await create_node(NodeCreateReq(
        node_id=tgt_id, node_type='FileInputNode', config={'path': '/dev/null'},
    ))
    try:
        result = await connect_nodes(ConnectReq(
            source_id=src_id, source_port='out',
            target_id=tgt_id, target_port='path', type='attribute',
        ))
        assert result == {'status': 'connected'}

        tgt = _nodes[tgt_id]
        deadline = asyncio.get_event_loop().time() + 2.0
        while tgt.config.get('path') != 'picked-path' and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.02)
        assert tgt.config.get('path') == 'picked-path'
        # The attribute pipe must not have landed as an ordinary named
        # input either - nothing should ever read self.inputs['path'].
        assert 'path' not in tgt.inputs

        key = (src_id, tgt_id, 'path')
        assert key in _attribute_pump_tasks
        task = _attribute_pump_tasks[key]
        assert not task.done()

        disconnect_result = await disconnect_nodes(ConnectReq(
            source_id=src_id, source_port='out',
            target_id=tgt_id, target_port='path', type='attribute',
        ))
        assert disconnect_result == {'status': 'disconnected'}
        # Disconnecting must cancel the pump task, not leak it.
        assert key not in _attribute_pump_tasks
        # Give the cancellation a moment to actually land before the test
        # tears the nodes down out from under it.
        await asyncio.sleep(0.02)
        assert task.cancelled() or task.done()
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)


async def test_delete_node_cancels_its_attribute_pump_tasks():
    # Regression test: deleting a node without first calling DELETE
    # /nodes/connect on every attribute edge touching it (the normal case
    # - the editor deletes a node and its links together, not
    # edge-by-edge first) used to leak that edge's _pump_attribute() task
    # forever - nothing else ever cancelled it, and the task itself loops
    # on `await pipe.get()` with no way to notice its target node is gone.
    # See the test above for why this calls the endpoint coroutines
    # directly instead of going through TestClient.
    import asyncio

    from pystreamflow.api.server import (
        ConnectReq,
        NodeCreateReq,
        _attribute_pump_tasks,
        connect_nodes,
        create_node,
        delete_node,
    )

    src_id, tgt_id = _unique_id('api-attr-del-src'), _unique_id('api-attr-del-tgt')
    await create_node(NodeCreateReq(node_id=src_id, node_type='ConstantValueNode', config={'value': 'x'}))
    await create_node(NodeCreateReq(node_id=tgt_id, node_type='FileInputNode', config={'path': '/dev/null'}))
    try:
        await connect_nodes(ConnectReq(
            source_id=src_id, source_port='out',
            target_id=tgt_id, target_port='path', type='attribute',
        ))
        key = (src_id, tgt_id, 'path')
        assert key in _attribute_pump_tasks
        task = _attribute_pump_tasks[key]

        await delete_node(tgt_id)
        assert key not in _attribute_pump_tasks
        await asyncio.sleep(0.02)
        assert task.cancelled() or task.done()
    finally:
        _nodes.pop(src_id, None)
        _nodes.pop(tgt_id, None)


# ---------- Workflows / plugins / yaml ----------

def test_list_workflows_includes_a_saved_one():
    wf = {
        'nodes': [{'id': 'a', 'type': 'ClockNode', 'config': {}}],
        'edges': [],
    }
    saved = client.post('/workflows', json=wf).json()
    assert 'error' not in saved
    listing = client.get('/workflows').json()
    import os
    assert os.path.basename(saved['path']) in listing['workflows']

def test_subgraph_file_round_trip(tmp_path):
    # Coverage for GET/POST /subgraph-file (Request C9: "there should be
    # some way to view/edit subgraphs" - these two endpoints are what lets
    # the node editor open a SubgraphNode's own embedded workflow file for
    # editing, and save it back).
    path = str(tmp_path / 'inner.yaml')
    write_resp = client.post('/subgraph-file', json={
        'path': path,
        'nodes': [
            {'id': 'a', 'type': 'ClockNode', 'config': {'interval': 1.0}, 'label': 'Clock', 'x': 10, 'y': 20},
            {'id': 'b', 'type': 'LogOutputNode', 'config': {}, 'label': 'Log', 'x': 200, 'y': 20},
        ],
        'edges': [
            {'source': 'a', 'target': 'b', 'source_port': 'out', 'target_port': 'in', 'type': 'data'},
        ],
    }).json()
    assert write_resp == {'status': 'saved', 'path': path}
    import os
    assert os.path.isfile(path)

    read_resp = client.get('/subgraph-file', params={'path': path}).json()
    assert 'error' not in read_resp
    ids = {n['id'] for n in read_resp['nodes']}
    assert ids == {'a', 'b'}
    node_a = next(n for n in read_resp['nodes'] if n['id'] == 'a')
    # Layout (x/y/label) round-trips via Graph.meta['editor_layout'],
    # even though core/models.py's Node dataclass itself has no such
    # fields.
    assert node_a['x'] == 10 and node_a['y'] == 20 and node_a['label'] == 'Clock'
    assert node_a['config'] == {'interval': 1.0}
    assert len(read_resp['edges']) == 1
    assert read_resp['edges'][0] == {
        'source': 'a', 'target': 'b', 'source_port': 'out', 'target_port': 'in', 'type': 'data',
    }

    # The plain YAML the SubgraphNode itself will actually load at
    # runtime must still parse with a bare Node(**n)/Edge(**e) - i.e. the
    # layout metadata must not have leaked into the nodes:/edges: lists
    # themselves.
    from pystreamflow.core.persistence import load_workflow
    graph = load_workflow(path)
    assert {n.id for n in graph.nodes} == {'a', 'b'}


def test_subgraph_file_read_missing_file():
    r = client.get('/subgraph-file', params={'path': '/nonexistent/path/does-not-exist.yaml'})
    assert 'error' in r.json()


def test_subgraph_file_write_rejects_bad_edge():
    r = client.post('/subgraph-file', json={
        'path': '/tmp/should_not_be_written.yaml',
        'nodes': [{'id': 'a', 'type': 'ClockNode', 'config': {}}],
        'edges': [{'source': 'a', 'target': 'missing', 'source_port': 'out', 'target_port': 'in', 'type': 'data'}],
    })
    assert 'error' in r.json()


def test_list_plugins():
    r = client.get('/plugins')
    assert r.status_code == 200
    assert 'plugins' in r.json()

def test_parse_yaml_error_path():
    r = client.post('/parse_yaml', json={'yaml_text': '{unclosed: ['})
    body = r.json()
    assert body['ok'] is False
    assert 'error' in body


# ---------- Sessions ----------

def test_session_not_found_paths():
    for method, path in [
        ('get', '/sessions/nope'), ('post', '/sessions/nope/start'),
        ('post', '/sessions/nope/stop'), ('post', '/sessions/nope/pause'),
        ('post', '/sessions/nope/resume'), ('get', '/sessions/nope/workflow'),
    ]:
        r = getattr(client, method)(path)
        assert r.json()['error'] in ('not found',)

def test_delete_session_not_found_returns_false():
    r = client.delete('/sessions/nope')
    assert r.json() == {'deleted': False}

def test_session_workflow_endpoint(tmp_path):
    wf = {
        'nodes': [{'id': 'a', 'type': 'ClockNode', 'config': {}}],
        'edges': [],
    }
    path = tmp_path / 'wf.yaml'
    path.write_text(yaml.dump(wf))
    created = client.post('/sessions', json={'workflow_path': str(path)}).json()
    sid = created['id']
    try:
        wf_view = client.get(f'/sessions/{sid}/workflow')
        assert wf_view.status_code == 200
        assert wf_view.json()['nodes'][0]['id'] == 'a'
    finally:
        session_manager.delete(sid)


# ---------- Reflection ----------

def test_reflection_endpoints():
    node_id = _unique_id('api-reflect')
    client.post('/nodes', json={'node_id': node_id, 'node_type': 'ClockNode', 'config': {}})
    try:
        nodes = client.get('/reflection/nodes').json()
        assert node_id in nodes['nodes']

        detail = client.get(f'/reflection/nodes/{node_id}').json()
        assert detail['id'] == node_id
        assert detail['type'] == 'ClockNode'

        graph = client.get('/reflection/graph').json()
        ids = [n['id'] for n in graph['nodes']]
        assert node_id in ids
    finally:
        _nodes.pop(node_id, None)

def test_reflection_node_not_found():
    r = client.get('/reflection/nodes/does-not-exist')
    assert r.json() == {'error': 'node not found'}


# ---------- Mounted MCP app ----------

def test_mcp_mounted_under_slash_mcp():
    r = client.get('/mcp/health')
    assert r.status_code == 200
    assert r.json()['service'] == 'mcp'
