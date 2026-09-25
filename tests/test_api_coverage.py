"""
API endpoint coverage tests
"""
from fastapi.testclient import TestClient
from conftest import TEST_API_KEY
from pystreamflow.api.server import app

_AUTH_HEADERS = {"Authorization": f"Bearer {TEST_API_KEY}"}
client = TestClient(app, headers=_AUTH_HEADERS)

def test_health():
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json() == {'status':'ok'}

def test_version():
    r = client.get('/version')
    assert r.status_code == 200
    assert 'version' in r.json()

def test_list_nodes():
    r = client.get('/nodes')
    assert r.status_code == 200
    assert isinstance(r.json(), dict)

def test_parse_yaml():
    r = client.post('/parse_yaml', json={'yaml_text':'a: 1\nb: [1,2]'})
    assert r.status_code == 200
    data = r.json()
    assert data['ok'] is True
    assert data['data']['a'] == 1

def test_sessions_list():
    r = client.get('/sessions')
    assert r.status_code == 200
    assert 'sessions' in r.json()

def test_node_schema_endpoint():
    # Phase 2: GET /node-schema exposes core/port_schema.py's per-type
    # port names - the foundation for fixing the node editor's wires,
    # which previously always used a hardcoded in0/out0 numbering that
    # matched no real node port (see port_schema.py's module docstring).
    #
    # Phase 2 of the wire-kind-unification design (see
    # claude/design_unified_wire_kinds_plan.md) retired the automatic
    # 'raw'/'raw_<name>' output-port duplication that used to be applied
    # to every fixed (non-dynamic) output port (port_schema.py's
    # now-deleted _add_raw_pairs()) - "raw" is now a per-wire delivery
    # kind chosen on a node's one real output port, not a second port
    # every node type gets for free. So each node's declared output list
    # is now just its real ports, with no auto-added raw/raw_<name>
    # entries.
    r = client.get('/node-schema')
    assert r.status_code == 200
    schema = r.json()
    assert schema['ClockNode'] == {'inputs': '*', 'outputs': ['out']}
    assert schema['LogOutputNode'] == {'inputs': ['in'], 'outputs': ['out']}
    assert schema['RollingWindowBufferNode'] == {
        'inputs': ['in', 'retain', 'trigger'],
        'outputs': ['history', 'out'],
    }
    # ApiOutputNode's 'raw' is a real, separately hand-declared output
    # port (predates and is unrelated to the retired auto-pairing - see
    # its _OVERRIDES entry) and is unaffected by this retirement.
    assert schema['ApiOutputNode'] == {'inputs': ['in'], 'outputs': ['out', 'raw']}

def test_config_schema_endpoint():
    # Phase 3: GET /config-schema exposes core/config_schema.py's per-type
    # config-FIELD overrides - the counterpart to /node-schema (which
    # covers wiring shape, not individual config values). This is what
    # lets the node editor's Inspector render TriggerIfNode.condition as
    # a fixed-choice dropdown instead of free text, and every trigger
    # node's target_node_id as a pick-a-node-from-the-canvas dropdown
    # instead of a hand-typed id field (the "typed node ID field" gap
    # 4.3 flagged).
    r = client.get('/config-schema')
    assert r.status_code == 200
    schema = r.json()
    assert schema['TriggerIfNode']['condition'] == {
        'kind': 'enum', 'options': ['truthy', 'equals', 'contains', 'regex'],
    }
    assert schema['TriggerIfNode']['target_node_id'] == {'kind': 'node_ref'}
    assert schema['TimerTriggerNode']['target_node_id'] == {'kind': 'node_ref'}
    # Every Trigger*-family type's `action` (a lifecycle command handled by
    # BaseNode.handle_control()) is a real four-value enum, verified
    # against that method's source - not the ["auto", "manual", "off"]
    # this used to assert as a GLOBAL override for *every* node type
    # regardless of whether anything actually used those three values
    # (nothing did - see core/config_schema.py's now-corrected comment).
    assert schema['TriggerIfNode']['action'] == {
        'kind': 'enum', 'options': ['start', 'stop', 'pause', 'resume'],
    }
    assert schema['TriggerToggleNode']['action_on'] == {
        'kind': 'enum', 'options': ['start', 'stop', 'pause', 'resume'],
    }
    assert schema['TriggerToggleNode']['target_node_id'] == {'kind': 'node_ref'}
    assert 'action' not in schema['TriggerToggleNode']
    # LineBufferNode/StackNode's own `mode` fields are real, distinct
    # small enums (verified against their own source), not the invented
    # global one either.
    assert schema['LineBufferNode'] == {
        'mode': {'kind': 'enum', 'options': ['single', 'multi']},
    }
    assert schema['StackNode'] == {
        'mode': {'kind': 'enum', 'options': ['push_pop', 'peek', 'clear']},
    }
    # A node type with no known special-cased field at all now gets no
    # invented overrides - previously this asserted a global mode/action
    # enum here too, even though FileInputNode has neither key.
    assert schema['FileInputNode'] == {}

def test_workflows_rejects_edge_with_bad_port():
    # Regression test for the "wires that go nowhere" bug: /workflows used
    # to accept and save any edge at all, including the port names the
    # current (buggy) UI always sends (in0/out0), which don't match any
    # node's real input/output keys - so the saved workflow would run with
    # that wire silently carrying no data. It must now be rejected up
    # front with a clear error instead.
    wf = {
        'nodes': [
            {'id': 'a', 'type': 'ClockNode', 'config': {}},
            {'id': 'b', 'type': 'LogOutputNode', 'config': {}},
        ],
        'edges': [
            {'source': 'a', 'target': 'b', 'source_port': 'out0', 'target_port': 'in0', 'type': 'data'},
        ],
    }
    r = client.post('/workflows', json=wf)
    assert r.status_code == 200
    body = r.json()
    assert 'error' in body
    assert 'out0' in body['error']

def test_run_workflow_via_session_actually_wires_and_pause_resumes():
    # End-to-end regression test for the most severe bug found in Phase 2:
    # the node editor's "Run" button never sent the canvas's nodes/edges
    # to the backend at all - it just POSTed /nodes/{id}/start to each
    # node in isolation, so nothing was ever actually wired together no
    # matter what was drawn. ui.html's runWorkflow() now does exactly what
    # this test does: POST /workflows, then /sessions, then
    # /sessions/{id}/start - which goes through Engine.run() (fixed in
    # Phase 1 to correctly wire and start every node). A background task
    # spawned mid-request needs the ASGI app's lifespan/event loop to
    # actually outlive the request that started it, which only happens
    # inside a `with TestClient(app) as c:` block (matching a real
    # long-running uvicorn process) - a bare module-level TestClient
    # doesn't keep it alive across separate calls.
    import time
    with TestClient(app, headers=_AUTH_HEADERS) as c:
        wf = {
            'nodes': [
                {'id': 'clk-e2e', 'type': 'ClockNode', 'config': {'interval': 0.02}},
                {'id': 'sink-e2e', 'type': 'LogOutputNode', 'config': {}},
            ],
            'edges': [
                {'source': 'clk-e2e', 'target': 'sink-e2e', 'source_port': 'out', 'target_port': 'in', 'type': 'data'},
            ],
        }
        save = c.post('/workflows', json=wf).json()
        assert 'error' not in save
        sess = c.post('/sessions', json={'workflow_path': save['path']}).json()
        assert 'error' not in sess
        sid = sess['id']
        start = c.post(f'/sessions/{sid}/start').json()
        assert start['status'] == 'running'

        time.sleep(0.3)
        info = c.get(f'/sessions/{sid}').json()
        assert info['status'] == 'running'
        last = c.get('/nodes/sink-e2e/last?n=1').json()
        # The sink actually received data through the wire - not just
        # "both nodes started independently".
        assert len(last['last']) > 0
        assert 'tick' in last['last'][0]['item']['log']

        pause = c.post(f'/sessions/{sid}/pause').json()
        assert pause['status'] == 'paused'
        resume = c.post(f'/sessions/{sid}/resume').json()
        assert resume['status'] == 'running'

        c.post(f'/sessions/{sid}/stop')

if __name__ == '__main__':
    test_health()
    test_version()
    test_list_nodes()
    test_parse_yaml()
    test_sessions_list()
    test_node_schema_endpoint()
    test_config_schema_endpoint()
    test_workflows_rejects_edge_with_bad_port()
    test_run_workflow_via_session_actually_wires_and_pause_resumes()
    print('api coverage tests passed')
