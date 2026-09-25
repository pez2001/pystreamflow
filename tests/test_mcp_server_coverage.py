"""
Coverage for pystreamflow/mcp/server.py - the single biggest coverage gap
in the project before this pass (289 statements, 11% covered: only the
module import itself and a couple of incidental hits from other tests
importing api/server.py, which pulls this module in). Almost none of its
~24 MCP tools (get_version, list_nodes, node lifecycle, graph editing,
session management) or its /call, /tools, /messages (JSON-RPC), /sse and
auth-gating endpoints had ever been exercised.

These tests drive the real FastAPI app via TestClient and the real shared
`_nodes` registry (pystreamflow.core.web_server._nodes) - the same
process-global dict this module, core/web_server.py and api/server.py all
share - so nodes created here are visible to the tool calls under test,
exactly as they would be through the real API/editor.

pystreamflow/mcp/server.py was rewritten on the official `mcp` Python SDK
(task: LM Studio couldn't connect - "SSE error: Non-200 status code (404)"
against http://mcp.lan:8000/mcp - the old hand-rolled /sse+/messages pair
was not a spec-compliant MCP transport at all: no real `initialize`
handshake, no SSE `event: endpoint` handshake message, and no route for a
bare /mcp). The old /messages (JSON-RPC) and /sse endpoints, and the
functions behind them (sse_event_generator, sse_endpoint,
messages_endpoint), no longer exist - they've been replaced by the SDK's
own real Streamable HTTP transport (mounted at "/", i.e. this module's own
`app` root) and real SSE transport (mounted at /sse + /messages/). The
tests that used to exercise those functions directly are replaced below
with tests that drive the real transports end-to-end through a live
uvicorn server (using the actual `mcp` SDK client, exactly as LM Studio or
any other real MCP client would) rather than reaching into now-deleted
internals.
"""

import asyncio

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver.exceptions import ToolError

from pystreamflow.core.session_manager import session_manager
from pystreamflow.core.web_server import _nodes
from pystreamflow.mcp.server import (
    MCP_API_KEY,
    ToolCall,
    app,
    call_tool,
    require_auth,
)

# live_mcp_server / live_mcp_server_with_key (a real pystreamflow.mcp.server
# run as its own OS subprocess) are defined in conftest.py, shared with
# test_new_feature_nodes.py - see that fixture's docstring for why a real
# subprocess is required rather than an in-process server.

client = TestClient(app)


def _call(tool, **arguments):
    r = client.post('/call', json={'tool': tool, 'arguments': arguments})
    assert r.status_code == 200
    return r.json()


@pytest.fixture
def clock_node():
    # ClockNode is cheap, self-contained and already used elsewhere in the
    # suite as the "trivial real node" fixture of choice.
    result = _call('create_node', node_id='mcp-clk', node_type='ClockNode',
                    config={'interval': 0.02})
    assert result['result']['status'] == 'created'
    yield 'mcp-clk'
    _nodes.pop('mcp-clk', None)


def test_health():
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json() == {'status': 'ok', 'service': 'mcp'}


def test_tools_list_endpoint():
    r = client.get('/tools')
    assert r.status_code == 200
    tools = r.json()['tools']
    names = {t['name'] for t in tools}
    assert 'get_version' in names
    assert 'create_session' in names


def test_get_version_tool():
    result = _call('get_version')
    assert result['result']['name'] == 'pystreamflow'
    assert 'version' in result['result']


def test_unknown_tool_returns_error():
    result = _call('not_a_real_tool')
    assert result == {'error': 'unknown tool'}


def test_node_lifecycle_via_tools(clock_node):
    node_id = clock_node

    listed = _call('list_nodes')
    assert node_id in listed['result']

    cfg = _call('get_node_config', node_id=node_id)
    assert cfg['result']['node'] == node_id

    updated = _call('update_node_config', node_id=node_id, config={'interval': 0.5})
    assert updated['result']['status'] == 'updated'
    assert _nodes[node_id].config['interval'] == 0.5

    started = _call('node_start', node_id=node_id)
    assert started['result']['status'] == 'started'

    stats = _call('get_node_stats', node_id=node_id)
    assert stats['result']['node'] == node_id
    assert 'health' in stats['result']

    last = _call('get_node_last', node_id=node_id, n=5)
    assert last['result']['node'] == node_id

    paused = _call('node_pause', node_id=node_id)
    assert paused['result']['status'] in ('paused', 'resumed')

    stepped = _call('node_step', node_id=node_id)
    # BaseNode has handle_control, so this should always succeed for a
    # real node type.
    assert stepped['result']['status'] == 'stepped'

    emitted = _call('node_emit', node_id=node_id)
    # Mirrors POST /nodes/{id}/emit (api/server.py) - manual_emit()
    # replays whatever ClockNode has already emitted on 'out' by now.
    assert emitted['result']['status'] == 'emitted'
    assert emitted['result']['node'] == node_id
    assert 'replayed' in emitted['result']

    reset = _call('node_reset', node_id=node_id)
    # Mirrors POST /nodes/{id}/reset (api/server.py) - see
    # BaseNode.reset()'s docstring for what gets cleared.
    assert reset['result']['status'] == 'reset'
    assert reset['result']['node'] == node_id

    stopped = _call('node_stop', node_id=node_id)
    assert stopped['result']['status'] == 'stopped'


def test_node_reset_tool_clears_accumulated_state():
    # Uses a StackNode since it overrides reset() to clear its own
    # accumulated `stack`, not just the generic stats/error bookkeeping -
    # a stronger check than the smoke test above (which only checks the
    # status string).
    node_id = 'mcp-reset-stack'
    _call('create_node', node_id=node_id, node_type='StackNode', config={})
    try:
        from pystreamflow.core.web_server import _nodes
        node = _nodes[node_id]
        node.stack.extend(['x', 'y'])
        node._error_count = 2
        result = _call('node_reset', node_id=node_id)
        assert result['result']['status'] == 'reset'
        import time
        time.sleep(0.2)
        assert node.stack == []
        assert node._error_count == 0
    finally:
        _call('delete_node', node_id=node_id)


def test_node_tools_report_not_found_for_missing_node():
    for tool in ('get_node_last', 'get_node_stats', 'get_node_config',
                 'node_start', 'node_stop', 'node_pause', 'node_step',
                 'node_emit', 'node_reset', 'reflect_node'):
        result = _call(tool, node_id='does-not-exist')
        assert result == {'error': 'node not found'}
    assert _call('update_node_config', node_id='does-not-exist', config={}) == {
        'error': 'node not found'}


def test_send_to_node_with_wired_input():
    from pystreamflow.core.stream import Pipe

    _call('create_node', node_id='mcp-sink', node_type='LogOutputNode', config={})
    try:
        _nodes['mcp-sink'].add_input('in', Pipe())
        result = _call('send_to_node', node_id='mcp-sink', payload={'x': 1})
        assert result['result']['status'] == 'sent'
    finally:
        _nodes.pop('mcp-sink', None)


def test_send_to_node_with_no_input_emits_directly():
    _call('create_node', node_id='mcp-src', node_type='ClockNode', config={})
    try:
        result = _call('send_to_node', node_id='mcp-src', payload={'x': 1})
        assert result['result']['status'] == 'emitted'
    finally:
        _nodes.pop('mcp-src', None)


def test_reflect_nodes_and_reflect_graph(clock_node):
    reflected = _call('reflect_nodes')
    assert clock_node in reflected['result']['nodes']
    entry = reflected['result']['nodes'][clock_node]
    assert entry['type'] == 'ClockNode'
    assert 'health' in entry

    graph = _call('reflect_graph')
    ids = [n['id'] for n in graph['result']['nodes']]
    assert clock_node in ids


def test_reflect_node_detail(clock_node):
    result = _call('reflect_node', node_id=clock_node)
    assert result['result']['id'] == clock_node
    assert result['result']['type'] == 'ClockNode'
    assert 'inputs' in result['result']
    assert 'outputs' in result['result']


def test_create_node_rejects_unknown_type():
    result = _call('create_node', node_id='x', node_type='NoSuchNodeType', config={})
    assert 'error' in result


def test_create_node_rejects_duplicate_id(clock_node):
    result = _call('create_node', node_id=clock_node, node_type='ClockNode', config={})
    assert result == {'error': 'node id already exists'}


def test_create_node_defaults_to_started_active_not_idle():
    # Matches api/server.py's identical create_node fix (see its own
    # test/comment in test_api_server_coverage.py): a node created via
    # this MCP tool used to sit inert until something separately called
    # the start_node tool - now it's auto-started immediately.
    import time
    node_id = 'mcp-autostart'
    try:
        result = _call('create_node', node_id=node_id, node_type='ClockNode', config={'interval': 0.05})
        assert result['result']['status'] == 'created'
        node = _nodes[node_id]
        deadline = time.monotonic() + 2.0
        while not node._running and time.monotonic() < deadline:
            time.sleep(0.02)
        assert node._running is True
    finally:
        _nodes.pop(node_id, None)


def test_delete_node(clock_node):
    result = _call('delete_node', node_id=clock_node)
    assert result['result']['status'] == 'deleted'
    assert clock_node not in _nodes


def test_delete_node_not_found():
    assert _call('delete_node', node_id='nope') == {'error': 'node not found'}


def test_connect_and_disconnect_nodes():
    _call('create_node', node_id='mcp-a', node_type='ClockNode', config={})
    _call('create_node', node_id='mcp-b', node_type='LogOutputNode', config={})
    try:
        connected = _call('connect_nodes', source_id='mcp-a', source_port='out',
                           target_id='mcp-b', target_port='in')
        assert connected['result']['status'] == 'connected'
        assert 'out' in _nodes['mcp-a'].outputs
        assert 'in' in _nodes['mcp-b'].inputs

        disconnected = _call('disconnect_nodes', source_id='mcp-a', source_port='out',
                              target_id='mcp-b', target_port='in')
        assert disconnected['result']['status'] == 'disconnected'
        assert 'out' not in _nodes['mcp-a'].outputs
        assert 'in' not in _nodes['mcp-b'].inputs
    finally:
        _nodes.pop('mcp-a', None)
        _nodes.pop('mcp-b', None)


def test_connect_nodes_missing_source_or_target():
    result = _call('connect_nodes', source_id='nope', source_port='out',
                   target_id='also-nope', target_port='in')
    assert result == {'error': 'source or target not found'}


@pytest.fixture
def workflow_file(tmp_path):
    wf = {
        'nodes': [
            {'id': 'mcp-wf-src', 'type': 'ClockNode', 'config': {'interval': 0.02}},
            {'id': 'mcp-wf-sink', 'type': 'LogOutputNode', 'config': {}},
        ],
        'edges': [
            {'source': 'mcp-wf-src', 'target': 'mcp-wf-sink',
             'source_port': 'out', 'target_port': 'in', 'type': 'data'},
        ],
    }
    path = tmp_path / 'mcp_test_workflow.yaml'
    path.write_text(yaml.dump(wf))
    yield str(path)


def test_session_lifecycle_via_tools(workflow_file):
    # As in test_api_coverage.py's run_workflow test: a session's engine
    # runs as a background task spawned mid-request, which only survives
    # across separate TestClient calls inside a `with TestClient(app) as
    # c:` block (matching a real long-running uvicorn process) - a bare
    # module-level TestClient lets it get torn down between calls, which
    # was making the session go straight from "running" to "stopped".
    with TestClient(app) as c:
        def call(tool, **arguments):
            r = c.post('/call', json={'tool': tool, 'arguments': arguments})
            assert r.status_code == 200
            return r.json()

        created = call('create_session', workflow_path=workflow_file)
        sid = created['result']['session_id']
        assert created['result']['status'] == 'created'

        listed = call('list_sessions')
        assert any(s['id'] == sid for s in listed['result']['sessions'])

        got = call('get_session', session_id=sid)
        assert got['result']['id'] == sid

        started = call('start_session', session_id=sid)
        assert started['result']['status'] == 'running'

        paused = call('pause_session', session_id=sid)
        assert paused['result']['status'] == 'paused'

        resumed = call('resume_session', session_id=sid)
        assert resumed['result']['status'] == 'running'

        stopped = call('stop_session', session_id=sid)
        assert stopped['result']['status'] == 'stopped'

        deleted = call('delete_session', session_id=sid)
        assert deleted['result']['deleted'] is True
        assert session_manager.get(sid) is None


def test_create_session_requires_workflow_path():
    result = _call('create_session')
    assert result == {'error': 'workflow_path required'}


def test_session_tools_report_not_found_for_missing_session():
    for tool in ('get_session', 'start_session', 'stop_session',
                 'pause_session', 'resume_session'):
        result = _call(tool, session_id='does-not-exist')
        assert result == {'error': 'session not found'}


def test_delete_session_not_found_returns_false():
    result = _call('delete_session', session_id='does-not-exist')
    assert result == {'result': {'deleted': False, 'session_id': 'does-not-exist'}}


# ---------- Real MCP transports (Streamable HTTP + SSE), end-to-end ----------
#
# These replace the old hand-rolled /messages (JSON-RPC) and /sse tests:
# there is no /messages or bespoke /sse handler left to test in isolation
# now that mcp/server.py is built on the real `mcp` SDK, so instead these
# drive the actual SDK client all the way through `initialize` ->
# tools/list -> tools/call against a live server, the same round trip a
# real client (LM Studio, Claude Desktop) makes.

def test_unwrap_raises_tool_error_not_plain_exception_on_tool_failure():
    # Fast, no-network unit check of the exact fix described in the
    # module docstring at the top of this file: _unwrap() must raise
    # ToolError (whose message the SDK's Tool.run() preserves to the
    # client) rather than a bare exception (whose message the SDK
    # deliberately discards, replacing it with a generic "Error executing
    # tool <name>" crash message). The live round-trip tests above confirm
    # this end-to-end through a real client; this pins the unit behind it.
    from pystreamflow.mcp.server import _unwrap

    with pytest.raises(ToolError) as exc_info:
        _unwrap({'error': 'node not found'})
    assert str(exc_info.value) == 'node not found'

    assert _unwrap({'result': {'ok': True}}) == {'ok': True}


async def test_streamable_http_transport_full_round_trip(live_mcp_server):
    # mcp/server.py mounts the Streamable HTTP transport at its own root
    # ("/"), matching how it's run standalone (`python -m
    # pystreamflow.mcp.server`); api/server.py then mounts this whole app
    # under /mcp for the real deployed daemon (covered separately in
    # test_api_server_coverage.py / this fix's own live verification).
    async with streamable_http_client(f'{live_mcp_server}/') as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = {t.name for t in tools.tools}
            assert 'get_version' in names
            assert 'connect_nodes' in names
            assert len(tools.tools) == 30

            result = await session.call_tool('get_version', {})
            assert result.is_error is False
            assert 'pystreamflow' in result.content[0].text


async def test_streamable_http_transport_preserves_real_tool_error_message(live_mcp_server):
    # Bug fix folded into this same rewrite: a plain exception raised
    # inside a @mcp_server.tool()-decorated function has its message
    # stripped by the SDK (Tool.run() only preserves ToolError/
    # ResourceError messages, deliberately masking anything else as a
    # generic "Error executing tool <name>" crash) - _unwrap() must raise
    # ToolError, not a bare exception, or the real "node not found"
    # message a real client needs to show a user never arrives.
    async with streamable_http_client(f'{live_mcp_server}/') as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool('get_node_config', {'node_id': 'nope'})
            assert result.is_error is True
            assert 'node not found' in result.content[0].text


async def test_sse_transport_full_round_trip(live_mcp_server):
    # The legacy SSE transport (GET to open the stream + the real
    # `event: endpoint` handshake telling the client where to POST) is
    # what LM Studio's "SSE error: Non-200 status code (404)" report was
    # actually about - the old server had no working handshake at all.
    async with sse_client(f'{live_mcp_server}/sse') as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            assert len(tools.tools) == 30
            result = await session.call_tool('get_version', {})
            assert result.is_error is False
            assert 'pystreamflow' in result.content[0].text


def test_health_stays_public_even_with_mcp_api_key_set(live_mcp_server_with_key):
    # Regression test for a real bug found while building this fix:
    # _MCPAuthMiddleware originally compared the RAW scope["path"]
    # (e.g. "/health") against its public-paths set, but that's only
    # correct for a request straight to this module's own `app` at
    # its own root - under a Mount (api/server.py mounts this whole app
    # at /mcp for real deployments) scope["path"] is always the FULL
    # original path ("/mcp/health"), which never matches, so /health
    # would incorrectly start requiring the key too. Fixed with
    # starlette.routing.get_route_path(scope), which is mount-relative.
    # This test hits mcp/server.py's own `app` directly (not mounted
    # under anything), so it can't reproduce the mount-relative bug
    # itself - that's covered in test_api_server_coverage.py - but it
    # does confirm /health's public exemption still holds once a key is
    # configured, which is the behavior the fix had to preserve.
    r = httpx.get(f'{live_mcp_server_with_key}/health', timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {'status': 'ok', 'service': 'mcp'}


def test_streamable_http_requires_key_when_configured(live_mcp_server_with_key):
    r = httpx.post(
        f'{live_mcp_server_with_key}/',
        json={'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}},
        headers={'Accept': 'application/json, text/event-stream'},
        timeout=5.0,
    )
    assert r.status_code == 401

    r2 = httpx.post(
        f'{live_mcp_server_with_key}/',
        json={'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}},
        headers={
            'Accept': 'application/json, text/event-stream',
            'Authorization': 'Bearer sse-fixture-secret',
        },
        timeout=5.0,
    )
    assert r2.status_code != 401


# ---------- Auth gating ----------

def test_require_auth_noop_when_no_api_key_configured():
    # MCP_API_KEY is read once at import time from PSF_MCP_API_KEY; in the
    # test environment it's unset, so require_auth must be a no-op
    # regardless of headers (this is what every _call() above relies on).
    assert MCP_API_KEY == ''
    require_auth(authorization=None, x_api_key=None)  # must not raise


def test_require_auth_enforces_key_when_configured(monkeypatch):
    from fastapi import HTTPException

    import pystreamflow.mcp.server as mcp_server_module
    monkeypatch.setattr(mcp_server_module, 'MCP_API_KEY', 'secret123')

    with pytest.raises(HTTPException) as exc_info:
        mcp_server_module.require_auth(authorization=None, x_api_key=None)
    assert exc_info.value.status_code == 401

    with pytest.raises(HTTPException) as exc_info2:
        mcp_server_module.require_auth(authorization='Bearer wrong', x_api_key=None)
    assert exc_info2.value.status_code == 403

    # Correct bearer token and correct X-API-Key header both succeed.
    mcp_server_module.require_auth(authorization='Bearer secret123', x_api_key=None)
    mcp_server_module.require_auth(authorization=None, x_api_key='secret123')


def test_call_endpoint_rejects_bad_api_key(monkeypatch):
    import pystreamflow.mcp.server as mcp_server_module
    monkeypatch.setattr(mcp_server_module, 'MCP_API_KEY', 'secret123')
    r = client.post('/call', json={'tool': 'get_version', 'arguments': {}})
    assert r.status_code == 401

    r2 = client.post('/call', json={'tool': 'get_version', 'arguments': {}},
                      headers={'Authorization': 'Bearer secret123'})
    assert r2.status_code == 200


def test_main_module_runs_uvicorn(monkeypatch):
    # Exercises the `if __name__ == "__main__":` block for coverage
    # without actually starting a server.
    import runpy
    from unittest import mock

    with mock.patch('uvicorn.run') as run_mock:
        runpy.run_module('pystreamflow.mcp.server', run_name='__main__')
        run_mock.assert_called_once()


async def _tool(tool, **arguments):
    # Calls the /call route coroutine directly rather than through
    # TestClient - found necessary while auditing this exact tool for
    # unimplemented functionality (task: MCP connect_nodes/disconnect_nodes
    # missing edge-type support). TestClient spins up a brand-new
    # throwaway event loop per request and tears it down - background
    # tasks included - the moment that one call returns (see
    # test_api_server_coverage.py's identical note on its own attribute-
    # pump tests), so any test that needs a pump task to still be alive
    # *after* the request that created it returns cannot go through
    # client.post(...)/the sync `_call()` helper above.
    return await call_tool(ToolCall(tool=tool, arguments=arguments))


async def test_update_node_config_routes_through_set_attribute():
    # Bug fix: this used to be a raw `node.config.update(config)` that
    # never touched a same-named cached self.<attr> at all - FileInputNode
    # caches config['path'] as self.path in its own init(), and its
    # process() loop reads self.path (not self.config['path']) on every
    # iteration (see input_file.py's own comment on why), so the old
    # behavior updated the dict with zero visible effect on an
    # already-running node.
    await _tool('create_node', node_id='mcp-cfg-a', node_type='FileInputNode',
                config={'path': '/dev/null'})
    try:
        node = _nodes['mcp-cfg-a']
        # create_node fires node.start() as a background task (so the tool
        # call itself returns promptly) - give init() a moment to actually
        # run and cache config['path'] onto self.path before asserting
        # anything about that cached copy.
        deadline = asyncio.get_event_loop().time() + 2.0
        while not hasattr(node, 'path') and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert node.path == '/dev/null'
        result = await _tool('update_node_config', node_id='mcp-cfg-a', config={'path': '/tmp'})
        assert result['result']['status'] == 'updated'
        assert node.config['path'] == '/tmp'
        assert node.path == '/tmp'
    finally:
        _nodes.pop('mcp-cfg-a', None)


async def test_update_node_config_not_found():
    result = await _tool('update_node_config', node_id='nope', config={'x': 1})
    assert result == {'error': 'node not found'}


async def test_connect_nodes_rejects_invalid_port():
    # Bug fix: connect_nodes() used to wire any two port names unconditionally,
    # with no validate_edge() call at all - unlike POST /nodes/connect, a bad
    # port name here used to succeed silently instead of being rejected.
    await _tool('create_node', node_id='mcp-val-a', node_type='ClockNode', config={})
    await _tool('create_node', node_id='mcp-val-b', node_type='LogOutputNode', config={})
    try:
        result = await _tool('connect_nodes', source_id='mcp-val-a', source_port='bogus',
                              target_id='mcp-val-b', target_port='in')
        assert 'error' in result
        assert 'bogus' in result['error']
        assert 'bogus' not in _nodes['mcp-val-a'].outputs
    finally:
        _nodes.pop('mcp-val-a', None)
        _nodes.pop('mcp-val-b', None)


async def test_connect_nodes_fans_out_to_two_targets_and_disconnect_leaves_the_other():
    # Phase 1 of the wire-kind-unification design (see
    # claude/design_unified_wire_kinds_plan.md): mirrors
    # test_api_server_coverage.py's identical HTTP-side test. connect_nodes()
    # used to do a plain `src.add_output(source_port, pipe)` that silently
    # overwrote whatever was already wired under that name - a second wire
    # from the same source_port to a different target used to steal the
    # first target's pipe with no error. Both this tool and the HTTP
    # endpoint share the same BaseNode.add_output()/remove_output() fix, so
    # both need their own regression coverage.
    # UserInputNode's process() loop is a quiet no-op (it only ever emits
    # via an HTTP submission or a manual emit) - deliberately chosen over
    # e.g. ClockNode so this test's own src.emit() calls are the only
    # things ever putting an item on 'out', with no real periodic tick
    # racing the assertions below.
    await _tool('create_node', node_id='mcp-fanout-src', node_type='UserInputNode', config={})
    await _tool('create_node', node_id='mcp-fanout-tgt1', node_type='LogOutputNode', config={})
    await _tool('create_node', node_id='mcp-fanout-tgt2', node_type='LogOutputNode', config={})
    try:
        for tgt_id in ('mcp-fanout-tgt1', 'mcp-fanout-tgt2'):
            result = await _tool('connect_nodes', source_id='mcp-fanout-src', source_port='out',
                                  target_id=tgt_id, target_port='in')
            assert result['result'] == {'status': 'connected'}

        src = _nodes['mcp-fanout-src']
        tgt1, tgt2 = _nodes['mcp-fanout-tgt1'], _nodes['mcp-fanout-tgt2']
        assert len(src.outputs['out']) == 2

        from pystreamflow.core.stream import Pipe
        probe1, probe2 = Pipe(), Pipe()
        tgt1.add_output('out', probe1)
        tgt2.add_output('out', probe2)

        item = {'probe': 'mcp-fanout'}
        src.emit('out', item)
        got1 = await asyncio.wait_for(probe1.get(), timeout=2.0)
        got2 = await asyncio.wait_for(probe2.get(), timeout=2.0)
        assert got1['log'] == item
        assert got2['log'] == item

        disc = await _tool('disconnect_nodes', source_id='mcp-fanout-src', source_port='out',
                            target_id='mcp-fanout-tgt1', target_port='in')
        assert disc['result'] == {'status': 'disconnected'}
        assert len(src.outputs['out']) == 1

        item2 = {'probe': 'mcp-fanout-2'}
        src.emit('out', item2)
        got2_again = await asyncio.wait_for(probe2.get(), timeout=2.0)
        assert got2_again['log'] == item2
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(probe1.get(), timeout=0.3)
    finally:
        _nodes.pop('mcp-fanout-src', None)
        _nodes.pop('mcp-fanout-tgt1', None)
        _nodes.pop('mcp-fanout-tgt2', None)


async def test_connect_nodes_control_edge_uses_reserved_pipe_and_registers_target():
    # Bug fix: a "control" edge used to land on tgt.inputs[target_port] -
    # an ordinary named data slot nothing control-related ever reads -
    # instead of the node's single reserved control pipe, and never
    # registered the target in src._graph_control_targets, so a trigger
    # node's control wire made through this tool could never actually fire.
    await _tool('create_node', node_id='mcp-ctrl-a', node_type='ClockNode', config={})
    await _tool('create_node', node_id='mcp-ctrl-b', node_type='LogOutputNode', config={})
    try:
        result = await _tool('connect_nodes', source_id='mcp-ctrl-a', source_port='out',
                              target_id='mcp-ctrl-b', target_port='in', type='control')
        assert result['result'] == {'status': 'connected'}
        src, tgt = _nodes['mcp-ctrl-a'], _nodes['mcp-ctrl-b']
        assert len(tgt._control_pipes) == 1
        assert 'in' not in tgt.inputs
        assert 'mcp-ctrl-b' in src._graph_control_targets

        disc = await _tool('disconnect_nodes', source_id='mcp-ctrl-a', source_port='out',
                            target_id='mcp-ctrl-b', target_port='in', type='control')
        assert disc['result'] == {'status': 'disconnected'}
        assert tgt._control_pipes == []
        assert 'mcp-ctrl-b' not in src._graph_control_targets
    finally:
        _nodes.pop('mcp-ctrl-a', None)
        _nodes.pop('mcp-ctrl-b', None)


async def test_two_control_edges_onto_same_target_both_survive_mcp():
    # Regression test for the single-incoming-control-pipe-per-target
    # limitation (see the identical test in test_api_server_coverage.py):
    # a second control edge onto an already-control-wired target used to
    # silently steal control from the first source. Mirrors the HTTP-side
    # test since this tool is meant to behave identically.
    await _tool('create_node', node_id='mcp-2ctrl-src1', node_type='TriggerOnNode', config={})
    await _tool('create_node', node_id='mcp-2ctrl-src2', node_type='TriggerOffNode', config={})
    await _tool('create_node', node_id='mcp-2ctrl-tgt', node_type='ClockNode', config={})
    try:
        r1 = await _tool('connect_nodes', source_id='mcp-2ctrl-src1', source_port='out',
                          target_id='mcp-2ctrl-tgt', target_port='in', type='control')
        assert r1['result'] == {'status': 'connected'}
        r2 = await _tool('connect_nodes', source_id='mcp-2ctrl-src2', source_port='out',
                          target_id='mcp-2ctrl-tgt', target_port='in', type='control')
        assert r2['result'] == {'status': 'connected'}
        tgt = _nodes['mcp-2ctrl-tgt']
        assert len(tgt._control_pipes) == 2

        disc = await _tool('disconnect_nodes', source_id='mcp-2ctrl-src2', source_port='out',
                            target_id='mcp-2ctrl-tgt', target_port='in', type='control')
        assert disc['result'] == {'status': 'disconnected'}
        assert len(tgt._control_pipes) == 1
    finally:
        _nodes.pop('mcp-2ctrl-src1', None)
        _nodes.pop('mcp-2ctrl-src2', None)
        _nodes.pop('mcp-2ctrl-tgt', None)


async def test_two_data_edges_into_same_input_both_survive_mcp():
    # Regression test for the third side of the single-slot bug already
    # fixed for outputs (fan-out) and control wires (fan-in) - a regular
    # data input port used to be a single overwritable slot too. Mirrors
    # the HTTP-side test in test_api_server_coverage.py since this tool is
    # meant to behave identically.
    from pystreamflow.core.stream import Pipe

    await _tool('create_node', node_id='mcp-datafanin-src1', node_type='UserInputNode', config={})
    await _tool('create_node', node_id='mcp-datafanin-src2', node_type='UserInputNode', config={})
    await _tool('create_node', node_id='mcp-datafanin-tgt', node_type='LogOutputNode', config={})
    try:
        for src_id in ('mcp-datafanin-src1', 'mcp-datafanin-src2'):
            result = await _tool('connect_nodes', source_id=src_id, source_port='out',
                                  target_id='mcp-datafanin-tgt', target_port='in')
            assert result['result'] == {'status': 'connected'}

        src1, src2 = _nodes['mcp-datafanin-src1'], _nodes['mcp-datafanin-src2']
        tgt = _nodes['mcp-datafanin-tgt']
        assert len(tgt._input_sources['in']) == 2

        probe = Pipe()
        tgt.add_output('out', probe)
        src1.emit('out', 'from-src1')
        src2.emit('out', 'from-src2')
        got1 = await asyncio.wait_for(probe.get(), timeout=2.0)
        got2 = await asyncio.wait_for(probe.get(), timeout=2.0)
        assert {got1['log'], got2['log']} == {'from-src1', 'from-src2'}

        disc = await _tool('disconnect_nodes', source_id='mcp-datafanin-src1', source_port='out',
                            target_id='mcp-datafanin-tgt', target_port='in')
        assert disc['result'] == {'status': 'disconnected'}
        assert len(tgt._input_sources['in']) == 1

        src2.emit('out', 'from-src2-again')
        got_again = await asyncio.wait_for(probe.get(), timeout=2.0)
        assert got_again['log'] == 'from-src2-again'

        src1.emit('out', 'should-not-arrive')
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(probe.get(), timeout=0.3)
    finally:
        _nodes.pop('mcp-datafanin-src1', None)
        _nodes.pop('mcp-datafanin-src2', None)
        _nodes.pop('mcp-datafanin-tgt', None)


async def test_connect_nodes_attribute_edge_pumps_into_target_and_disconnect_cancels_it():
    # Bug fix: an "attribute" edge used to get no live pump task at all -
    # nothing in a node's own process() loop reads self.inputs under an
    # arbitrary attribute-port name - so wiring a value source into another
    # node's config attribute through this tool had zero effect, forever.
    from pystreamflow.core.engine import _attribute_pump_tasks

    await _tool('create_node', node_id='mcp-attr-src', node_type='ConstantValueNode',
                config={'value': 'mcp-picked-path', 'repeat': True, 'interval': 0.02})
    await _tool('create_node', node_id='mcp-attr-tgt', node_type='FileInputNode',
                config={'path': '/dev/null'})
    try:
        result = await _tool('connect_nodes', source_id='mcp-attr-src', source_port='out',
                              target_id='mcp-attr-tgt', target_port='path', type='attribute')
        assert result['result'] == {'status': 'connected'}

        tgt = _nodes['mcp-attr-tgt']
        deadline = asyncio.get_event_loop().time() + 2.0
        while tgt.config.get('path') != 'mcp-picked-path' and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.02)
        assert tgt.config.get('path') == 'mcp-picked-path'
        # The attribute pipe must not have landed as an ordinary named
        # input either - nothing should ever read self.inputs['path'].
        assert 'path' not in tgt.inputs

        key = ('mcp-attr-src', 'mcp-attr-tgt', 'path')
        assert key in _attribute_pump_tasks
        task = _attribute_pump_tasks[key]
        assert not task.done()

        disc = await _tool('disconnect_nodes', source_id='mcp-attr-src', source_port='out',
                            target_id='mcp-attr-tgt', target_port='path', type='attribute')
        assert disc['result'] == {'status': 'disconnected'}
        assert key not in _attribute_pump_tasks
        await asyncio.sleep(0.02)
        assert task.cancelled() or task.done()
    finally:
        _nodes.pop('mcp-attr-src', None)
        _nodes.pop('mcp-attr-tgt', None)


async def test_delete_node_cancels_its_attribute_pump_tasks():
    # Bug fix: delete_node() used to pop the node and stop it with no
    # attempt to cancel any attribute-edge pump task still feeding it -
    # the normal case (deleting a node deletes its edges too, not
    # edge-by-edge first) used to leak that task forever.
    from pystreamflow.core.engine import _attribute_pump_tasks

    await _tool('create_node', node_id='mcp-del-src', node_type='ConstantValueNode',
                config={'value': 'x', 'repeat': True, 'interval': 0.02})
    await _tool('create_node', node_id='mcp-del-tgt', node_type='FileInputNode',
                config={'path': '/dev/null'})
    try:
        await _tool('connect_nodes', source_id='mcp-del-src', source_port='out',
                     target_id='mcp-del-tgt', target_port='path', type='attribute')
        key = ('mcp-del-src', 'mcp-del-tgt', 'path')
        assert key in _attribute_pump_tasks
        task = _attribute_pump_tasks[key]

        result = await _tool('delete_node', node_id='mcp-del-tgt')
        assert result['result']['status'] == 'deleted'
        assert key not in _attribute_pump_tasks
        await asyncio.sleep(0.02)
        assert task.cancelled() or task.done()
    finally:
        _nodes.pop('mcp-del-src', None)
        _nodes.pop('mcp-del-tgt', None)


async def test_mcp_and_http_api_share_the_same_attribute_pump_tracking():
    # The two surfaces run in the same process against the same shared
    # `_nodes` registry (api/server.py even mounts this module's whole
    # FastAPI app) - an edge wired through one must be visible to (and
    # cleaned up by) the other, not tracked in two independent dicts that
    # can each leak the other's tasks. Confirms it's the literal same
    # object, not just two dicts that happen to agree right now.
    import pystreamflow.api.server as api_server_module
    from pystreamflow.core.engine import _attribute_pump_tasks as engine_tasks

    assert api_server_module._attribute_pump_tasks is engine_tasks
