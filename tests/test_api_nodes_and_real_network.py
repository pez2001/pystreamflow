"""
Coverage for the new ApiInputNode/ApiOutputNode types, plus a genuine
real-network regression test proving WebInputNode (and, by the same
mechanism, ApiInputNode) actually opens a connectable TCP socket.

Context: every existing test touching WebInputNode/WebOutputNode either
checked internal flags only, drove routes through FastAPI TestClient's
in-process ASGI transport (no real socket at all), or called
node.emit()/in_pipe.put() directly, bypassing HTTP entirely. Nothing in
the suite had ever confirmed that a real HTTP client, on a real socket,
talking to a real bound port, actually reaches one of these nodes. The
tests in TestRealNetworkVerification below close that gap using raw
`socket` plus `httpx` (a real dependency of this project) rather than
TestClient, with `port=0` (OS-assigned ephemeral port) discovered
afterward via `core.web_server.get_bound_port()` - a small addition made
alongside these tests specifically to make that discovery possible
without flakiness or a hardcoded port.

Cleanup note: unlike WebOutputNode/WebOutputJSONNode (which never call
start_server() themselves), ApiInputNode and ApiOutputNode both do -
matching WebInputNode's pattern, and closing a real gap where an
API-output-only workflow would otherwise register a route nothing is
actually serving. That means every test below that starts one of these
two node types uses `port: 0` (never the shared default of 8080 - two
tests both binding that would collide) and calls
`await web_server.stop_server()` in its `finally`, exactly like the
existing `test_phase6_hardening.py` WebInputNode test already does -
`core/web_server.py`'s module-global server state is shared across every
test in the process, so whichever test starts it is responsible for
stopping it again.
"""
import asyncio
import socket
import uuid

import httpx
from fastapi.testclient import TestClient

from pystreamflow.core import web_server
from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes, app
from pystreamflow.nodes.input_api import ApiInputNode
from pystreamflow.nodes.input_web import WebInputNode
from pystreamflow.nodes.output_api import ApiOutputNode
from pystreamflow.nodes.web_output import WebOutputNode
from pystreamflow.nodes.web_output_json import WebOutputJSONNode

client = TestClient(app)


def _unique_uri(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:8]}'


def _route_endpoint(path, method='GET'):
    for route in app.routes:
        if getattr(route, 'path', None) == path and method in getattr(route, 'methods', set()):
            return route.endpoint
    raise AssertionError(f'no route registered for {method} {path}')


# ---------- ApiInputNode: in-process route coverage ----------

async def test_api_input_node_maps_uri_to_slash_api_prefix():
    uri = _unique_uri('orders')
    n = ApiInputNode('api-in-1', {'uri': uri, 'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        assert n.path == f'/api/{uri}'
        r = client.post(f'/api/{uri}', json={'order_id': 42})
        assert r.status_code == 200
        assert r.json() == {'status': 'ok', 'node': 'api-in-1'}
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'order_id': 42}
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-in-1', None)


async def test_api_input_node_uri_defaults_to_node_id():
    n = ApiInputNode('api-in-default', {'port': 0})
    await n.start()
    try:
        assert n.path == '/api/api-in-default'
        r = client.post('/api/api-in-default', json={'x': 1})
        assert r.status_code == 200
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-in-default', None)


async def test_api_input_node_strips_leading_and_trailing_slashes_from_uri():
    # init() alone (no start()) already registers the route and computes
    # self.path - it doesn't call start_server(), so no shared-server
    # cleanup is needed here.
    n = ApiInputNode('api-in-slashes', {'uri': '/orders/'})
    await n.init()
    try:
        assert n.path == '/api/orders'
    finally:
        await n.stop()
        _nodes.pop('api-in-slashes', None)


async def test_api_input_node_uri_config_update_reregisters_route_live():
    """Regression test for a real bug found live, from a direct report:
    editing a live ApiInputNode's `uri` field (PUT /nodes/{id}/config,
    exercised here via set_attribute()+on_config_updated() the same way
    that endpoint now calls them) used to update self.config with zero
    effect on the actual HTTP route - self.path/self.uri were only ever
    computed once in init(), so the *old* uri kept working (if anything
    was even listening there) and the *new* one 404'd until the node was
    deleted and recreated or the whole workflow was re-run. Confirms both
    halves: the new uri starts working, and the old one stops.
    """
    old_uri = _unique_uri('cfg-old')
    new_uri = _unique_uri('cfg-new')
    n = ApiInputNode('api-in-cfgupdate', {'uri': old_uri, 'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        r_old = client.post(f'/api/{old_uri}', json={'v': 'via-old-uri'})
        assert r_old.status_code == 200
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'v': 'via-old-uri'}

        # Simulate the editor's config panel: PUT /nodes/{id}/config now
        # routes each field through set_attribute() then calls
        # on_config_updated() - exercise both directly here.
        n.set_attribute('uri', new_uri)
        n.on_config_updated({'uri'})
        assert n.path == f'/api/{new_uri}'

        r_new = client.post(f'/api/{new_uri}', json={'v': 'via-new-uri'})
        assert r_new.status_code == 200
        item2 = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item2 == {'v': 'via-new-uri'}

        r_old_again = client.post(f'/api/{old_uri}', json={'v': 'should-not-arrive'})
        assert r_old_again.status_code == 404, (
            "the stale route under the old uri is still live after the "
            "config update - it should have been replaced, not left "
            "stacked alongside the new one"
        )
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-in-cfgupdate', None)


async def test_api_input_node_rejects_input_while_stopped():
    """Regression test for a real bug found live, from a direct report:
    stopping an ApiInputNode/WebInputNode via POST /nodes/{id}/stop used
    to have zero effect on its HTTP route at all - the route is
    registered once in init() and lives independently of whatever task
    stop() cancels, so a "stopped" node kept silently accepting and
    forwarding every request exactly as if it were still running. The
    node's title going grey / health reporting "stopped" was actively
    misleading about what the HTTP endpoint would actually do.
    """
    uri = _unique_uri('stopgate')
    n = ApiInputNode('api-in-stopgate', {'uri': uri, 'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        r_running = client.post(f'/api/{uri}', json={'v': 'while-running'})
        assert r_running.status_code == 200
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'v': 'while-running'}

        await n.stop()
        r_stopped = client.post(f'/api/{uri}', json={'v': 'while-stopped'})
        assert r_stopped.status_code == 503
        assert r_stopped.json()['status'] == 'stopped'
        # Nothing new should have reached the output pipe.
        assert real_out.queue.qsize() == 0
    finally:
        await web_server.stop_server()
        _nodes.pop('api-in-stopgate', None)


async def test_api_input_node_is_a_post_only_endpoint():
    uri = _unique_uri('post-only')
    n = ApiInputNode('api-in-getcheck', {'uri': uri, 'port': 0})
    await n.start()
    try:
        r = client.get(f'/api/{uri}')
        assert r.status_code == 405  # Method Not Allowed - registered as POST only
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-in-getcheck', None)


# ---------- ApiOutputNode: in-process route coverage ----------

async def test_api_output_node_json_and_raw_routes_non_sse():
    uri = _unique_uri('out')
    n = ApiOutputNode('api-out-1', {'uri': uri, 'sse': False, 'port': 0})
    await n.start()
    try:
        await n.in_pipe.put({'result': 'ok', 'id': 7})
        await asyncio.sleep(0.2)
        r_json = client.get(f'/api/{uri}')
        r_raw = client.get(f'/api/{uri}/raw')
        assert r_json.status_code == 200
        assert r_json.json() == {'latest': {'result': 'ok', 'id': 7}}
        assert r_raw.status_code == 200
        assert r_raw.text == str({'result': 'ok', 'id': 7})
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-out-1', None)


async def test_api_output_node_forwards_to_emit_out_port_too():
    uri = _unique_uri('passthrough')
    n = ApiOutputNode('api-out-passthrough', {'uri': uri, 'sse': False, 'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await n.in_pipe.put({'v': 9})
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'v': 9}
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-out-passthrough', None)


async def test_api_output_node_has_graph_level_raw_output_port():
    # The actual fix for "ui is not showing raw outputs edge points": an
    # earlier pass only added the /api/<uri>/raw HTTP route (covered by
    # test_api_output_node_sse_raw_route_is_not_json_encapsulated below)
    # but never a real, wireable graph output port - so the editor
    # correctly showed nothing new, since there was nothing to show.
    # ApiOutputNode must also have a 'raw' output *port*, distinct from
    # 'out', carrying the same unencapsulated item.
    uri = _unique_uri('rawport')
    n = ApiOutputNode('api-out-rawport', {'uri': uri, 'sse': False, 'port': 0})
    real_out = Pipe()
    real_raw = Pipe()
    n.add_output('out', real_out)
    n.add_output('raw', real_raw)
    await n.start()
    try:
        await n.in_pipe.put({'v': 5})
        out_item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        raw_item = await asyncio.wait_for(real_raw.get(), timeout=2.0)
        assert out_item == {'v': 5}
        assert raw_item == {'v': 5}
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-out-rawport', None)


def test_api_output_node_port_schema_declares_raw_output():
    from pystreamflow.core.port_schema import get_port_schema
    schema = get_port_schema('ApiOutputNode')
    assert schema['outputs'] == ['out', 'raw']


async def test_api_output_node_sse_raw_route_is_not_json_encapsulated():
    # The core of the "raw output edge point" ask: the /raw route must
    # emit the same data as the JSON route, but without JSON-encoding it.
    uri = _unique_uri('sse')
    n = ApiOutputNode('api-out-sse', {'uri': uri, 'sse': True, 'port': 0})
    await n.start()
    try:
        await n.in_pipe.put({'v': 1})
        await asyncio.sleep(0.15)
        raw_chunk = await (await _route_endpoint(f'/api/{uri}/raw')()).body_iterator.__anext__()
        json_chunk = await (await _route_endpoint(f'/api/{uri}')()).body_iterator.__anext__()
        # Raw: no JSON braces/quotes escaping the dict - just Python's str().
        assert "'v': 1" in raw_chunk
        # JSON route: real JSON encoding (double-quoted keys).
        assert '"v"' in json_chunk
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-out-sse', None)


async def test_api_output_node_starts_its_own_server():
    # Deliberate improvement over WebOutputNode/WebOutputJSONNode (which
    # never call start_server() themselves and rely on some other node in
    # the same workflow having done so): an API-output-only workflow must
    # still actually serve its route over a real socket.
    n = ApiOutputNode('api-out-selfstart', {'uri': _unique_uri('selfstart'), 'port': 0})
    await n.start()
    try:
        port = await web_server.get_bound_port(timeout=5.0)
        assert port is not None and port != 0
    finally:
        await n.stop()
        await web_server.stop_server()
        _nodes.pop('api-out-selfstart', None)


# ---------- Real-network verification (the actual "does it open a port?" ask) ----------

class TestRealNetworkVerification:
    """Genuine socket-level checks: a raw `socket.create_connection` and a
    real `httpx` HTTP request over an actual loopback TCP connection, not
    FastAPI's in-process TestClient transport and not a direct
    node.emit()/pipe.put() call. This is what the rest of the suite never
    verified for WebInputNode before this file was added.
    """

    async def test_web_input_node_opens_a_real_connectable_port(self):
        n = WebInputNode('real-net-webin', {'path': '/real_net_webin', 'port': 0})
        real_out = Pipe()
        n.add_output('out', real_out)
        await n.start()
        try:
            port = await web_server.get_bound_port(timeout=5.0)
            assert port is not None and port != 0

            # Raw TCP connect - proves a real listening socket exists,
            # independent of any HTTP/ASGI machinery.
            with socket.create_connection(('127.0.0.1', port), timeout=2.0):
                pass

            # Real HTTP POST over the network (httpx's real transport,
            # not ASGITransport) reaching the node's real output pipe.
            async with httpx.AsyncClient() as http_client:
                resp = await http_client.post(
                    f'http://127.0.0.1:{port}/real_net_webin',
                    json={'hello': 'world'},
                    timeout=5.0,
                )
            assert resp.status_code == 200
            assert resp.json() == {'status': 'ok', 'node': 'real-net-webin'}

            item = await asyncio.wait_for(real_out.get(), timeout=3.0)
            assert item == {'hello': 'world'}
        finally:
            await n.stop()
            await web_server.stop_server()

    async def test_api_input_node_opens_a_real_connectable_port(self):
        uri = _unique_uri('real-net')
        n = ApiInputNode('real-net-apiin', {'uri': uri, 'port': 0})
        real_out = Pipe()
        n.add_output('out', real_out)
        await n.start()
        try:
            port = await web_server.get_bound_port(timeout=5.0)
            assert port is not None and port != 0

            with socket.create_connection(('127.0.0.1', port), timeout=2.0):
                pass

            async with httpx.AsyncClient() as http_client:
                resp = await http_client.post(
                    f'http://127.0.0.1:{port}/api/{uri}',
                    json={'order_id': 99},
                    timeout=5.0,
                )
            assert resp.status_code == 200
            item = await asyncio.wait_for(real_out.get(), timeout=3.0)
            assert item == {'order_id': 99}
        finally:
            await n.stop()
            await web_server.stop_server()

    async def test_get_bound_port_returns_none_when_no_server_started(self):
        # Force the "never started" state regardless of what earlier
        # tests in this module left behind, then confirm get_bound_port()
        # times out and returns None rather than hanging or crashing.
        await web_server.stop_server()
        assert web_server._server is None
        result = await web_server.get_bound_port(timeout=0.2)
        assert result is None


class TestLateConnectRewiring:
    """Regression coverage for a real bug found while verifying the new
    "a node added to a graph should default to started/active" fix
    (api/server.py's create_node and mcp/server.py's create_node tool now
    call node.start() immediately instead of leaving a freshly created
    node inert until a separate manual start).

    That fix makes an ad-hoc "POST /nodes (create + auto-start), then
    POST /nodes/connect (wire it up)" sequence the normal order of
    operations for a node added to a live graph one at a time - and
    WebOutputNode/WebOutputJSONNode/ApiOutputNode's process() loops used
    to read from `self.in_pipe`, a reference captured exactly once in
    init() and never updated. A wire arriving via /nodes/connect *after*
    start() (as now always happens for an auto-started node) updates
    `self.inputs['in']` but never that already-captured attribute, so the
    real upstream data was silently never read - only a wire present
    before start() (the Engine's own full-workflow-run path, which always
    wires before starting nodes) ever worked. Fixed by re-resolving
    `self.inputs.get('in', self.in_pipe)` on every loop iteration instead
    of once outside it, so a late wire is picked up within one ~1s poll
    interval instead of never. LMStudioNode had the identical bug (see
    test_json_modify_and_llm_coverage.py for its own regression test) and
    was fixed the same way.
    """

    async def test_web_output_node_picks_up_a_wire_added_after_start(self):
        n = WebOutputNode('late-wire-wo', {'path': _unique_uri('/late-wo'), 'sse': False})
        await n.start()
        try:
            await asyncio.sleep(0.05)
            late_pipe = Pipe()
            n.add_input('in', late_pipe)  # simulates POST /nodes/connect after auto-start
            await late_pipe.put({'v': 'late'})
            await asyncio.wait_for(_poll_for_item(n), timeout=3.0)
        finally:
            await n.stop()
            _nodes.pop('late-wire-wo', None)

    async def test_web_output_json_node_picks_up_a_wire_added_after_start(self):
        n = WebOutputJSONNode('late-wire-woj', {'path': _unique_uri('/late-woj'), 'sse': False})
        await n.start()
        try:
            await asyncio.sleep(0.05)
            late_pipe = Pipe()
            n.add_input('in', late_pipe)
            await late_pipe.put({'v': 'late'})
            await asyncio.wait_for(_poll_for_item(n), timeout=3.0)
        finally:
            await n.stop()
            _nodes.pop('late-wire-woj', None)

    async def test_api_output_node_picks_up_a_wire_added_after_start(self):
        n = ApiOutputNode('late-wire-ao', {'uri': _unique_uri('late-ao'), 'sse': False, 'port': 0})
        await n.start()
        try:
            await asyncio.sleep(0.05)
            late_pipe = Pipe()
            n.add_input('in', late_pipe)
            await late_pipe.put({'v': 'late'})
            await asyncio.wait_for(_poll_for_item(n), timeout=3.0)
        finally:
            await n.stop()
            await web_server.stop_server()
            _nodes.pop('late-wire-ao', None)


async def _poll_for_item(node, interval=0.05):
    while True:
        last = node.get_last(5)
        if any(entry.get('item') == {'v': 'late'} for entry in last):
            return
        await asyncio.sleep(interval)


# ---------- Bug fix: default host was '127.0.0.1', unreachable through a
# Docker-published port for this node's shared web server (Docker's
# port-forwarding connects to the container's real interface, never to a
# loopback-only bind inside it) - see ApiInputNode.init()'s own comment.
# SocketOutputNode is deliberately excluded: unlike these five, its own
# `host` default has an explicit, different security rationale (see its
# own docstring) and was never touched by this fix.

class TestDefaultHostIsAllInterfaces:
    """`config.get('host', ...)`'s *default* value only - a workflow that
    explicitly sets `host` anywhere in this suite (nearly everywhere else,
    for test isolation) is completely unaffected either way.
    """

    async def test_api_input_node_default_host(self):
        n = ApiInputNode('host-default-ai', {'uri': _unique_uri('host-default')})
        await n.init()
        assert n.host == '0.0.0.0'

    async def test_web_input_node_default_host(self):
        n = WebInputNode('host-default-wi', {'path': _unique_uri('host-default-wi')})
        await n.init()
        assert n.host == '0.0.0.0'
        await n.stop()
        _nodes.pop('host-default-wi', None)

    async def test_api_output_node_default_host(self):
        n = ApiOutputNode('host-default-ao', {'uri': _unique_uri('host-default-ao'), 'sse': False})
        await n.init()
        assert n.host == '0.0.0.0'
        _nodes.pop('host-default-ao', None)

    async def test_web_output_node_default_host(self):
        n = WebOutputNode('host-default-wo', {'sse': False})
        await n.init()
        assert n.host == '0.0.0.0'
        _nodes.pop('host-default-wo', None)

    async def test_web_output_json_node_default_host(self):
        n = WebOutputJSONNode('host-default-woj', {'sse': False})
        await n.init()
        assert n.host == '0.0.0.0'
        _nodes.pop('host-default-woj', None)

    async def test_shared_server_actually_binds_all_interfaces_by_default(self):
        """The deeper, real-network version of the check above: not just
        that the node's own `self.host` attribute reads '0.0.0.0', but
        that the value genuinely reaches `start_server()` and the shared
        server's recorded bind address - this is what would have caught
        the original bug (every other test in this file connects via
        '127.0.0.1', which a loopback-only bind also happily accepts, so
        connecting successfully was never proof the bind was correct).
        """
        n = ApiInputNode('host-default-real', {'uri': _unique_uri('host-default-real'), 'port': 0})
        await n.start()
        try:
            port = await web_server.get_bound_port(timeout=5.0)
            assert port is not None and port != 0
            assert web_server._bound_host_port == ('0.0.0.0', 0)
            # Still genuinely connectable via loopback too - 0.0.0.0 is a
            # superset of 127.0.0.1, not a replacement for it.
            with socket.create_connection(('127.0.0.1', port), timeout=2.0):
                pass
        finally:
            await n.stop()
            await web_server.stop_server()


# ---------- Bug fix: reflect_node/get_node_config (MCP) and
# /nodes/{id}/config, /reflection/nodes/{id} (HTTP API) used to return
# only a node's raw config (e.g. just {"uri": "demo/in"} for an
# ApiInputNode) with no way for a caller to derive the real, effective
# HTTP route (the "/api/" prefix) or port - see
# core.web_server.http_endpoint_info()'s own docstring.

class TestHttpEndpointInfo:
    def test_none_for_a_non_network_node(self):
        from pystreamflow.nodes.value_constant import ConstantValueNode
        n = ConstantValueNode('not-network', {'value': 'x'})
        assert web_server.http_endpoint_info(n) is None

    async def test_api_input_node_surfaces_real_prefixed_path(self):
        uri = _unique_uri('endpoint-info')
        n = ApiInputNode('endpoint-info-ai', {'uri': uri, 'port': 8080})
        await n.init()
        info = web_server.http_endpoint_info(n)
        assert info == {
            'method': 'POST',
            'path': f'/api/{uri}',
            'port': 8080,
            'note': info['note'],  # content asserted loosely below
        }
        assert 'http://<host>:8080' in info['note']
        assert f'/api/{uri}' in info['note']
        _nodes.pop('endpoint-info-ai', None)

    async def test_api_output_node_surfaces_raw_path_and_sse(self):
        uri = _unique_uri('endpoint-info-ao')
        n = ApiOutputNode('endpoint-info-ao', {'uri': uri, 'sse': True, 'port': 8080})
        await n.init()
        info = web_server.http_endpoint_info(n)
        assert info['method'] == 'GET'
        assert info['path'] == f'/api/{uri}'
        assert info['raw_path'] == f'/api/{uri}/raw'
        assert info['sse'] is True
        _nodes.pop('endpoint-info-ao', None)

    async def test_reflect_node_mcp_tool_includes_http_endpoint(self):
        from pystreamflow.mcp import server as mcp_server_module
        uri = _unique_uri('endpoint-info-mcp')
        n = ApiInputNode('endpoint-info-mcp', {'uri': uri, 'port': 8080})
        await n.init()
        try:
            result = await mcp_server_module._execute_tool('reflect_node', {'node_id': 'endpoint-info-mcp'})
            endpoint = result['result']['http_endpoint']
            assert endpoint['path'] == f'/api/{uri}'
            assert endpoint['port'] == 8080
        finally:
            _nodes.pop('endpoint-info-mcp', None)

    async def test_get_node_config_mcp_tool_includes_http_endpoint(self):
        from pystreamflow.mcp import server as mcp_server_module
        uri = _unique_uri('endpoint-info-mcp2')
        n = ApiInputNode('endpoint-info-mcp2', {'uri': uri, 'port': 8080})
        await n.init()
        try:
            result = await mcp_server_module._execute_tool('get_node_config', {'node_id': 'endpoint-info-mcp2'})
            endpoint = result['result']['http_endpoint']
            assert endpoint['path'] == f'/api/{uri}'
        finally:
            _nodes.pop('endpoint-info-mcp2', None)

    async def test_http_api_config_endpoint_includes_http_endpoint(self):
        # This node's own registry entry lives in the shared `_nodes` dict
        # imported above, but the `/nodes/{id}/config` *route* lives on
        # the main `pystreamflow.api.server` app, not this file's `client`
        # (which wraps `core.web_server.app`, a separate, smaller FastAPI
        # app) - a real, separate TestClient against the real app is
        # needed here, matching test_api_server_coverage.py's own setup.
        from conftest import TEST_API_KEY
        from fastapi.testclient import TestClient as _TestClient
        from pystreamflow.api.server import app as api_app
        api_client = _TestClient(api_app, headers={"Authorization": f"Bearer {TEST_API_KEY}"})
        uri = _unique_uri('endpoint-info-http')
        n = ApiInputNode('endpoint-info-http', {'uri': uri, 'port': 8080})
        await n.init()
        try:
            resp = api_client.get('/nodes/endpoint-info-http/config')
            assert resp.status_code == 200
            endpoint = resp.json()['http_endpoint']
            assert endpoint['path'] == f'/api/{uri}'
        finally:
            _nodes.pop('endpoint-info-http', None)

    async def test_http_api_reflection_endpoint_includes_http_endpoint(self):
        from conftest import TEST_API_KEY
        from fastapi.testclient import TestClient as _TestClient
        from pystreamflow.api.server import app as api_app
        api_client = _TestClient(api_app, headers={"Authorization": f"Bearer {TEST_API_KEY}"})
        uri = _unique_uri('endpoint-info-refl')
        n = ApiInputNode('endpoint-info-refl', {'uri': uri, 'port': 8080})
        await n.init()
        try:
            resp = api_client.get('/reflection/nodes/endpoint-info-refl')
            assert resp.status_code == 200
            endpoint = resp.json()['http_endpoint']
            assert endpoint['path'] == f'/api/{uri}'
        finally:
            _nodes.pop('endpoint-info-refl', None)
