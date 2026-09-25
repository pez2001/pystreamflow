"""
Additional coverage for pystreamflow/core/web_server.py - previously 45%
covered. test_web_server_coverage.py already covers register_node();
this rounds out /health, /nodes, /nodes/{id}/last, register_input()'s
dynamically-registered POST route, and start_server()/stop_server()
(including the regression fix making stop_server() actually cancel the
background uvicorn task instead of only flipping a flag - see the code
comment in web_server.py).
"""
import asyncio

from fastapi.testclient import TestClient

from pystreamflow.core import web_server
from pystreamflow.core.web_server import _nodes, app, register_input, register_route


class _Dummy:
    def __init__(self, node_id):
        self.id = node_id
        self.emitted = []
    def get_last(self, n):
        return [{'port': 'out', 'item': 'x'}][:n]
    def emit(self, port, item):
        self.emitted.append((port, item))


def test_health_endpoint():
    client = TestClient(app)
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json() == {'status': 'ok'}


def test_list_nodes_endpoint():
    d = _Dummy('list-node-1')
    _nodes[d.id] = d
    try:
        client = TestClient(app)
        r = client.get('/nodes')
        assert r.status_code == 200
        assert r.json()[d.id] == '_Dummy'
    finally:
        _nodes.pop(d.id, None)


def test_node_last_endpoint_found_and_not_found():
    d = _Dummy('last-node-1')
    _nodes[d.id] = d
    try:
        client = TestClient(app)
        r = client.get(f'/nodes/{d.id}/last?n=1')
        assert r.json() == {'node': d.id, 'last': [{'port': 'out', 'item': 'x'}]}

        r2 = client.get('/nodes/does-not-exist/last')
        assert r2.json() == {'error': 'node not found'}
    finally:
        _nodes.pop(d.id, None)


def test_register_input_prefixes_path_and_creates_working_route():
    d = _Dummy('input-node-1')
    _nodes[d.id] = d
    try:
        path = register_input(d.id, 'no-leading-slash', d)
        assert path == '/no-leading-slash'
        client = TestClient(app)
        r = client.post(path, json={'value': 42})
        assert r.status_code == 200
        assert r.json() == {'status': 'ok', 'node': d.id}
        assert d.emitted == [('out', {'value': 42})]
    finally:
        _nodes.pop(d.id, None)


def test_register_route_replaces_existing_route_under_the_same_name():
    """Regression test for a real bug found live: "workflow is running,
    web input node is started, message was not received and nothing
    shown in the UI". Starlette's router never deduplicates routes by
    name or path - app.get()/app.post() just appends a new Route object
    every call, and the router matches in registration order (first
    match wins). Every node type that re-registers its HTTP route under
    a stable, node-id-derived name every time it's (re)started - which
    happens on every session restart, since a new node instance is
    created under the same workflow-authored id - used to leave the OLD
    route live and permanently matching first, silently swallowing every
    future request into a dead node from a previous run while returning
    a completely normal 200 the whole time. register_route() fixes this
    by removing any existing route registered under the same `name`
    before adding the new one.
    """
    name = 'route-replace-test'
    path = '/route-replace-test'
    calls = []

    async def first_handler():
        calls.append('first')
        return {'which': 'first'}

    async def second_handler():
        calls.append('second')
        return {'which': 'second'}

    register_route('get', path, name, first_handler)
    matching = [r for r in app.router.routes if getattr(r, 'name', None) == name]
    assert len(matching) == 1

    client = TestClient(app)
    r1 = client.get(path)
    assert r1.json() == {'which': 'first'}

    # Re-registering under the same name (simulating a session restart)
    # must replace, not stack, the route.
    register_route('get', path, name, second_handler)
    matching = [r for r in app.router.routes if getattr(r, 'name', None) == name]
    assert len(matching) == 1, "stale route under the same name was not removed"

    r2 = client.get(path)
    assert r2.json() == {'which': 'second'}, (
        "request reached the OLD handler - this is exactly the silent "
        "message-vanishes-into-a-dead-node bug"
    )
    assert calls == ['first', 'second']


def test_register_input_reregistration_reaches_the_new_node_not_the_old_one():
    """Same bug as above, exercised through register_input() the way
    WebInputNode/ApiInputNode actually call it: create a session's node,
    "restart" (a new node instance reusing the same node_id/path - what
    happens on every real session stop+start), and confirm a request
    after the restart reaches the NEW instance, not the one from before.
    """
    node_id = 'restart-node-1'
    old = _Dummy(node_id)
    new = _Dummy(node_id)
    try:
        path = register_input(node_id, 'restart-node-1', old)
        client = TestClient(app)
        client.post(path, json={'value': 'before-restart'})
        assert old.emitted == [('out', {'value': 'before-restart'})]

        # Simulate the session restarting: a brand new node instance is
        # created under the same id and re-registers the same route.
        register_input(node_id, 'restart-node-1', new)
        client.post(path, json={'value': 'after-restart'})

        assert new.emitted == [('out', {'value': 'after-restart'})], (
            "the post-restart request never reached the new node instance"
        )
        assert old.emitted == [('out', {'value': 'before-restart'})], (
            "the old, superseded node instance kept receiving traffic "
            "after the restart"
        )
    finally:
        _nodes.pop(node_id, None)


async def test_start_and_stop_server_lifecycle():
    assert web_server._running is False
    try:
        await web_server.start_server(host='127.0.0.1', port=0)
        assert web_server._running is True
        assert web_server._server_task is not None

        # Calling start_server() again while already running is a no-op
        # (the `if _running: return` guard) - no second task is created.
        first_task = web_server._server_task
        await web_server.start_server(host='127.0.0.1', port=0)
        assert web_server._server_task is first_task

        await asyncio.sleep(0.05)
    finally:
        await web_server.stop_server()
        assert web_server._running is False
        assert web_server._server_task is None
