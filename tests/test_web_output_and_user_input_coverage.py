"""
Coverage for the three HTTP-driven node types that self-register FastAPI
routes onto the shared core/web_server.py `app` (WebOutputNode,
WebOutputJSONNode - previously 55%/51% covered; UserInputNode - previously
20% covered). These drive the actual registered routes through a real
TestClient rather than only constructing the node.
"""
import asyncio
import uuid

from fastapi.testclient import TestClient

from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes, app
from pystreamflow.nodes.user_input_node import UserInputNode
from pystreamflow.nodes.web_output import WebOutputNode
from pystreamflow.nodes.web_output_json import WebOutputJSONNode


def _unique_path(prefix):
    return f'/{prefix}-{uuid.uuid4().hex[:8]}'


def _route_endpoint(path, method='GET'):
    for route in app.routes:
        if getattr(route, 'path', None) == path and method in getattr(route, 'methods', set()):
            return route.endpoint
    raise AssertionError(f'no route registered for {method} {path}')


async def _first_sse_chunk(path, method='GET'):
    # Driving a genuinely never-ending SSE stream (these nodes' generators
    # loop `while True`, only ever pausing on a timeout to yield a
    # keepalive) through TestClient's real streaming transport reliably
    # hangs the whole test process in this environment - even
    # pytest-timeout's signal-based timeout couldn't interrupt it, so it's
    # not just a slow test. Instead, call the registered route's endpoint
    # function directly (the same function TestClient would have called
    # through ASGI) and drain only the first chunk of the StreamingResponse
    # it returns - this exercises the exact same generator code without
    # the hang.
    endpoint = _route_endpoint(path, method)
    response = await endpoint()
    chunk = await response.body_iterator.__anext__()
    await response.body_iterator.aclose()
    return chunk


# ---------- WebOutputNode ----------

async def test_web_output_node_plain_text_route():
    path = _unique_path('wo-plain')
    n = WebOutputNode('n', {'path': path, 'sse': False})
    await n.start()
    try:
        with TestClient(app) as c:
            empty = c.get(path)
            assert empty.status_code == 200
            assert empty.text == ''

            await n.in_pipe.put('hello world')
            await asyncio.sleep(0.1)

            got = c.get(path)
            assert got.status_code == 200
            assert got.text == 'hello world'
    finally:
        await n.stop()
        _nodes.pop('n', None)


async def test_web_output_node_forwards_to_emit_port_too():
    path = _unique_path('wo-emit')
    n = WebOutputNode('n2', {'path': path, 'sse': False})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        await n.in_pipe.put('x')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'x'
    finally:
        await n.stop()
        _nodes.pop('n2', None)


async def test_web_output_node_sse_route_streams_data():
    path = _unique_path('wo-sse')
    n = WebOutputNode('n3', {'path': path, 'sse': True})
    await n.start()
    try:
        await n.in_pipe.put('streamed-item')
        await asyncio.sleep(0.1)
        chunk = await _first_sse_chunk(path)
        assert 'streamed-item' in chunk
    finally:
        await n.stop()
        _nodes.pop('n3', None)


async def test_web_output_node_queue_full_drops_oldest():
    # WebOutputNode's internal SSE queue is bounded (maxsize=1000); when
    # full, process() drops the oldest queued item to make room for the
    # newest rather than blocking or crashing.
    path = _unique_path('wo-full')
    n = WebOutputNode('n4', {'path': path, 'sse': True})
    await n.start()
    try:
        n._queue = asyncio.Queue(maxsize=1)
        n._queue.put_nowait('first')
        await n.in_pipe.put('second')
        await asyncio.sleep(0.1)
        # The queue should now hold only the newest item.
        assert n._queue.qsize() == 1
        assert n._queue.get_nowait() == 'second'
    finally:
        await n.stop()
        _nodes.pop('n4', None)


# ---------- WebOutputJSONNode ----------

async def test_web_output_json_node_get_route_returns_latest():
    path = _unique_path('woj-plain')
    n = WebOutputJSONNode('n5', {'path': path, 'sse': False})
    await n.start()
    try:
        with TestClient(app) as c:
            empty = c.get(path)
            assert empty.json() == {'latest': None}

            await n.in_pipe.put({'a': 1})
            await asyncio.sleep(0.1)

            got = c.get(path)
            assert got.json() == {'latest': {'a': 1}}
    finally:
        await n.stop()
        _nodes.pop('n5', None)


async def test_web_output_json_node_sse_route_streams_json():
    path = _unique_path('woj-sse')
    n = WebOutputJSONNode('n6', {'path': path, 'sse': True})
    await n.start()
    try:
        await n.in_pipe.put({'k': 'v'})
        await asyncio.sleep(0.1)
        chunk = await _first_sse_chunk(path)
        assert '"k"' in chunk and '"v"' in chunk
    finally:
        await n.stop()
        _nodes.pop('n6', None)


async def test_web_output_json_node_non_serializable_item_falls_back():
    path = _unique_path('woj-fallback')
    n = WebOutputJSONNode('n7', {'path': path, 'sse': True})
    await n.start()
    try:
        # A plain object() isn't JSON-serializable - json.dumps() on it
        # raises, so the generator's except-branch wraps it as
        # {'data': str(item)} instead of crashing the stream. (A set used
        # to be the example here; since media plan phase 2 the payload
        # goes through core/media.py's to_jsonable() first, which turns a
        # set into a JSON array instead.)
        await n.in_pipe.put(object())
        await asyncio.sleep(0.1)
        chunk = await _first_sse_chunk(path)
        assert '"data"' in chunk
    finally:
        await n.stop()
        _nodes.pop('n7', None)


# ---------- UserInputNode ----------

async def test_user_input_node_post_submit_route():
    path = _unique_path('ui-sse')
    n = UserInputNode('n8', {'path': path})
    await n.start()
    try:
        out = Pipe()
        n.add_output('out', out)
        with TestClient(app) as c:
            r = c.post(f'{path}/submit', json={'value': 'typed-answer'})
            assert r.status_code == 200
            assert r.json() == {'status': 'accepted', 'value': 'typed-answer'}
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'typed-answer'
    finally:
        await n.stop()
        _nodes.pop('n8', None)


async def test_user_input_node_post_route_when_sse_disabled():
    path = _unique_path('ui-nosse')
    n = UserInputNode('n9', {'path': path, 'sse': False})
    await n.start()
    try:
        with TestClient(app) as c:
            r = c.post(path, json={'value': 'direct'})
            assert r.json()['value'] == 'direct'
    finally:
        await n.stop()
        _nodes.pop('n9', None)


async def test_user_input_node_get_route_disabled_by_default():
    path = _unique_path('ui-get-off')
    n = UserInputNode('n10', {'path': path})
    await n.start()
    try:
        with TestClient(app) as c:
            r = c.get(f'{path}/get')
            assert r.status_code == 405
    finally:
        await n.stop()
        _nodes.pop('n10', None)


async def test_user_input_node_get_route_enabled_returns_latest():
    path = _unique_path('ui-get-on')
    n = UserInputNode('n11', {'path': path, 'allow_get': True})
    await n.start()
    try:
        with TestClient(app) as c:
            c.post(f'{path}/submit', json={'value': 'stored'})
            r = c.get(f'{path}/get')
            assert r.status_code == 200
            assert r.json()['latest'] == 'stored'
    finally:
        await n.stop()
        _nodes.pop('n11', None)


async def test_user_input_node_post_without_json_body_falls_back_to_raw_text():
    path = _unique_path('ui-raw')
    n = UserInputNode('n12', {'path': path})
    await n.start()
    try:
        with TestClient(app) as c:
            r = c.post(f'{path}/submit', content=b'plain text, not json',
                       headers={'content-type': 'text/plain'})
            assert r.status_code == 200
            assert r.json()['value'] == 'plain text, not json'
    finally:
        await n.stop()
        _nodes.pop('n12', None)


async def test_user_input_node_sse_route_yields_prompt_first():
    path = _unique_path('ui-sse-prompt')
    n = UserInputNode('n13', {'path': path, 'prompt': 'Pick one:'})
    await n.start()
    try:
        chunk = await _first_sse_chunk(path)
        assert 'Pick one' in chunk
    finally:
        await n.stop()
        _nodes.pop('n13', None)
