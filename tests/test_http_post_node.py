"""
Real, behavior-checking tests for HttpPostNode - the outbound-HTTP-POST
node type added in response to a direct feature request: "add a node to
post data to external webservers". The real network call is mocked via
httpx.MockTransport (the same technique already used for LMStudioNode's
own outbound HTTP calls in test_json_modify_and_llm_coverage.py) rather
than requiring a live external server.
"""
import asyncio
import json

import httpx

from pystreamflow.core.port_schema import get_port_schema
from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.http_post_node import HttpPostNode


def _patch_http_post_client(monkeypatch, handler):
    import pystreamflow.nodes.http_post_node as http_post_module
    original_client = http_post_module.httpx.AsyncClient

    def patched_client(*args, **kwargs):
        kwargs['transport'] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(http_post_module.httpx, 'AsyncClient', patched_client)


async def test_http_post_node_defaults():
    n = HttpPostNode('n', {})
    await n.init()
    assert n.method == 'POST'
    assert n.content_type == 'json'
    assert n.timeout == 30.0
    assert n.headers == {}
    assert n.auth_token == ''


async def test_http_post_node_method_is_configurable_and_uppercased(monkeypatch):
    seen = {}

    def handler(request):
        seen['method'] = request.method
        return httpx.Response(200, json={'ok': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook', 'method': 'put'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({'x': 1})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert seen['method'] == 'PUT'
    finally:
        await n.stop()


async def test_http_post_node_sends_json_body_by_default(monkeypatch):
    seen = {}

    def handler(request):
        seen['url'] = str(request.url)
        seen['content_type'] = request.headers.get('content-type', '')
        seen['body'] = json.loads(request.content)
        return httpx.Response(200, json={'ok': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({'a': 1, 'b': 'two'})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert seen['url'] == 'http://example.com/hook'
        assert 'application/json' in seen['content_type']
        assert seen['body'] == {'a': 1, 'b': 'two'}
    finally:
        await n.stop()


async def test_http_post_node_wraps_non_dict_item_as_value_for_json():
    n = HttpPostNode('n', {})
    await n.init()
    assert n._request_kwargs('hello') == {'json': {'value': 'hello'}}
    assert n._request_kwargs(42) == {'json': {'value': 42}}
    assert n._request_kwargs({'a': 1}) == {'json': {'a': 1}}
    assert n._request_kwargs([1, 2]) == {'json': [1, 2]}


async def test_http_post_node_form_content_type(monkeypatch):
    seen = {}

    def handler(request):
        seen['content_type'] = request.headers.get('content-type', '')
        seen['body'] = request.content.decode()
        return httpx.Response(200, text='ok')

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook', 'content_type': 'form'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({'name': 'tim'})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert 'application/x-www-form-urlencoded' in seen['content_type']
        assert seen['body'] == 'name=tim'
    finally:
        await n.stop()


async def test_http_post_node_text_content_type(monkeypatch):
    seen = {}

    def handler(request):
        seen['body'] = request.content.decode()
        return httpx.Response(200, text='ok')

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook', 'content_type': 'text'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put('plain body text')
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert seen['body'] == 'plain body text'
    finally:
        await n.stop()


async def test_http_post_node_adds_bearer_auth_header(monkeypatch):
    seen = {}

    def handler(request):
        seen['auth'] = request.headers.get('authorization', '')
        return httpx.Response(200, json={'ok': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook', 'auth_token': 'secret123'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert seen['auth'] == 'Bearer secret123'
    finally:
        await n.stop()


async def test_http_post_node_custom_authorization_header_wins_over_auth_token(monkeypatch):
    # An explicit headers['Authorization'] is a deliberate choice (e.g. a
    # non-Bearer scheme) and should not be silently overwritten by
    # auth_token just because both happen to be configured.
    seen = {}

    def handler(request):
        seen['auth'] = request.headers.get('authorization', '')
        return httpx.Response(200, json={'ok': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {
        'url': 'http://example.com/hook',
        'auth_token': 'should-not-be-used',
        'headers': {'Authorization': 'Basic abc123'},
    })
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert seen['auth'] == 'Basic abc123'
    finally:
        await n.stop()


async def test_http_post_node_success_emits_request_response_stats_and_out(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={'received': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    request_out, response_out, stats_out, out_out = Pipe(), Pipe(), Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('request', request_out)
    n.add_output('response', response_out)
    n.add_output('stats', stats_out)
    n.add_output('out', out_out)
    await n.start()
    try:
        await inp.put({'a': 1})
        assert await asyncio.wait_for(request_out.get(), timeout=2.0) == {'a': 1}
        assert await asyncio.wait_for(response_out.get(), timeout=2.0) == {'received': True}
        stats = await asyncio.wait_for(stats_out.get(), timeout=2.0)
        assert stats['status_code'] == 200
        assert stats['url'] == 'http://example.com/hook'
        assert isinstance(stats['latency_s'], float)
        out = await asyncio.wait_for(out_out.get(), timeout=2.0)
        assert out == {'request': {'a': 1}, 'status_code': 200, 'response': {'received': True}}
    finally:
        await n.stop()


async def test_http_post_node_non_2xx_status_is_treated_as_error(monkeypatch):
    def handler(request):
        return httpx.Response(500, json={'detail': 'boom'})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    errors_out, out_out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('errors', errors_out)
    n.add_output('out', out_out)
    await n.start()
    try:
        await inp.put({'a': 1})
        error_msg = await asyncio.wait_for(errors_out.get(), timeout=2.0)
        assert '500' in error_msg
        out = await asyncio.wait_for(out_out.get(), timeout=2.0)
        assert out['request'] == {'a': 1}
        assert out['status_code'] == 500
        # The server's own error body is still attached, not lost, even
        # though this is the failure branch.
        assert out['response'] == {'detail': 'boom'}
        assert 'error' in out
    finally:
        await n.stop()


async def test_http_post_node_connection_error_emits_error_with_no_status(monkeypatch):
    def handler(request):
        raise httpx.ConnectError('connection refused', request=request)

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    errors_out, out_out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('errors', errors_out)
    n.add_output('out', out_out)
    await n.start()
    try:
        await inp.put({'a': 1})
        error_msg = await asyncio.wait_for(errors_out.get(), timeout=2.0)
        assert 'connection refused' in error_msg
        out = await asyncio.wait_for(out_out.get(), timeout=2.0)
        assert out['status_code'] is None
        assert out['response'] is None
    finally:
        await n.stop()


async def test_http_post_node_request_port_fires_even_when_the_request_fails(monkeypatch):
    def handler(request):
        raise httpx.ConnectError('down', request=request)

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    request_out = Pipe()
    n.add_input('in', inp)
    n.add_output('request', request_out)
    await n.start()
    try:
        await inp.put({'a': 1})
        assert await asyncio.wait_for(request_out.get(), timeout=2.0) == {'a': 1}
    finally:
        await n.stop()


async def test_http_post_node_falls_back_to_raw_text_when_response_is_not_json(monkeypatch):
    def handler(request):
        return httpx.Response(200, text='plain text response')

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook'})
    inp = Pipe()
    response_out = Pipe()
    n.add_input('in', inp)
    n.add_output('response', response_out)
    await n.start()
    try:
        await inp.put({'a': 1})
        assert await asyncio.wait_for(response_out.get(), timeout=2.0) == 'plain text response'
    finally:
        await n.stop()


async def test_http_post_node_builds_split_timeout_from_config(monkeypatch):
    # Mirrors LMStudioNode's identical test/fix: httpx.MockTransport
    # doesn't go through real socket I/O so it never actually enforces a
    # Timeout - this checks the mechanism (a read-leg-only configurable
    # httpx.Timeout with a short fixed connect leg) rather than timing.
    import pystreamflow.nodes.http_post_node as http_post_module

    captured = {}
    original_timeout_cls = http_post_module.httpx.Timeout

    def capturing_timeout(*args, **kwargs):
        t = original_timeout_cls(*args, **kwargs)
        captured['kwargs'] = kwargs
        captured['timeout'] = t
        return t

    monkeypatch.setattr(http_post_module.httpx, 'Timeout', capturing_timeout)

    def handler(request):
        return httpx.Response(200, json={'ok': True})

    _patch_http_post_client(monkeypatch, handler)

    n = HttpPostNode('n', {'url': 'http://example.com/hook', 'timeout': 12})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({})
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert captured['kwargs'].get('read') == 12.0
        assert captured['timeout'].connect == 10.0
        assert captured['timeout'].read == 12.0
    finally:
        await n.stop()


async def test_http_post_node_url_is_live_updatable_via_attribute_wire(monkeypatch):
    # url is a plain self.<name> config field set in init(), so it's
    # eligible for BaseNode's existing generic attribute-wire live-update
    # mechanism (set_attribute()) with no HttpPostNode-specific code
    # needed - the same way DisplayNode's prefix/suffix already are.
    n = HttpPostNode('n', {'url': 'http://example.com/original'})
    await n.init()
    n.set_attribute('url', 'http://example.com/updated')
    assert n.url == 'http://example.com/updated'
    assert n.config['url'] == 'http://example.com/updated'


def test_http_post_node_port_schema_is_fixed_five_ports():
    schema = get_port_schema('HttpPostNode')
    assert schema['inputs'] == ['in']
    assert schema['outputs'] == ['out', 'request', 'response', 'errors', 'stats']
