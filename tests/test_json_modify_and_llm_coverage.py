"""
Coverage for pystreamflow/nodes/json_modify.py (JSONModifyNode's set/
rename/delete ops, previously 15% covered) and pystreamflow/nodes/
llm_lmstudio.py (LMStudioNode, previously 20% covered - its real network
call is mocked via httpx.MockTransport rather than requiring a live LM
Studio server).
"""
import asyncio
import json

import httpx

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.json_modify import JSONModifyNode
from pystreamflow.nodes.llm_lmstudio import LMStudioNode


async def _feed_and_get(node, item, timeout=2.0):
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output('out', out)
    await node.start()
    try:
        await inp.put(item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()


# ---------- JSONModifyNode ----------

async def test_json_modify_set_op_on_dict_input():
    n = JSONModifyNode('n', {'ops': [{'action': 'set', 'path': 'a.b', 'value': 5}]})
    result = await _feed_and_get(n, {'a': {}})
    assert result == {'a': {'b': 5}}

async def test_json_modify_set_op_parses_json_string_input():
    n = JSONModifyNode('n', {'ops': [{'action': 'set', 'path': 'x', 'value': 1}]})
    result = await _feed_and_get(n, '{"y": 2}')
    assert result == {'y': 2, 'x': 1}

async def test_json_modify_rename_op():
    n = JSONModifyNode('n', {'ops': [{'action': 'rename', 'from': 'old', 'to': 'new'}]})
    result = await _feed_and_get(n, {'old': 1})
    assert result == {'new': 1}

async def test_json_modify_delete_op():
    n = JSONModifyNode('n', {'ops': [{'action': 'delete', 'path': 'a'}]})
    result = await _feed_and_get(n, {'a': 1, 'b': 2})
    assert result == {'b': 2}

async def test_json_modify_unparseable_string_passes_through_as_is():
    n = JSONModifyNode('n', {'ops': [{'action': 'rename', 'from': 'x', 'to': 'y'}]})
    # Not valid JSON and not a dict, so `json.loads` fails, data stays the
    # raw string, and the rename op (which only applies to dicts) is a
    # silent no-op.
    result = await _feed_and_get(n, 'not-json')
    assert result == 'not-json'

async def test_json_modify_multiple_ops_in_sequence():
    n = JSONModifyNode('n', {'ops': [
        {'action': 'set', 'path': 'a', 'value': 1},
        {'action': 'rename', 'from': 'a', 'to': 'b'},
        {'action': 'delete', 'path': 'c'},
    ]})
    result = await _feed_and_get(n, {'c': 'gone'})
    assert result == {'b': 1}

async def test_json_modify_idles_with_no_input_pipe():
    n = JSONModifyNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


# ---------- LMStudioNode ----------

def _patch_lmstudio_client(monkeypatch, handler):
    import pystreamflow.nodes.llm_lmstudio as llm_module
    original_client = llm_module.httpx.AsyncClient

    def patched_client(*args, **kwargs):
        kwargs['transport'] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(llm_module.httpx, 'AsyncClient', patched_client)


async def test_lmstudio_node_timeout_defaults_to_600s():
    # Bug fix (real-world report): a local model can legitimately take
    # several minutes to finish generating (the report's own llama.cpp
    # log showed 218s for one completion at ~3.6 tokens/sec) - the old
    # hardcoded 60s httpx timeout aborted the request mid-generation,
    # which the server then logged as "Connection handling canceled" (the
    # client dropping the connection, not a real server-side failure).
    n = LMStudioNode('n', {})
    await n.init()
    assert n.timeout == 600.0


async def test_lmstudio_node_timeout_is_configurable():
    n = LMStudioNode('n', {'timeout': 45})
    await n.init()
    assert n.timeout == 45.0


async def test_lmstudio_node_builds_split_timeout_from_config(monkeypatch):
    # httpx.MockTransport (used by every other test in this file) doesn't
    # go through any real socket I/O, so it never actually enforces a
    # Timeout regardless of its value - there's nothing for a fixed-delay
    # mock handler to time out against (verified directly: a MockTransport
    # handler that awaits asyncio.sleep(0.3) still completes normally
    # under a read=0.05 timeout). So this checks the fix's actual
    # mechanism instead: process() must build an httpx.Timeout with the
    # configured value on the READ leg and a short, fixed connect leg -
    # not that a mock call happens to finish before some delay.
    import pystreamflow.nodes.llm_lmstudio as llm_module

    captured = {}
    original_timeout_cls = llm_module.httpx.Timeout

    def capturing_timeout(*args, **kwargs):
        t = original_timeout_cls(*args, **kwargs)
        captured['args'] = args
        captured['kwargs'] = kwargs
        captured['timeout'] = t
        return t

    monkeypatch.setattr(llm_module.httpx, 'Timeout', capturing_timeout)

    def handler(request):
        return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}]})

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {'timeout': 45})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put('hello')
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert captured['kwargs'].get('read') == 45.0
        assert captured['timeout'].connect == 10.0
        assert captured['timeout'].read == 45.0
    finally:
        await n.stop()


async def test_lmstudio_node_success_path(monkeypatch):
    def handler(request):
        body = json.loads(request.content)
        assert body['messages'][-1]['content'] == 'hello'
        return httpx.Response(200, json={
            'choices': [{'message': {'content': 'hi there'}}],
        })

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {'system': 'be nice'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put('hello')
        result = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        # 'out' carries the full picture (feature request: separate named
        # outputs for prompt/reasoning/results/errors/stats, in addition
        # to this one - see llm_lmstudio.py's process() and the dedicated
        # test_lmstudio_node_emits_to_named_ports test below).
        assert result == {
            'prompt': 'hello', 'completion': 'hi there', 'results': 'hi there',
            'reasoning': None, 'usage': {},
        }
    finally:
        await n.stop()


async def test_lmstudio_node_emits_to_named_ports(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={
            'choices': [{'message': {'content': 'hi there'}}],
            'usage': {'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5},
        })

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {})
    inp = Pipe()
    prompt_out, results_out, stats_out = Pipe(), Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('prompt', prompt_out)
    n.add_output('results', results_out)
    n.add_output('stats', stats_out)
    await n.start()
    try:
        await inp.put('hello')
        assert await asyncio.wait_for(prompt_out.get(), timeout=2.0) == 'hello'
        assert await asyncio.wait_for(results_out.get(), timeout=2.0) == 'hi there'
        stats = await asyncio.wait_for(stats_out.get(), timeout=2.0)
        assert stats['prompt_tokens'] == 3
        assert stats['completion_tokens'] == 2
        assert stats['total_tokens'] == 5
        assert isinstance(stats['latency_s'], float)
    finally:
        await n.stop()


async def test_lmstudio_node_splits_think_tag_into_reasoning(monkeypatch):
    # Some reasoning models served through LM Studio don't report a
    # separate reasoning_content field at all - they inline the chain of
    # thought as a <think>...</think> block at the start of the message
    # content instead. 'reasoning' should still end up holding just that
    # block, and 'results' just the real answer after it.
    def handler(request):
        return httpx.Response(200, json={
            'choices': [{'message': {'content': '<think>step 1, step 2</think>final answer'}}],
        })

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {})
    inp = Pipe()
    reasoning_out, results_out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('reasoning', reasoning_out)
    n.add_output('results', results_out)
    await n.start()
    try:
        await inp.put('hello')
        assert await asyncio.wait_for(reasoning_out.get(), timeout=2.0) == 'step 1, step 2'
        assert await asyncio.wait_for(results_out.get(), timeout=2.0) == 'final answer'
    finally:
        await n.stop()


async def test_lmstudio_node_error_path_emits_error(monkeypatch):
    def handler(request):
        return httpx.Response(500, text='server error')

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {})
    inp = Pipe()
    errors_out = Pipe()
    n.add_input('in', inp)
    n.add_output('errors', errors_out)
    await n.start()
    try:
        await inp.put('hello')
        result = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert result['prompt'] == 'hello'
        assert 'error' in result
        assert await asyncio.wait_for(errors_out.get(), timeout=2.0) == result['error']
    finally:
        await n.stop()


async def test_lmstudio_node_self_wires_input_when_unconnected(monkeypatch):
    # process() self-creates an 'in' pipe via add_input() if nothing
    # wired one externally before start() - unlike most node types,
    # which rely entirely on external wiring.
    def handler(request):
        return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}]})

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {})
    await n.start()
    try:
        await asyncio.sleep(0.05)
        assert 'in' in n.inputs
        await n.inputs['in'].put('auto-wired')
        result = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert result == {
            'prompt': 'auto-wired', 'completion': 'ok', 'results': 'ok',
            'reasoning': None, 'usage': {},
        }
    finally:
        await n.stop()


async def test_lmstudio_node_picks_up_a_wire_added_after_start(monkeypatch):
    # Regression test for a real bug found while verifying the "a node
    # added to a graph should default to started/active" fix
    # (api/server.py's create_node now auto-starts a node immediately
    # instead of leaving it inert until a separate manual start). That
    # makes "create node, then wire it up via POST /nodes/connect" the
    # normal order of operations for a node added to a live graph one at
    # a time - but process() used to resolve `self.inputs.get('in')`
    # exactly once, before its while loop, so a wire arriving after
    # start() was never picked up (only a wire present *before* start() -
    # the Engine's own full-workflow-run path always wires before
    # starting nodes - ever worked). Fixed by re-resolving
    # `self.inputs.get('in', self.in_pipe)` on every loop iteration
    # instead; see the identical fix + regression tests for
    # WebOutputNode/WebOutputJSONNode/ApiOutputNode in
    # test_api_nodes_and_real_network.py.
    def handler(request):
        return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}]})

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n-late-wire', {})
    await n.start()
    try:
        await asyncio.sleep(0.05)
        late_pipe = Pipe()
        n.add_input('in', late_pipe)  # simulates POST /nodes/connect after auto-start
        await late_pipe.put('late-hello')
        result = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert result == {
            'prompt': 'late-hello', 'completion': 'ok', 'results': 'ok',
            'reasoning': None, 'usage': {},
        }
    finally:
        await n.stop()


async def test_lmstudio_node_non_string_prompt_is_stringified(monkeypatch):
    captured = {}

    def handler(request):
        body = json.loads(request.content)
        captured['content'] = body['messages'][-1]['content']
        return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}]})

    _patch_lmstudio_client(monkeypatch, handler)

    n = LMStudioNode('n', {})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put(42)
        await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert captured['content'] == '42'
    finally:
        await n.stop()
