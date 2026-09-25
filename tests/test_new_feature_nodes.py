"""
Real, behavior-checking tests for the 6 new node types added in response
to a direct feature request (split-by-value routing, an MCP client, a
cron-scheduled source, a variable-input sync barrier, a queue+pop gate,
and a visual LED-activity relay). Each test drives the node through real
Pipes end to end and checks the actual emitted value(s), matching the
discipline already established for every other node-type test file in
this suite.
"""
import asyncio
import contextlib
import json
from typing import Optional

import httpx
import httpx2
import pytest
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from pystreamflow.core.stream import Pipe
from pystreamflow.core.port_schema import get_port_schema
from pystreamflow.nodes.modifier_split_by_value import SplitByValueNode
from pystreamflow.nodes.cron_node import CronNode, parse_cron_part
from pystreamflow.nodes.sync_barrier_node import SyncBarrierNode
from pystreamflow.nodes.queue_gate_node import QueueGateNode
from pystreamflow.nodes.led_activity_node import LedActivityNode
from pystreamflow.nodes.round_robin_node import RoundRobinNode
from pystreamflow.nodes.mcp_client_node import MCPClientNode
import pystreamflow.nodes.mcp_client_node as mcp_client_module


# ---------- SplitByValueNode ----------

async def test_split_by_value_routes_to_matching_numbered_output():
    n = SplitByValueNode('split', {'values': ['ok', 'error']})
    inp = Pipe()
    out0, out1, default = Pipe(), Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    n.add_output('default', default)
    await n.start()
    try:
        await inp.put('error')
        result = await asyncio.wait_for(out1.get(), timeout=2.0)
        assert result == 'error'
    finally:
        await n.stop()


async def test_split_by_value_unmatched_goes_to_default():
    n = SplitByValueNode('split', {'values': ['ok', 'error']})
    inp = Pipe()
    out0, default = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('default', default)
    await n.start()
    try:
        await inp.put('unrelated')
        result = await asyncio.wait_for(default.get(), timeout=2.0)
        assert result == 'unrelated'
    finally:
        await n.stop()


async def test_split_by_value_matches_on_a_dot_path_key():
    # Real-world shape: route a dict message by one of its own fields
    # rather than the whole item - mirrors JSONExtractNode's `path` syntax.
    n = SplitByValueNode('split', {'values': ['warn', 'crit'], 'key': 'level'})
    inp = Pipe()
    out1, default = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out1', out1)
    n.add_output('default', default)
    await n.start()
    try:
        await inp.put({'level': 'crit', 'msg': 'disk full'})
        result = await asyncio.wait_for(out1.get(), timeout=2.0)
        assert result == {'level': 'crit', 'msg': 'disk full'}
    finally:
        await n.stop()


async def test_split_by_value_non_strict_compares_stringified_by_default():
    # Config values arrive from the editor's text widget as strings even
    # when the upstream value is a real int - the default (non-strict)
    # comparison must still match "404" (str) against 404 (int).
    n = SplitByValueNode('split', {'values': ['404']})
    inp = Pipe()
    out0 = Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    await n.start()
    try:
        await inp.put(404)
        result = await asyncio.wait_for(out0.get(), timeout=2.0)
        assert result == 404
    finally:
        await n.stop()


def test_split_by_value_port_schema_is_dynamic_output():
    schema = get_port_schema('SplitByValueNode')
    assert schema['inputs'] == ['in']
    assert schema['outputs'] == '*'


# ---------- CronNode ----------

def test_parse_cron_part_supports_star_range_step_and_list():
    assert parse_cron_part('*', 0, 4) == {0, 1, 2, 3, 4}
    assert parse_cron_part('1-3', 0, 10) == {1, 2, 3}
    assert parse_cron_part('*/15', 0, 59) == {0, 15, 30, 45}
    assert parse_cron_part('0,30', 0, 59) == {0, 30}
    assert parse_cron_part('9-17/2', 0, 23) == {9, 11, 13, 15, 17}


async def test_cron_node_fires_on_permissive_expression():
    # "* * * * *" matches every minute, including the one this test runs
    # in - a fast check_interval means we don't have to wait for a real
    # minute boundary to see it.
    n = CronNode('cron', {'cron': '* * * * *', 'check_interval': 0.02})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert item['cron'] == '* * * * *'
        assert item['tick'] == 1
        assert 'timestamp' in item
    finally:
        await n.stop()


async def test_cron_node_does_not_fire_on_a_minute_that_cannot_match():
    # Minute field restricted to a value that can never equal "now" for
    # the ~60s this test could conceivably run - a real behavioral
    # negative check, not just "the constructor didn't raise".
    from datetime import datetime, timezone
    now_minute = datetime.now(timezone.utc).minute
    impossible_minute = (now_minute + 30) % 60
    n = CronNode('cron', {'cron': f'{impossible_minute} * * * *', 'check_interval': 0.02})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out.get(), timeout=0.3)
    finally:
        await n.stop()


async def test_cron_node_rejects_a_malformed_expression():
    n = CronNode('cron', {'cron': 'not a cron expression'})
    with pytest.raises(ValueError, match='5-field'):
        await n.init()


# ---------- SyncBarrierNode ----------

async def test_sync_barrier_waits_for_every_input_then_releases_all_at_once():
    n = SyncBarrierNode('barrier', {'flush': True})
    in0, in1 = Pipe(), Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in0', in0)
    n.add_input('in1', in1)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        await in0.put('a')
        # Only one of two inputs has contributed - nothing should be
        # released yet.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out0.get(), timeout=0.3)
        await in1.put('b')
        r0 = await asyncio.wait_for(out0.get(), timeout=2.0)
        r1 = await asyncio.wait_for(out1.get(), timeout=2.0)
        assert (r0, r1) == ('a', 'b')
    finally:
        await n.stop()


async def test_sync_barrier_flush_true_requires_a_fresh_value_on_every_input_again():
    n = SyncBarrierNode('barrier', {'flush': True})
    in0, in1 = Pipe(), Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in0', in0)
    n.add_input('in1', in1)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        await in0.put('a1')
        await in1.put('b1')
        await asyncio.wait_for(out0.get(), timeout=2.0)
        await asyncio.wait_for(out1.get(), timeout=2.0)
        # After a flush, a second message on only ONE input must not
        # release anything again until the other one also contributes.
        await in0.put('a2')
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out0.get(), timeout=0.3)
    finally:
        await n.stop()


async def test_sync_barrier_flush_false_holds_and_re_releases_on_any_update():
    n = SyncBarrierNode('barrier', {'flush': False})
    in0, in1 = Pipe(), Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in0', in0)
    n.add_input('in1', in1)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        await in0.put('a1')
        await in1.put('b1')
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == 'a1'
        assert await asyncio.wait_for(out1.get(), timeout=2.0) == 'b1'
        # Without flushing, a new value on just ONE input should
        # immediately trigger another release using the held value for
        # the other.
        await in0.put('a2')
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == 'a2'
        assert await asyncio.wait_for(out1.get(), timeout=2.0) == 'b1'
    finally:
        await n.stop()


# ---------- QueueGateNode ----------

async def test_queue_gate_relays_one_item_per_pop_in_fifo_order():
    n = QueueGateNode('gate', {})
    data_in, pop_in, out = Pipe(), Pipe(), Pipe()
    n.add_input('in', data_in)
    n.add_input('pop', pop_in)
    n.add_output('out', out)
    await n.start()
    try:
        await data_in.put('first')
        await data_in.put('second')
        await asyncio.sleep(0.1)  # let both land in the queue
        await pop_in.put(None)  # content is discarded - only arrival matters
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'first'
        # A second pop releases the next queued item, not "first" again.
        await pop_in.put('ignored-content')
        result2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result2 == 'second'
    finally:
        await n.stop()


async def test_queue_gate_pop_on_empty_queue_is_a_no_op():
    n = QueueGateNode('gate', {})
    data_in, pop_in, out = Pipe(), Pipe(), Pipe()
    n.add_input('in', data_in)
    n.add_input('pop', pop_in)
    n.add_output('out', out)
    await n.start()
    try:
        await pop_in.put(None)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out.get(), timeout=0.3)
    finally:
        await n.stop()


def test_queue_gate_port_schema_has_two_named_inputs():
    schema = get_port_schema('QueueGateNode')
    assert schema['inputs'] == ['in', 'pop']


# ---------- LedActivityNode ----------

async def test_led_activity_relays_each_input_to_its_matching_output():
    n = LedActivityNode('led', {})
    in0, in1 = Pipe(), Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in0', in0)
    n.add_input('in1', in1)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        await in1.put('hello')
        result = await asyncio.wait_for(out1.get(), timeout=2.0)
        assert result == 'hello'
        # out0 must not have received anything from an in1 message.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out0.get(), timeout=0.2)
    finally:
        await n.stop()


def test_led_activity_port_schema_is_dynamic_both_ways():
    schema = get_port_schema('LedActivityNode')
    assert schema['inputs'] == '*'
    assert schema['outputs'] == '*'


# ---------- RoundRobinNode ----------
#
# Feature request, verbatim: "add a node to feed inputs to multiple
# outputs and it cycles through all outputs (first input message gets to
# the first output, the second incoming message goes to the second
# output. if all outputs received one message start the cycle again)".

async def test_round_robin_cycles_through_outputs_in_arrival_order():
    n = RoundRobinNode('rr', {'count': 3})
    inp = Pipe()
    out0, out1, out2 = Pipe(), Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    n.add_output('out2', out2)
    await n.start()
    try:
        await inp.put('a')
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == 'a'
        await inp.put('b')
        assert await asyncio.wait_for(out1.get(), timeout=2.0) == 'b'
        await inp.put('c')
        assert await asyncio.wait_for(out2.get(), timeout=2.0) == 'c'
    finally:
        await n.stop()


async def test_round_robin_wraps_back_to_the_first_output_after_a_full_cycle():
    n = RoundRobinNode('rr-wrap', {'count': 2})
    inp = Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        for item in ('1', '2', '3', '4'):
            await inp.put(item)
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == '1'
        assert await asyncio.wait_for(out1.get(), timeout=2.0) == '2'
        # Third message wraps back to out0, fourth to out1 again - "if all
        # outputs received one message start the cycle again".
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == '3'
        assert await asyncio.wait_for(out1.get(), timeout=2.0) == '4'
    finally:
        await n.stop()


async def test_round_robin_advances_its_turn_even_when_that_output_is_unwired():
    # The cycle position is a strict, positional schedule - "2nd message
    # goes to the 2nd output" - not "skip to whichever output happens to
    # be wired". With only out0/out2 wired (out1 deliberately left
    # unconnected), the 2nd message's turn (out1) is simply not delivered
    # anywhere, and the 3rd message still lands on out2 as its own turn
    # dictates, rather than sliding into out1's empty slot.
    n = RoundRobinNode('rr-gap', {'count': 3})
    inp = Pipe()
    out0, out2 = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('out2', out2)
    await n.start()
    try:
        await inp.put('first')
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == 'first'
        await inp.put('second')  # this one's turn is out1, which is unwired
        await inp.put('third')
        assert await asyncio.wait_for(out2.get(), timeout=2.0) == 'third'
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out0.get(), timeout=0.2)
    finally:
        await n.stop()


async def test_round_robin_default_count_is_three():
    n = RoundRobinNode('rr-default', {})
    await n.init()
    assert n.count == 3


async def test_round_robin_count_is_clamped_to_at_least_one():
    n = RoundRobinNode('rr-clamped', {'count': 0})
    await n.init()
    assert n.count == 1


async def test_round_robin_reset_restarts_the_cycle_at_the_first_output():
    n = RoundRobinNode('rr-reset', {'count': 2})
    inp = Pipe()
    out0, out1 = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('out1', out1)
    await n.start()
    try:
        await inp.put('x')
        await asyncio.wait_for(out0.get(), timeout=2.0)
        # Now mid-cycle (next turn is out1). Reset should put it back at
        # out0 - the same "restart the cycle" the feature request itself
        # describes - not leave it wherever it happened to be.
        await n.reset()
        await inp.put('y')
        assert await asyncio.wait_for(out0.get(), timeout=2.0) == 'y'
    finally:
        await n.stop()


def test_round_robin_port_schema_is_dynamic_output():
    schema = get_port_schema('RoundRobinNode')
    assert schema['inputs'] == ['in']
    assert schema['outputs'] == '*'


# ---------- MCPClientNode ----------
#
# MCPClientNode was rewritten on the real `mcp` Python SDK client at the
# same time pystreamflow/mcp/server.py itself was rewritten on the SDK
# server side for spec compliance (task: LM Studio couldn't connect - see
# that module's own docstring). The node's old hand-rolled JSON-RPC-over-
# HTTP implementation (a bespoke, non-standard convention only this
# project's own now-removed /messages endpoint ever understood) is gone,
# so these tests no longer mock a raw httpx handler - they drive the node
# against a real, tiny, purpose-built MCP server (`fake_mcp_app` below)
# over the real Streamable HTTP transport, in-process via an ASGI
# transport (no real network socket needed for these three - they're
# testing the node's own argument-wrapping/result/error-shape behavior,
# not the network layer). The fourth test still separately confirms this
# node can talk to this project's *own* real mcp/server.py server, now
# via a real subprocess (see the live_mcp_server fixture in conftest.py).

@contextlib.asynccontextmanager
async def fake_mcp_app():
    """A minimal, real MCPServer (not a mock) with a single `echo` tool
    that just reflects whatever `input`/`x` arguments it received - real
    enough to exercise the actual MCP protocol end-to-end (initialize,
    tools/call, real error responses for an unknown tool), while staying
    small and fast enough to build fresh per test.

    A plain async context manager used directly inside each test's own
    coroutine (`async with fake_mcp_app() as app:`), not a pytest fixture:
    entering it wraps an anyio task group (the SDK's own
    StreamableHTTPSessionManager.run()), and anyio's cancel scopes require
    the exact same asyncio Task to both enter and exit a scope - a
    function-scoped async-generator *fixture* has its setup and teardown
    halves driven as separate steps by pytest-asyncio, which isn't
    guaranteed to resume the generator on the same Task, and reliably
    tripped "Attempted to exit cancel scope in a different task than it
    was entered in" here. Using it as an ordinary context manager inside
    the test body itself keeps setup and teardown in the one Task actually
    running the test, sidestepping that entirely.

    transport_security is explicitly disabled here because the SDK's
    low-level Server.streamable_http_app() auto-enables DNS-rebinding
    protection for a loopback `host` (its default), which only allow-
    lists real 127.0.0.1/localhost Host headers - not the synthetic
    "testserver" host an in-process ASGI transport uses. That protection
    matters for a server accepting real network connections (like the
    live subprocess server in conftest.py); it's irrelevant for a fake
    server that's never reachable from outside this process.
    """
    m = MCPServer(name='pystreamflow-test-echo-server')

    @m.tool()
    def echo(input: Optional[str] = None, x: Optional[int] = None) -> dict:
        return {'input': input, 'x': x}

    app = m.streamable_http_app(
        streamable_http_path='/',
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    async with app.router.lifespan_context(app):
        yield app


def _patch_mcp_client_to_asgi_app(monkeypatch, app):
    # MCPClientNode's streamable_http transport builds its own
    # httpx2.AsyncClient (see mcp_client_node.py's _transport_cm()) -
    # patching httpx2.AsyncClient itself (mirrors the discipline of the
    # old test's identical patch of the project's own httpx.AsyncClient)
    # redirects that client onto this fake app instead of a real socket.
    original_client = mcp_client_module.httpx2.AsyncClient

    def patched_client(*args, **kwargs):
        kwargs['transport'] = httpx2.ASGITransport(app=app)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(mcp_client_module.httpx2, 'AsyncClient', patched_client)


async def test_mcp_client_node_calls_tool_and_emits_real_result(monkeypatch):
    async with fake_mcp_app() as app:
        _patch_mcp_client_to_asgi_app(monkeypatch, app)

        n = MCPClientNode('mcp', {'url': 'http://testserver/', 'tool': 'echo'})
        inp = Pipe()
        n.add_input('in', inp)
        await n.start()
        try:
            await inp.put({'x': 1})
            result = await asyncio.wait_for(n.out_pipe.get(), timeout=5.0)
            assert result['tool'] == 'echo'
            assert result['result'] == {'input': None, 'x': 1}
        finally:
            await n.stop()


async def test_mcp_client_node_wraps_non_dict_input_as_arguments(monkeypatch):
    async with fake_mcp_app() as app:
        _patch_mcp_client_to_asgi_app(monkeypatch, app)

        n = MCPClientNode('mcp', {'url': 'http://testserver/', 'tool': 'echo'})
        inp = Pipe()
        n.add_input('in', inp)
        await n.start()
        try:
            await inp.put('plain-string-input')
            result = await asyncio.wait_for(n.out_pipe.get(), timeout=5.0)
            assert result['result'] == {'input': 'plain-string-input', 'x': None}
        finally:
            await n.stop()


async def test_mcp_client_node_emits_error_when_tool_call_fails(monkeypatch):
    async with fake_mcp_app() as app:
        _patch_mcp_client_to_asgi_app(monkeypatch, app)

        n = MCPClientNode('mcp', {'url': 'http://testserver/', 'tool': 'nonexistent_tool'})
        inp = Pipe()
        n.add_input('in', inp)
        await n.start()
        try:
            await inp.put({})
            result = await asyncio.wait_for(n.out_pipe.get(), timeout=5.0)
            assert 'error' in result
            assert 'nonexistent_tool' in str(result['error'])
        finally:
            await n.stop()


async def test_mcp_client_node_connects_to_the_projects_own_mcp_server(live_mcp_server):
    """End-to-end integration check, not a mock: drives MCPClientNode
    against this project's *real* mcp/server.py server, running as a real
    subprocess (see conftest.py's live_mcp_server fixture), calling the
    real `get_version` tool and asserting on the real response it
    returns. Confirms the two sides of this feature (a new MCP client
    node, and this project's own MCP server) actually speak the same
    protocol to each other, not just that each one's own isolated test
    passes.
    """
    n = MCPClientNode('mcp', {'url': f'{live_mcp_server}/', 'tool': 'get_version'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put({})
        result = await asyncio.wait_for(n.out_pipe.get(), timeout=5.0)
        assert result['tool'] == 'get_version'
        assert result.get('result', {}).get('name') == 'pystreamflow'
    finally:
        await n.stop()
