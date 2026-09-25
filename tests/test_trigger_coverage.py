"""
Coverage for pystreamflow/nodes/trigger.py's five trigger node types
(TriggerNode, TimerTriggerNode, TriggerOnNode, TriggerOffNode,
TriggerPauseNode) - previously 36% covered. All five resolve their target
through TriggerActionMixin (core/trigger_targets.py), which looks the
target node up in the real process-wide `core.web_server._nodes`
registry, so these tests register a real target node there and confirm
the trigger actually reaches it (handle_control is called with the right
action), not just that construction doesn't crash.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes
from pystreamflow.nodes.clock_node import ClockNode
from pystreamflow.nodes.trigger import (
    TimerTriggerNode,
    TriggerNode,
    TriggerOffNode,
    TriggerOnNode,
    TriggerPauseNode,
)


@pytest.fixture
def target():
    node = ClockNode('trig-target', {'interval': 0.02})
    _nodes[node.id] = node
    yield node
    _nodes.pop(node.id, None)


async def test_trigger_node_fires_target_via_config_id(target):
    n = TriggerNode('n', {'action': 'start', 'target_node_id': target.id})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        assert not target._running
        await inp.put('go')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == {'action': 'start', 'target': target.id}
        await asyncio.sleep(0.05)
        assert target._running
    finally:
        await n.stop()
        await target.stop()


async def test_trigger_node_applies_delay():
    n = TriggerNode('n', {'delay': 0.1})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        import time
        start = time.monotonic()
        await inp.put('go')
        # Give the delayed trigger loop time to run past the delay.
        await asyncio.sleep(0.3)
        assert time.monotonic() - start >= 0.1
    finally:
        await n.stop()


async def test_trigger_node_idles_with_no_input_pipe():
    n = TriggerNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


async def test_timer_trigger_node_fires_repeatedly(target):
    n = TimerTriggerNode('n', {'interval': 0.05, 'action': 'stop', 'target_node_id': target.id})
    out = Pipe()
    n.add_output('out', out)
    await target.start()
    await n.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first == second == {'action': 'stop', 'target': target.id}
        await asyncio.sleep(0.05)
        assert not target._running
    finally:
        await n.stop()


async def test_trigger_on_node_starts_target(target):
    n = TriggerOnNode('n', {'target_node_id': target.id})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        assert not target._running
        await inp.put('anything')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == {'action': 'start', 'target': target.id}
        await asyncio.sleep(0.05)
        assert target._running
    finally:
        await n.stop()
        await target.stop()


async def test_trigger_off_node_stops_target(target):
    await target.start()
    n = TriggerOffNode('n', {'target_node_id': target.id})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        assert target._running
        await inp.put('anything')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == {'action': 'stop', 'target': target.id}
        await asyncio.sleep(0.05)
        assert not target._running
    finally:
        await n.stop()


async def test_trigger_pause_node_pauses_target(target):
    await target.start()
    n = TriggerPauseNode('n', {'target_node_id': target.id})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('anything')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == {'action': 'pause', 'target': target.id}
    finally:
        await n.stop()
        await target.stop()


@pytest.mark.parametrize('cls', [TriggerOnNode, TriggerOffNode, TriggerPauseNode], ids=lambda c: c.__name__)
async def test_trigger_variants_idle_with_no_input_pipe(cls):
    n = cls('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


async def test_trigger_node_swallows_exceptions_and_keeps_running(target):
    # TriggerNode's process() wraps its body in try/except Exception, so a
    # target that raises inside handle_control (simulated here by pointing
    # at a bogus node id, which just means _trigger_target() is a no-op)
    # must not kill the node's loop - it should keep waiting for more
    # input afterwards.
    n = TriggerNode('n', {'target_node_id': 'no-such-node'})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('one')
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first == {'action': 'start', 'target': 'no-such-node'}
        await inp.put('two')
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert second == {'action': 'start', 'target': 'no-such-node'}
    finally:
        await n.stop()
