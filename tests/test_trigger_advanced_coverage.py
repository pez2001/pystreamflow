"""
Coverage for pystreamflow/nodes/trigger_advanced.py's five node types
(TriggerIfNode, TriggerThresholdNode, TriggerDebounceNode,
TriggerPulseNode, TriggerToggleNode) - previously 77% covered.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.trigger_advanced import (
    TriggerDebounceNode,
    TriggerIfNode,
    TriggerPulseNode,
    TriggerThresholdNode,
    TriggerToggleNode,
)


async def _wire(node, out_name='out'):
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output(out_name, out)
    return inp, out


# ---------- TriggerIfNode ----------

async def test_trigger_if_truthy_condition():
    n = TriggerIfNode('n', {'condition': 'truthy'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put(1)
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'start'
        await inp.put(0)
        result2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result2['action'] == 'skip'
    finally:
        await n.stop()

async def test_trigger_if_equals_condition_with_field():
    n = TriggerIfNode('n', {'condition': 'equals', 'field': 'status', 'value': 'ok'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put({'status': 'ok'})
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'start'
    finally:
        await n.stop()

async def test_trigger_if_equals_condition_without_field():
    n = TriggerIfNode('n', {'condition': 'equals', 'value': 42})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put(42)
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'start'
    finally:
        await n.stop()

async def test_trigger_if_contains_condition():
    n = TriggerIfNode('n', {'condition': 'contains', 'substring': 'err'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('an error occurred')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'start'
    finally:
        await n.stop()

async def test_trigger_if_regex_condition():
    n = TriggerIfNode('n', {'condition': 'regex', 'pattern': r'^\d+$'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('12345')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'start'
        await inp.put('abc')
        result2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result2['action'] == 'skip'
    finally:
        await n.stop()

async def test_trigger_if_unknown_condition_defaults_to_skip():
    n = TriggerIfNode('n', {'condition': 'bogus'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('anything')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'skip'
    finally:
        await n.stop()

async def test_trigger_if_matches_swallows_exceptions():
    # 'equals' with a field on a non-dict item raises AttributeError inside
    # _matches() (item.get() on a string) - must be caught and treated as
    # no-match rather than crashing the node.
    n = TriggerIfNode('n', {'condition': 'equals', 'field': 'x', 'value': 1})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('not-a-dict')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['action'] == 'skip'
    finally:
        await n.stop()

async def test_trigger_if_idles_with_no_input_pipe():
    n = TriggerIfNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


# ---------- TriggerThresholdNode ----------

async def test_trigger_threshold_fires_and_resets():
    n = TriggerThresholdNode('n', {'threshold': 2, 'reset': True})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('a')
        r1 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r1 == {'count': 1}
        await inp.put('b')
        r2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r2 == {'count': 2}
        assert n._count == 0  # reset after hitting threshold
    finally:
        await n.stop()

async def test_trigger_threshold_no_reset_keeps_counting():
    n = TriggerThresholdNode('n', {'threshold': 1, 'reset': False})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('a')
        await asyncio.wait_for(out.get(), timeout=2.0)
        assert n._count == 1
        await inp.put('b')
        await asyncio.wait_for(out.get(), timeout=2.0)
        assert n._count == 2
    finally:
        await n.stop()

async def test_trigger_threshold_idles_with_no_input_pipe():
    n = TriggerThresholdNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


# ---------- TriggerDebounceNode ----------

async def test_trigger_debounce_fires_once_after_quiet_period():
    n = TriggerDebounceNode('n', {'debounce': 0.05})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('a')
        await asyncio.sleep(0.01)
        await inp.put('b')  # resets the timer before it fires
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == {'action': 'start', 'target': None}
    finally:
        await n.stop()

async def test_trigger_debounce_idles_with_no_input_pipe():
    n = TriggerDebounceNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


# ---------- TriggerPulseNode ----------

async def test_trigger_pulse_infinite_emits_repeatedly():
    n = TriggerPulseNode('n', {'interval': 0.02, 'pulse_count': 0})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first['pulse'] == 0
        assert second['pulse'] == 1
    finally:
        await n.stop()

async def test_trigger_pulse_stops_after_pulse_count():
    n = TriggerPulseNode('n', {'interval': 0.01, 'pulse_count': 2})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        await asyncio.wait_for(out.get(), timeout=2.0)
        await asyncio.wait_for(out.get(), timeout=2.0)
        await asyncio.sleep(0.1)
        # process() should have returned (break) after 2 pulses, ending
        # the node's background task on its own.
        assert n._task.done()
    finally:
        await n.stop()


# ---------- TriggerToggleNode ----------

async def test_trigger_toggle_alternates_action():
    n = TriggerToggleNode('n', {'action_on': 'start', 'action_off': 'stop'})
    inp, out = await _wire(n)
    await n.start()
    try:
        await inp.put('x')
        r1 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r1['action'] == 'start' and r1['state'] is True
        await inp.put('x')
        r2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r2['action'] == 'stop' and r2['state'] is False
    finally:
        await n.stop()

async def test_trigger_toggle_idles_with_no_input_pipe():
    n = TriggerToggleNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


@pytest.mark.parametrize('cls', [
    TriggerIfNode, TriggerThresholdNode, TriggerDebounceNode, TriggerToggleNode,
], ids=lambda c: c.__name__)
async def test_variants_actually_reach_a_registered_target(cls):
    from pystreamflow.core.web_server import _nodes
    from pystreamflow.nodes.clock_node import ClockNode

    target = ClockNode('trig-adv-target', {'interval': 0.02})
    _nodes[target.id] = target
    config = {'target_node_id': target.id}
    if cls is TriggerThresholdNode:
        config['threshold'] = 1  # default is 10 - lower it so one item fires
    n = cls('n', config)
    inp, out = await _wire(n)
    await n.start()
    try:
        assert not target._running
        await inp.put('go')
        await asyncio.wait_for(out.get(), timeout=2.0)
        await asyncio.sleep(0.05)
        assert target._running
    finally:
        await n.stop()
        await target.stop()
        _nodes.pop(target.id, None)
