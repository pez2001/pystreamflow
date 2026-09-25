"""
Regression tests for the boolean logic gate node family (AndNode,
OrNode, NotNode, NandNode, NorNode, XorNode, XnorNode).

Phase 5 finding: every one of these node types evaluated its wired
inputs exactly once and then returned from process() - which meant the
node's background task simply ended, and it never produced a second
result no matter how much more data arrived on its inputs. Every other
node type in this library loops for its whole lifetime; these were
copy-pasted from each other with the same missing `while self._running:`
wrapper around their evaluation logic. Fixed by adding that loop; these
tests drive each gate through multiple rounds of input to prove it now
keeps evaluating instead of going idle after one shot.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes import (
    AndNode,
    NandNode,
    NorNode,
    NotNode,
    OrNode,
    XnorNode,
    XorNode,
)

TWO_INPUT_ROUNDS = [(True, True), (True, False), (False, False), (True, True)]

TWO_INPUT_CASES = [
    (AndNode, [True, False, False, True]),
    (OrNode, [True, True, False, True]),
    (NandNode, [False, True, True, False]),
    (NorNode, [False, False, True, False]),
    (XorNode, [False, True, False, False]),
    (XnorNode, [True, False, True, True]),
]


async def _drive_two_input_gate(cls, config=None):
    n = cls('n', config or {})
    pipes = [Pipe(), Pipe()]
    n.add_input('in0', pipes[0])
    n.add_input('in1', pipes[1])
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        results = []
        for a, b in TWO_INPUT_ROUNDS:
            await pipes[0].put(a)
            await pipes[1].put(b)
            results.append(await asyncio.wait_for(out.get(), timeout=2.0))
        return results
    finally:
        await n.stop()


@pytest.mark.parametrize('cls,expected', TWO_INPUT_CASES, ids=[c.__name__ for c, _ in TWO_INPUT_CASES])
async def test_gate_keeps_evaluating_across_multiple_rounds(cls, expected):
    assert await _drive_two_input_gate(cls) == expected


async def test_not_node_keeps_evaluating_across_multiple_rounds():
    n = NotNode('n', {})
    inp = Pipe()
    n.add_input('in', inp)
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        results = []
        for v in (True, False, True, True):
            await inp.put(v)
            results.append(await asyncio.wait_for(out.get(), timeout=2.0))
        assert results == [False, True, False, False]
    finally:
        await n.stop()


@pytest.mark.parametrize('cls', [AndNode, OrNode, NandNode, NorNode, XorNode, XnorNode],
                          ids=lambda c: c.__name__)
async def test_gate_idles_with_no_input_pipes(cls):
    n = cls('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


async def test_not_node_idles_with_no_input_pipe():
    n = NotNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


async def test_and_node_waits_for_inputs_needed_before_emitting():
    # inputs_needed defaults to 2; wiring only 1 pipe means len(values)
    # never reaches inputs_needed, so the node should never emit at all -
    # confirms the fix's `while` loop doesn't emit early/incorrectly.
    n = AndNode('n', {})
    inp = Pipe()
    n.add_input('in0', inp)
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put(True)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out.get(), timeout=0.5)
    finally:
        await n.stop()
