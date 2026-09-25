"""
Regression tests for Phase 4's numeric_*/text_* dedup
(pystreamflow/core/transform_node.py's SingleInputTransformNode).

Before this refactor, each of these ~21 node types was a hand-copied
file implementing the same "read one item from `in`, transform it, emit
the result (or the original item unchanged on error)" loop. Collapsing
them onto one shared base class only helps if every node type still
behaves exactly as it did before - these tests drive each one through a
real Pipe end to end (not just "does it start without crashing", the way
test_nodes_coverage.py's existing checks do) to confirm that.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.core.transform_node import SingleInputTransformNode
from pystreamflow.nodes import (
    NumericAbsNode,
    NumericAddNode,
    NumericClampNode,
    NumericDivNode,
    NumericMaxNode,
    NumericMinNode,
    NumericModNode,
    NumericMulNode,
    NumericPowNode,
    NumericRoundNode,
    NumericSubNode,
    TextJoinNode,
    TextLowerNode,
    TextReplaceNode,
    TextReverseNode,
    TextSplitNode,
    TextStripNode,
    TextSubstringNode,
    TextTitleNode,
    TextTrimNode,
    TextUpperNode,
)


async def _run_one(cls, config, input_item):
    n = cls('n', config)
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put(input_item)
    try:
        result = await asyncio.wait_for(out.get(), timeout=2.0)
    finally:
        await n.stop()
    return result


CASES = [
    (NumericAddNode, {'value': 5}, 10, 15.0),
    (NumericSubNode, {'value': 3}, 10, 7.0),
    (NumericMulNode, {'value': 3}, 4, 12.0),
    (NumericDivNode, {'value': 2}, 10, 5.0),
    (NumericDivNode, {'value': 0}, 10, None),
    (NumericModNode, {'value': 3}, 10, 1.0),
    (NumericPowNode, {'value': 2}, 3, 9.0),
    (NumericMinNode, {'value': 5}, 10, 5.0),
    (NumericMaxNode, {'value': 5}, 10, 10.0),
    (NumericClampNode, {'min': 0, 'max': 5}, 10, 5.0),
    (NumericClampNode, {'min': 0, 'max': 5}, -10, 0.0),
    (NumericRoundNode, {'ndigits': 2}, 3.14159, 3.14),
    (NumericAbsNode, {}, -5, 5.0),
    # A transform that raises (can't float() a non-numeric string) must
    # pass the ORIGINAL item through unchanged, not crash the node or
    # drop the item - this is SingleInputTransformNode.process()'s
    # contract, inherited by every one of these node types.
    (NumericAddNode, {'value': 5}, 'not-a-number', 'not-a-number'),
    (TextUpperNode, {}, 'hello', 'HELLO'),
    (TextLowerNode, {}, 'HELLO', 'hello'),
    (TextTrimNode, {}, '  hi  ', 'hi'),
    (TextReplaceNode, {'find': 'a', 'replace': 'b'}, 'banana', 'bbnbnb'),
    (TextSubstringNode, {'start': 1, 'end': 3}, 'hello', 'el'),
    (TextReverseNode, {}, 'abc', 'cba'),
    (TextTitleNode, {}, 'hello world', 'Hello World'),
    (TextStripNode, {'chars': 'x'}, 'xxhixx', 'hi'),
    (TextStripNode, {}, '  hi  ', 'hi'),
    (TextSplitNode, {'sep': ','}, 'a,b,c', ['a', 'b', 'c']),
    (TextJoinNode, {'sep': '-'}, ['a', 'b', 'c'], 'a-b-c'),
    (TextJoinNode, {'sep': '-'}, 'notalist', 'notalist'),
]


@pytest.mark.parametrize('cls,config,input_item,expected', CASES, ids=[
    f"{c.__name__}:{item!r}" for c, _, item, _ in CASES
])
async def test_transform_node_result(cls, config, input_item, expected):
    assert await _run_one(cls, config, input_item) == expected


async def test_transform_node_idles_with_no_input_pipe():
    # Every one of these node types must idle rather than crash when it
    # has no 'in' pipe wired at all (e.g. dropped onto the canvas but not
    # yet connected) - SingleInputTransformNode.process() returns after
    # one 0.5s sleep in that case, same as every pre-refactor file did.
    n = NumericAddNode('n', {'value': 1})
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()


async def test_text_substring_node_does_not_shadow_start_method():
    # Regression test for a real, pre-existing bug found while verifying
    # this dedup: the original text_substring.py did
    # `self.start = int(self.config.get('start', 0))` in init(), which
    # silently replaces BaseNode's own start() lifecycle *method* with a
    # plain int on the instance. That's invisible the first time start()
    # runs (Python already resolved the bound method before init() ran),
    # but any LATER call to node.start() - a Pipe auto-starting its owning
    # node the first time something reads from it, or a pause/resume
    # cycle - would hit "TypeError: 'int' object is not callable" instead
    # of actually starting. Fixed by renaming to start_idx/end_idx; this
    # test calls start() twice, which is exactly what used to break.
    n = TextSubstringNode('n', {'start': 1, 'end': 3})
    await n.start()
    await n.start()  # must not raise
    assert isinstance(n.start_idx, int)
    await n.stop()


async def test_single_input_transform_node_default_transform_raises():
    # A subclass that forgets to implement transform() should get a clear
    # NotImplementedError (surfaced as the item passing through unchanged,
    # per process()'s own error handling) rather than silently doing
    # nothing or crashing with an unrelated AttributeError.
    class Incomplete(SingleInputTransformNode):
        pass

    n = Incomplete('n', {})
    with pytest.raises(NotImplementedError):
        n.transform('x')
