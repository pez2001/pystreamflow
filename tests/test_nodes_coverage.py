"""
Node coverage tests
"""
import asyncio
from pystreamflow.nodes import (
    GrepNode, MergeNode, ForkNode, NumericAddNode, TextUpperNode,
    JSONModifyNode, AndNode, OrNode, NotNode, StackNode, FIFOQueueNode, LIFOQueueNode, ClockNode, HTMLScraperNode, Base64DecodeNode, Base64EncodeNode, UserPromptNode, RollingWindowBufferNode
)
from pystreamflow.nodes.table import TableNode
from pystreamflow.nodes.line_buffer import LineBufferNode
from pystreamflow.core.stream import Pipe

async def test_grep_node():
    n = GrepNode('g1', {'pattern': 'foo'})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('foo bar')
    # Wait a bit for processing
    await asyncio.sleep(0.05)
    # GrepNode should forward matching items
    # We can't easily inspect without implementation details, just ensure no error
    await n.stop()

async def test_merge_node():
    n = MergeNode('m1')
    p1 = Pipe()
    p2 = Pipe()
    out = Pipe()
    n.add_input('in1', p1)
    n.add_input('in2', p2)
    n.add_output('out', out)
    await n.start()
    await p1.put('a')
    await p2.put('b')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_fork_node():
    n = ForkNode('f1')
    inp = Pipe()
    out1 = Pipe()
    out2 = Pipe()
    n.add_input('in', inp)
    n.add_output('out1', out1)
    n.add_output('out2', out2)
    await n.start()
    await inp.put('x')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_fork_node_duplicates_to_paired_raw_outputs_too():
    # The editor's per-normal-output "raw" pairing for ForkNode (request:
    # "fork needs multiple raw outputs, each for every normal output",
    # see api/static/editor.js's psfPairedRawOutputs and rebuildPorts())
    # relies entirely on ForkNode already broadcasting to *every* wired
    # output regardless of name - port_schema.py declares ForkNode's
    # outputs DYNAMIC specifically so any name (including a UI-wired
    # 'raw0'/'raw1') is accepted. This proves that holds for a realistic
    # multi-pair setup: two normal outputs, each with its own raw
    # counterpart, all four wired under the UI's out{n}/raw{n} naming
    # scheme - every one of them must receive every item, identically.
    n = ForkNode('f-paired')
    inp = Pipe()
    out0, raw0, out1, raw1 = Pipe(), Pipe(), Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out0', out0)
    n.add_output('raw0', raw0)
    n.add_output('out1', out1)
    n.add_output('raw1', raw1)
    await n.start()
    try:
        await inp.put('x')
        for pipe in (out0, raw0, out1, raw1):
            item = await asyncio.wait_for(pipe.get(), timeout=2.0)
            assert item == 'x'
    finally:
        await n.stop()

async def test_numeric_add():
    n = NumericAddNode('add1', {'a':2,'b':3})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()

async def test_text_upper():
    n = TextUpperNode('up1')
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('hello')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_logic_nodes():
    for cls, cfg in [(AndNode, {}), (OrNode, {}), (NotNode, {})]:
        n = cls('l1', cfg)
        inp = Pipe()
        out = Pipe()
        n.add_input('in', inp)
        n.add_output('out', out)
        await n.start()
        await asyncio.sleep(0.01)
        await n.stop()

async def test_stack_node():
    n = StackNode('stack1', {'max_size': 10, 'mode': 'peek'})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('a')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_fifo_node():
    n = FIFOQueueNode('fifo1')
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('x')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_lifo_node():
    n = LIFOQueueNode('lifo1')
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('y')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_stack_node_reset_clears_generic_state_and_the_stack_itself():
    # Regression test for the new 'reset' control action: the generic
    # BaseNode.reset() only clears stats/error bookkeeping, so StackNode
    # overrides it to also clear the actual accumulated `stack` - reset()
    # wouldn't reset anything a user of this node would notice otherwise.
    n = StackNode('stack-reset', {'mode': 'push_pop'})
    await n.init()
    n.stack.extend(['a', 'b', 'c'])
    n._error_count = 2
    n._items_in = 5
    await n.reset()
    assert n.stack == []
    assert n._error_count == 0
    assert n._items_in == 0

async def test_fifo_node_reset_clears_the_queue():
    n = FIFOQueueNode('fifo-reset')
    await n.init()
    n.queue.extend(['x', 'y'])
    await n.reset()
    assert list(n.queue) == []

async def test_lifo_node_reset_clears_the_queue():
    n = LIFOQueueNode('lifo-reset')
    await n.init()
    n.queue.extend(['x', 'y'])
    await n.reset()
    assert list(n.queue) == []

async def test_table_node_reset_clears_rows():
    n = TableNode('table-reset', {'columns': ['a']})
    await n.init()
    n.rows.append({'a': 1})
    n.last_emit = 12345.0
    await n.reset()
    assert n.rows == []
    assert n.last_emit == 0

async def test_line_buffer_node_reset_clears_buffer():
    n = LineBufferNode('linebuf-reset')
    await n.init()
    n.buffer.append('partial line')
    await n.reset()
    assert n.buffer == []

async def test_clock_node():
    n = ClockNode('clock1', {'interval': 0.01, 'start_value': 0})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    await asyncio.sleep(0.05)
    await n.stop()

async def test_html_scraper_node():
    n = HTMLScraperNode('scrape1', {'extractors': [{'name':'title','css_selector':'title','text_only':True}]})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('<html><head><title>Test</title></head></html>')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_base64_decode_node():
    n = Base64DecodeNode('b64dec1', {'strip_whitespace': True})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('SGVsbG8gV29ybGQ=')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_base64_encode_node():
    n = Base64EncodeNode('b64enc1', {'urlsafe': False})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('Hello World')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_user_prompt_node():
    n = UserPromptNode('prompt1', {'prompt': 'Enter value', 'timeout': 0.1, 'default': 'default_val'})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    await asyncio.sleep(0.2)
    await n.stop()

async def test_rolling_window_buffer_node():
    n = RollingWindowBufferNode('buf1', {'max_size': 3, 'retain_after_flush': True, 'emit_passthrough': False})
    inp = Pipe()
    trig = Pipe()
    hist = Pipe()
    n.add_input('in', inp)
    n.add_input('trigger', trig)
    n.add_output('history', hist)
    await n.start()
    await inp.put('a')
    await inp.put('b')
    await inp.put('c')
    await trig.put('flush')
    await asyncio.sleep(0.05)
    await n.stop()

if __name__ == '__main__':
    async def run():
        await test_grep_node()
        await test_merge_node()
        await test_fork_node()
        await test_numeric_add()
        await test_text_upper()
        await test_logic_nodes()
        await test_stack_node()
        await test_fifo_node()
        await test_lifo_node()
        await test_clock_node()
        await test_html_scraper_node()
        await test_base64_decode_node()
        await test_base64_encode_node()
        await test_user_prompt_node()
        await test_rolling_window_buffer_node()
        print('node coverage tests passed')
    asyncio.run(run())
