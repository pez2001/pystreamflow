"""
Coverage for pystreamflow/core/stream.py - previously 57% covered.
Pipe's put()/close() edge cases (closed-pipe rejection, drop/raise/block
drop policies, draining on close) and the Fork/Merge fan-out/fan-in
primitives (not currently wired into any node type, per their own
docstrings, but reusable and worth exercising directly) were largely
untested.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Fork, Merge, Pipe

# ---------- Pipe ----------

async def test_put_on_closed_pipe_raises():
    p = Pipe()
    await p.close()
    with pytest.raises(RuntimeError, match='Pipe closed'):
        await p.put('x')

async def test_put_with_timeout_and_drop_policy_drops_and_counts():
    p = Pipe(maxsize=1, drop_policy='drop')
    await p.put('first')  # fills the only slot
    ok = await p.put('second', timeout=0.05)
    assert ok is False
    assert p.dropped == 1

async def test_put_with_timeout_and_raise_policy_raises_timeout():
    p = Pipe(maxsize=1, drop_policy='raise')
    await p.put('first')
    with pytest.raises(asyncio.TimeoutError):
        await p.put('second', timeout=0.05)

async def test_put_with_timeout_and_block_policy_blocks_until_space():
    p = Pipe(maxsize=1, drop_policy='block')
    await p.put('first')

    async def drain_after_delay():
        await asyncio.sleep(0.1)
        await p.get()

    drainer = asyncio.create_task(drain_after_delay())
    # timeout is shorter than the drain delay, so the initial wait_for
    # times out and falls through to the "else block forever" branch,
    # which must still eventually succeed once space frees up.
    ok = await p.put('second', timeout=0.02)
    assert ok is True
    await drainer

async def test_get_with_timeout():
    p = Pipe()
    await p.put('x')
    assert await p.get(timeout=1.0) == 'x'

async def test_get_with_timeout_raises_on_empty():
    p = Pipe()
    with pytest.raises(asyncio.TimeoutError):
        await p.get(timeout=0.05)

async def test_close_drains_pending_items():
    p = Pipe()
    await p.put('a')
    await p.put('b')
    await p.close()
    assert p.closed is True
    assert p.stats()['size'] == 0


# ---------- Fork ----------

async def test_fork_fans_out_to_all_branches():
    source = Pipe()
    fork = Fork(source)
    b1 = fork.add_branch()
    b2 = fork.add_branch()
    task = asyncio.create_task(fork.run())
    try:
        await source.put('hello')
        assert await asyncio.wait_for(b1.get(), timeout=2.0) == 'hello'
        assert await asyncio.wait_for(b2.get(), timeout=2.0) == 'hello'
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

async def test_fork_stops_when_source_closed():
    source = Pipe()
    fork = Fork(source)
    task = asyncio.create_task(fork.run())
    await asyncio.sleep(0.05)
    await source.close()
    await asyncio.wait_for(task, timeout=2.0)
    assert task.done()


# ---------- Merge ----------

async def test_merge_combines_multiple_sources():
    merge = Merge()
    s1, s2 = Pipe(), Pipe()
    merge.add_source(s1)
    merge.add_source(s2)
    task = asyncio.create_task(merge.run())
    try:
        await s1.put('from-1')
        await s2.put('from-2')
        got = {
            await asyncio.wait_for(merge.output.get(), timeout=2.0),
            await asyncio.wait_for(merge.output.get(), timeout=2.0),
        }
        assert got == {'from-1', 'from-2'}
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

async def test_merge_stops_pulling_from_a_closed_source():
    merge = Merge()
    s1, s2 = Pipe(), Pipe()
    merge.add_source(s1)
    merge.add_source(s2)
    task = asyncio.create_task(merge.run())
    try:
        await s1.put('last-from-1')
        assert await asyncio.wait_for(merge.output.get(), timeout=2.0) == 'last-from-1'
        await s1.close()
        await asyncio.sleep(0.05)
        # s2 should still be live and mergeable after s1 drops out.
        await s2.put('still-alive')
        assert await asyncio.wait_for(merge.output.get(), timeout=2.0) == 'still-alive'
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

async def test_merge_drops_a_source_whose_get_raises():
    class ExplodingSource:
        def __init__(self):
            self._raised = False
        async def get(self):
            if not self._raised:
                self._raised = True
                raise ValueError('simulated source failure')
            await asyncio.sleep(999)  # never resolves again

    merge = Merge()
    good = Pipe()
    merge.add_source(ExplodingSource())
    merge.add_source(good)
    task = asyncio.create_task(merge.run())
    try:
        await good.put('still-works')
        # The exploding source's task raised and should have been dropped
        # (not retried), while the good source keeps merging normally.
        assert await asyncio.wait_for(merge.output.get(), timeout=2.0) == 'still-works'
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

async def test_merge_with_no_sources_returns_immediately():
    merge = Merge()
    await asyncio.wait_for(merge.run(), timeout=1.0)
