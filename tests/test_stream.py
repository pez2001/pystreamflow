import asyncio
from pystreamflow.core.stream import Pipe

async def test_pipe_put_get():
    p = Pipe(maxsize=1)
    await p.put('a')
    assert await p.get() == 'a'

async def test_pipe_drop():
    p = Pipe(maxsize=1, drop_policy='drop')
    await p.put('a')
    # second put should drop due to timeout
    res = await p.put('b', timeout=0.01)
    assert res is False
    assert p.dropped == 1

async def test_pipe_stats():
    p = Pipe()
    await p.put(1)
    s = p.stats()
    assert s['size'] == 1

if __name__ == '__main__':
    asyncio.run(test_pipe_put_get())
    asyncio.run(test_pipe_drop())
    asyncio.run(test_pipe_stats())
    print('stream tests passed')
