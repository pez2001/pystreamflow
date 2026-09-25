import asyncio
from pystreamflow.core.stream import Pipe
from pystreamflow.core.node import BaseNode

class DummyNode(BaseNode):
    async def process(self):
        await asyncio.sleep(0.01)

async def test_pipe():
    p = Pipe(maxsize=1)
    await p.put('a')
    assert await p.get() == 'a'

async def test_node_health():
    n = DummyNode('test')
    assert n.health()['health'] == 'unknown'

if __name__ == '__main__':
    asyncio.run(test_pipe())
    asyncio.run(test_node_health())
    print('tests passed')
