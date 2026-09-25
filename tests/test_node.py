import asyncio
from pystreamflow.core.node import BaseNode
from pystreamflow.core.stream import Pipe

class DummyNode(BaseNode):
    async def process(self):
        self.emit('out', 'test')

async def test_node_emit():
    n = DummyNode('n1', {})
    p = Pipe()
    n.add_output('out', p)
    await n.start()
    await asyncio.sleep(0.1)
    await n.stop()
    # Should have emitted
    assert len(n.get_last()) >= 1

if __name__ == '__main__':
    asyncio.run(test_node_emit())
    print('node tests passed')
