from pystreamflow.core.node import BaseNode
import asyncio

class ExamplePluginNode(BaseNode):
    async def init(self):
        self.msg = self.config.get('msg', 'hello from plugin')
    async def process(self):
        while self._running:
            self.emit('out', {'plugin': True, 'msg': self.msg})
            await asyncio.sleep(2)
