from ..core.node import BaseNode
import asyncio
import time

class TimerNode(BaseNode):
    async def init(self):
        self.interval = float(self.config.get('interval', 1.0))
        self.emit_payload = self.config.get('emit_payload', 'tick')
        self.counter = 0

    async def process(self):
        while self._running:
            self.counter += 1
            payload = {
                'tick': self.counter,
                'timestamp': time.time(),
                'payload': self.emit_payload
            }
            self.emit('out', payload)
            await asyncio.sleep(self.interval)
