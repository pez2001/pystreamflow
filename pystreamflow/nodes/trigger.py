import asyncio

from ..core.node import BaseNode
from ..core.trigger_targets import TriggerActionMixin


class TriggerNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')
        # Optional delay
        self.delay = float(self.config.get('delay', 0))

    async def process(self):
        # Wait for input trigger
        while self._running:
            try:
                pipe = next(iter(self.inputs.values()), None)
                if not pipe:
                    await asyncio.sleep(0.5)
                    continue
                await pipe.get()
                if self.delay > 0:
                    await asyncio.sleep(self.delay)
                await self._trigger_target()
                self.emit('out', {'action': self.action, 'target': self.target_node_id})
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

class TimerTriggerNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.interval = float(self.config.get('interval', 5.0))
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')

    async def process(self):
        while self._running:
            await self._trigger_target()
            self.emit('out', {'action': self.action, 'target': self.target_node_id})
            await asyncio.sleep(self.interval)

class TriggerOnNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.target_node_id = self.config.get('target_node_id')
    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            await pipe.get()
            await self._trigger('start')
            self.emit('out', {'action': 'start', 'target': self.target_node_id})

class TriggerOffNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.target_node_id = self.config.get('target_node_id')
    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            await pipe.get()
            await self._trigger('stop')
            self.emit('out', {'action': 'stop', 'target': self.target_node_id})

class TriggerPauseNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.target_node_id = self.config.get('target_node_id')
    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            await pipe.get()
            await self._trigger('pause')
            self.emit('out', {'action': 'pause', 'target': self.target_node_id})
