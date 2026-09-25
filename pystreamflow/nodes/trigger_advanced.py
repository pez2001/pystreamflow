import asyncio

from ..core.node import BaseNode
from ..core.trigger_targets import TriggerActionMixin


class TriggerIfNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.condition = self.config.get('condition', 'truthy')
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')
        self.field = self.config.get('field')

    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.2)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if self._matches(item):
                await self._trigger()
                self.emit('out', {'action': self.action, 'target': self.target_node_id, 'item': item})
            else:
                self.emit('out', {'action': 'skip', 'target': self.target_node_id, 'item': item})

    def _matches(self, item):
        try:
            if self.condition == 'truthy':
                return bool(item)
            if self.condition == 'equals':
                val = self.config.get('value')
                if self.field:
                    return item.get(self.field) == val
                return item == val
            if self.condition == 'contains':
                sub = self.config.get('substring', '')
                return sub in str(item)
            if self.condition == 'regex':
                import re
                pat = self.config.get('pattern', '')
                return re.search(pat, str(item)) is not None
        except Exception:
            return False
        return False

class TriggerThresholdNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.threshold = int(self.config.get('threshold', 10))
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')
        self.reset = bool(self.config.get('reset', True))
        self._count = 0

    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.2)
                continue
            try:
                await asyncio.wait_for(pipe.get(), timeout=0.5)
                self._count += 1
                self.emit('out', {'count': self._count})
                if self._count >= self.threshold:
                    await self._trigger()
                    if self.reset:
                        self._count = 0
            except asyncio.TimeoutError:
                continue

class TriggerDebounceNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.debounce = float(self.config.get('debounce', 1.0))
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')
        self._timer = None

    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.2)
                continue
            try:
                await asyncio.wait_for(pipe.get(), timeout=0.5)
                # reset timer
                if self._timer:
                    self._timer.cancel()
                self._timer = asyncio.create_task(self._delayed_trigger())
            except asyncio.TimeoutError:
                continue

    async def _delayed_trigger(self):
        await asyncio.sleep(self.debounce)
        await self._trigger()
        self.emit('out', {'action': self.action, 'target': self.target_node_id})
        self._timer = None

class TriggerPulseNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.interval = float(self.config.get('interval', 5.0))
        self.pulse_count = int(self.config.get('pulse_count', 0))  # 0 = infinite
        self.action = self.config.get('action', 'start')
        self.target_node_id = self.config.get('target_node_id')
        self._sent = 0

    async def process(self):
        while self._running:
            await self._trigger()
            self.emit('out', {'action': self.action, 'target': self.target_node_id, 'pulse': self._sent})
            self._sent += 1
            if self.pulse_count > 0 and self._sent >= self.pulse_count:
                break
            await asyncio.sleep(self.interval)

class TriggerToggleNode(TriggerActionMixin, BaseNode):
    async def init(self):
        self.action_on = self.config.get('action_on', 'start')
        self.action_off = self.config.get('action_off', 'stop')
        self.target_node_id = self.config.get('target_node_id')
        self._state = False

    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.2)
                continue
            try:
                await asyncio.wait_for(pipe.get(), timeout=0.5)
                self._state = not self._state
                action = self.action_on if self._state else self.action_off
                await self._trigger(action)
                self.emit('out', {'action': action, 'target': self.target_node_id, 'state': self._state})
            except asyncio.TimeoutError:
                continue
