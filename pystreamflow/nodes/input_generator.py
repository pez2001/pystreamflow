from ..core.node import BaseNode
import asyncio

class GeneratorInputNode(BaseNode):
    """Generator Input Node.

    Emits `count` items, one every `interval` seconds, then stops.

    Bug fix (found while verifying multi-session/headless behavior via
    workflows/headless_demo.yaml, which configures this node with
    {interval: 1.0, value: 'hello'}): this used to silently ignore both
    fields entirely - it always slept a hardcoded 0.5s between emissions
    regardless of `interval`, and always emitted an auto-incrementing
    {'value': i} counter regardless of `value`, so a workflow author
    configuring either had no effect on this node's actual behavior.

    Config:
      count: number of items to emit before stopping (default 10)
      interval: seconds to sleep between emissions (default 0.5 -
        matching this node's previous hardcoded, now-configurable, delay)
      value: if set, every emitted item is {'value': <this literal
        value>} instead of an auto-incrementing counter (the default
        behavior, preserved exactly when `value` isn't configured)
    """
    async def init(self):
        self.count = int(self.config.get('count', 10))
        self.interval = float(self.config.get('interval', 0.5))
        self.value = self.config.get('value')
        self.i = 0

    async def process(self):
        while self._running and self.i < self.count:
            if self.value is not None:
                self.emit('out', {'value': self.value})
            else:
                self.emit('out', {'value': self.i})
            self.i += 1
            await asyncio.sleep(self.interval)
