from ..core.node import BaseNode
import asyncio
import time

class ClockNode(BaseNode):
    async def init(self):
        self.interval = float(self.config.get('interval', 1.0))  # seconds per tick
        self.start_value = int(self.config.get('start_value', 0))
        self.count_up = bool(self.config.get('count_up', True))
        self.reset_on_input = bool(self.config.get('reset_on_input', False))
        self._counter = self.start_value

    async def process(self):
        while self._running:
            # Optionally reset on input
            if self.reset_on_input:
                pipe_in = next(iter(self.inputs.values()), None)
                if pipe_in:
                    try:
                        await asyncio.wait_for(pipe_in.get(), timeout=0.01)
                        self._counter = self.start_value
                    except asyncio.TimeoutError:
                        pass

            # Emit tick
            tick = {
                'tick': self._counter,
                'timestamp': time.time(),
                'interval': self.interval
            }
            self.emit('out', tick)

            # Update counter
            if self.count_up:
                self._counter += 1
            else:
                self._counter -= 1

            await asyncio.sleep(self.interval)
