from ..core.node import BaseNode
import asyncio
from collections import deque

class RollingWindowBufferNode(BaseNode):
    async def init(self):
        self.max_size = int(self.config.get('max_size', 1000))
        self.retain_after_flush = bool(self.config.get('retain_after_flush', True))
        self.emit_passthrough = bool(self.config.get('emit_passthrough', True))
        # Plain unbounded deque, not deque(maxlen=...) - see the identical
        # note in queue_fifo_node.py/queue_lifo_node.py: a deque's maxlen
        # is fixed forever at construction, so max_size used to be frozen
        # at whatever it was when init() ran. Capacity is enforced
        # manually below, re-reading self.max_size on every append.
        self.buffer = deque()
        # Last value received on the 'retain' port, held here until a
        # flush actually consumes it - previously `retain_override` was a
        # purely local variable re-fetched (with a 0.01s timeout) on every
        # loop pass independently of whether a flush also happened that
        # same pass, so unless a 'retain' value and a 'trigger' pulse
        # landed within the same ~10ms iteration, the retain value was
        # silently read and discarded on an earlier iteration and never
        # actually applied to any flush.
        self._pending_retain = None

    async def process(self):
        while self._running:
            # Handle data input
            data_pipe = self.inputs.get('in')
            trigger_pipe = self.inputs.get('trigger')
            retain_pipe = self.inputs.get('retain')

            # Non-blocking check for data
            if data_pipe:
                try:
                    item = await asyncio.wait_for(data_pipe.get(), timeout=0.01)
                    self.buffer.append(item)
                    max_size = self.max_size
                    if max_size > 0:
                        while len(self.buffer) > max_size:
                            self.buffer.popleft()
                    if self.emit_passthrough:
                        self.emit('out', item)
                except asyncio.TimeoutError:
                    pass

            # Handle flush trigger
            flush_triggered = False
            if trigger_pipe:
                try:
                    _ = await asyncio.wait_for(trigger_pipe.get(), timeout=0.01)
                    flush_triggered = True
                except asyncio.TimeoutError:
                    pass

            # Handle retain override - remember whatever arrives until a
            # flush actually uses it, rather than only looking at "this
            # exact iteration".
            if retain_pipe:
                try:
                    self._pending_retain = await asyncio.wait_for(retain_pipe.get(), timeout=0.01)
                except asyncio.TimeoutError:
                    pass

            if flush_triggered:
                snapshot = list(self.buffer)
                self.emit('history', {'buffer': snapshot, 'size': len(snapshot)})
                # Decide whether to retain
                if self._pending_retain is not None:
                    retain = bool(self._pending_retain)
                    self._pending_retain = None
                else:
                    retain = self.retain_after_flush
                if not retain:
                    self.buffer.clear()

            await asyncio.sleep(0.01)
