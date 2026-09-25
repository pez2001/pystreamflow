from ..core.node import BaseNode
import asyncio

class LineBufferNode(BaseNode):
    async def init(self):
        self.mode = self.config.get('mode', 'multi')  # 'single' or 'multi'
        self.lines = int(self.config.get('lines', 1))
        self.timeout = float(self.config.get('timeout', 0.5))
        self.buffer = []

    async def reset(self):
        # See the identical note on StackNode.reset(): the generic
        # BaseNode.reset() only clears stats/error bookkeeping, so the
        # actual accumulated line buffer needs its own clear here.
        await super().reset()
        self.buffer = []

    async def process(self):
        # See the identical note in core/transform_node.py: re-fetch
        # self.inputs.get('in') every pass instead of once before the
        # loop, so a port wired after this node starts is actually noticed
        # instead of ending the node's process task for good.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=self.timeout)
                self.buffer.append(str(item))
                if self.mode == 'single':
                    # emit each line immediately
                    for line in self.buffer:
                        self.emit('out', line)
                    self.buffer = []
                else:
                    if len(self.buffer) >= self.lines:
                        self.emit('out', self.buffer.copy())
                        self.buffer = []
            except asyncio.TimeoutError:
                # flush on timeout
                if self.buffer:
                    if self.mode == 'single':
                        for line in self.buffer:
                            self.emit('out', line)
                    else:
                        self.emit('out', self.buffer.copy())
                    self.buffer = []
