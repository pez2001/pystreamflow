from ..core.node import BaseNode
import asyncio

class NotNode(BaseNode):
    async def init(self):
        pass
    async def process(self):
        # See logic_and.py's AndNode for the "used to evaluate once and
        # stop" bug this loop fixes - NotNode had the same shape (a single
        # try/except with no surrounding while), so it emitted exactly one
        # negated value for its entire lifetime and then went idle. It also
        # had the separate "pipe captured once outside the loop, so a
        # late-wired input is never seen" bug fixed here by re-fetching
        # `pipe` on every outer iteration.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                self.emit('out', not bool(item))
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
