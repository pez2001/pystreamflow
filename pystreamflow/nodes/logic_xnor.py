from ..core.node import BaseNode
import asyncio

class XnorNode(BaseNode):
    async def init(self):
        self.inputs_needed = int(self.config.get('inputs_needed', 2))

    async def process(self):
        # See logic_and.py's AndNode for the "used to evaluate once and
        # stop" bug this loop fixes, and the separate "pipes captured once
        # outside the loop, so a late-wired input is never seen" bug fixed
        # by re-fetching `pipes` on every outer iteration - both identical
        # across the whole boolean gate family.
        while self._running:
            pipes = list(self.inputs.values())
            if not pipes:
                await asyncio.sleep(0.5)
                continue
            values = []
            for p in pipes:
                try:
                    item = await asyncio.wait_for(p.get(), timeout=0.1)
                    values.append(bool(item))
                except asyncio.TimeoutError:
                    continue
            if len(values) >= self.inputs_needed:
                result = len([v for v in values[:self.inputs_needed] if v]) % 2 == 0
                self.emit('out', result)
