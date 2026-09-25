from ..core.node import BaseNode
import asyncio

class MergeNode(BaseNode):
    async def init(self):
        pass
    async def process(self):
        # Merge multiple inputs into one output
        # For simplicity, round-robin
        #
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipes` used to be captured once, outside this
        # loop - a wire arriving via POST /nodes/connect after this node
        # was created and auto-started (the normal ad-hoc node-editor
        # sequence) updates self.inputs but never that already-captured
        # list, so a late-wired input was never seen at all. Re-fetching
        # it every outer iteration picks up newly-wired inputs within one
        # pass instead of never.
        while self._running:
            pipes = list(self.inputs.values())
            if not pipes:
                await asyncio.sleep(0.5)
                continue
            for p in pipes:
                try:
                    item = await asyncio.wait_for(p.get(), timeout=0.1)
                    self.emit('out', item)
                except asyncio.TimeoutError:
                    continue
