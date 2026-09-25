from ..core.node import BaseNode
import asyncio

class TemplateNode(BaseNode):
    async def init(self):
        self.template = self.config.get('template', '{data}')
    async def process(self):
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipe` used to be captured once, before this
        # loop even started, so a wire arriving via POST /nodes/connect
        # after this node was created and auto-started (the normal ad-hoc
        # node-editor sequence) was never seen even though this loop kept
        # running. Re-fetching `pipe` every outer iteration picks up a late
        # wire within one pass instead of never.
        while self._running:
            pipe = self.inputs.get('in')
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    out = self.template.replace('{data}', str(item))
                    self.emit('out', out)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
