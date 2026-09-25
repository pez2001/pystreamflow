from ..core.node import BaseNode
import asyncio

class LineSplitterNode(BaseNode):
    async def init(self):
        self.keepends = bool(self.config.get('keepends', False))

    async def process(self):
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipe` used to be captured once, before this
        # loop even started - if this node was created and auto-started
        # (the normal ad-hoc node-editor sequence) before it was wired,
        # process() returned immediately and the node's background task
        # simply ended, so a wire arriving afterward via POST
        # /nodes/connect was never seen at all, ever. Re-fetching `pipe`
        # every outer iteration instead - and looping (not returning) when
        # unwired - picks up a late wire within one pass instead of never.
        buffer = ''
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=0.5)
                buffer += str(item)
                while '\n' in buffer:
                    if self.keepends:
                        line, _, rest = buffer.partition('\n')
                        line += '\n'
                    else:
                        line, _, rest = buffer.partition('\n')
                    self.emit('out', line)
                    buffer = rest
                # flush remaining on stop? keep buffer
            except asyncio.TimeoutError:
                # emit partial if needed
                continue
