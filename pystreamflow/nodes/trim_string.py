from ..core.node import BaseNode
import asyncio

class TrimStringNode(BaseNode):
    async def init(self):
        # `chars` used to only be a local variable inside process(),
        # captured once before the loop started with no same-named
        # self.chars instance attribute at all - so a live attribute-wire
        # update to `chars` had nowhere to land (BaseNode.set_attribute()'s
        # hasattr-sync never fired) and the loop kept using the original
        # value forever. Caching it here as self.chars, and re-reading it
        # fresh every iteration in process(), makes it live-reconfigurable.
        self.chars = self.config.get('chars')

    async def process(self):
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipe` used to be captured once, before this
        # loop even started - a wire arriving via POST /nodes/connect
        # after this node was created and auto-started (the normal ad-hoc
        # node-editor sequence) was never seen. Re-fetching `pipe` every
        # outer iteration - and looping instead of returning when unwired -
        # picks up a late wire within one pass instead of never.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                s = str(item)
                # An explicit chars='' means "strip nothing" - str.strip('')
                # is a real no-op distinct from str.strip()'s "strip
                # whitespace" default, so only fall back to the whitespace
                # default when chars is unset (None), not merely falsy.
                if self.chars is not None:
                    result = s.strip(self.chars)
                else:
                    result = s.strip()
                self.emit('out', result)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
