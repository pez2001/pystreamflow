from ..core.node import BaseNode
import asyncio
import re

class TokenizerNode(BaseNode):
    async def init(self):
        # `pattern` used to only ever be a local variable here, with no
        # same-named `self.pattern` instance attribute at all - so
        # BaseNode.set_attribute()'s hasattr-based sync never even fired,
        # and self.regex (the only thing process() reads) was never
        # recompiled. Caching it onto self.pattern below lets
        # set_attribute() update it, and process() now recompiles
        # self.regex whenever it sees a live change.
        self.pattern = self.config.get('pattern', r'\S+')
        self.regex = re.compile(self.pattern)
        self._compiled_pattern = self.pattern

    def _current_regex(self):
        if self.pattern != self._compiled_pattern:
            try:
                self.regex = re.compile(self.pattern)
                self._compiled_pattern = self.pattern
            except re.error as e:
                self._last_error = f'invalid pattern {self.pattern!r}: {e}'
        return self.regex

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
                text = str(item)
                tokens = self._current_regex().findall(text)
                for tok in tokens:
                    self.emit('out', tok)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
