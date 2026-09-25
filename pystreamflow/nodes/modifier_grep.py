from ..core.node import BaseNode
import asyncio
import re

class GrepNode(BaseNode):
    async def init(self):
        self.pattern = self.config.get('pattern', '.*')
        self.regex = re.compile(self.pattern)
        self._compiled_pattern = self.pattern

    def _current_regex(self):
        # self.regex used to be compiled once in init() and process() only
        # ever read self.regex, never self.pattern - so a live
        # attribute-wire update to `pattern` updated self.pattern/
        # self.config['pattern'] correctly but self.regex (the only thing
        # actually used to filter) was never recompiled, making the
        # attribute-wire permanently inert. Fixed by recompiling whenever
        # the live pattern differs from what's currently compiled. An
        # invalid live pattern is reported via _last_error and the
        # previous working regex keeps being used, rather than crashing
        # the node.
        if self.pattern != self._compiled_pattern:
            try:
                self.regex = re.compile(self.pattern)
                self._compiled_pattern = self.pattern
            except re.error as e:
                self._last_error = f'invalid pattern {self.pattern!r}: {e}'
        return self.regex

    async def process(self):
        pipe = self.inputs.get('in')
        while self._running:
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    text = str(item)
                    if self._current_regex().search(text):
                        self.emit('out', item)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
