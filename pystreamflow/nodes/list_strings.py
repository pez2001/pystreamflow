from ..core.node import BaseNode
import asyncio

class ListStringsNode(BaseNode):
    async def init(self):
        self.strings = self.config.get('strings', [])
        if isinstance(self.strings, str):
            # allow comma-separated string
            self.strings = [s.strip() for s in self.strings.split(',')]
        self.interval = float(self.config.get('interval', 1.0))
        self.loop = bool(self.config.get('loop', True))
        self.index = 0

    async def process(self):
        while self._running:
            if not self.strings:
                await asyncio.sleep(self.interval)
                continue
            # A live attribute-wire can shrink `strings` at any time; clamp
            # self.index back into range first instead of indexing
            # directly, which used to raise an uncaught IndexError (and,
            # with the engine's default retries=0, permanently kill the
            # node) whenever strings shrank to at or below the current
            # index.
            if self.index >= len(self.strings):
                self.index = 0
            item = self.strings[self.index]
            self.emit('out', {'value': item, 'index': self.index})
            self.index += 1
            if not self.loop and self.index >= len(self.strings):
                # emit end and stop
                self.emit('out', {'value': None, 'done': True})
                break
            if self.loop:
                self.index %= len(self.strings)
            await asyncio.sleep(self.interval)
