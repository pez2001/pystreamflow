from ..core.node import BaseNode
import asyncio

class DisplayNode(BaseNode):
    async def init(self):
        self.prefix = self.config.get('prefix', '')
        self.suffix = self.config.get('suffix', '')

    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=0.5)
                # No brackets/label wrapping here - a node with an empty
                # `title`/label used to still print a bare "[]" marker
                # (from f"[{title}] ..." with title=''), which looked like
                # a formatting bug rather than "no title set". prefix/
                # suffix are plain literal text glued directly onto the
                # item with nothing else added, so an empty value truly
                # means nothing extra is shown.
                msg = f"{self.prefix}{item}{self.suffix}"
                print(msg)
                # also record and emit
                self.emit('out', {'display': msg, 'item': item})
            except asyncio.TimeoutError:
                continue
            except Exception:
                await asyncio.sleep(0.5)
