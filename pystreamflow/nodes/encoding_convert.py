from ..core.node import BaseNode
import asyncio

class EncodingConvertNode(BaseNode):
    async def init(self):
        self.input_encoding = self.config.get('input_encoding', 'utf-8')
        self.output_encoding = self.config.get('output_encoding', 'utf-8')
        self.decode_to_text = bool(self.config.get('decode_to_text', True))

    async def process(self):
        # `pipe` used to be fetched once, before this loop started; if the
        # 'in' port wasn't wired at that exact instant, the whole method
        # returned for good instead of retrying, permanently ending this
        # node - see the identical fix/note in core/transform_node.py.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                try:
                    if isinstance(item, bytes):
                        text = item.decode(self.input_encoding, errors='replace')
                    else:
                        text = str(item)
                        # try to encode then decode to normalize
                        text = text.encode(self.input_encoding, errors='replace').decode(self.input_encoding, errors='replace')
                    if self.decode_to_text:
                        out = text
                    else:
                        out = text.encode(self.output_encoding, errors='replace')
                    self.emit('out', out)
                except Exception:
                    self.emit('out', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
