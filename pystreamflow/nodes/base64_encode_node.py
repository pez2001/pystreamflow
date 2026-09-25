from ..core.node import BaseNode
import asyncio
import base64

class Base64EncodeNode(BaseNode):
    async def init(self):
        self.input_encoding = self.config.get('input_encoding', 'utf-8')
        self.output_encoding = self.config.get('output_encoding', 'utf-8')
        self.urlsafe = bool(self.config.get('urlsafe', False))
        self.add_newlines = bool(self.config.get('add_newlines', False))
        self.line_length = int(self.config.get('line_length', 76))

    async def process(self):
        while self._running:
            pipe_in = next(iter(self.inputs.values()), None)
            if not pipe_in:
                await asyncio.sleep(0.1)
                continue
            try:
                item = await asyncio.wait_for(pipe_in.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            try:
                # Normalize input to bytes
                if isinstance(item, bytes):
                    raw_bytes = item
                else:
                    s = str(item)
                    raw_bytes = s.encode(self.input_encoding)
                
                if self.urlsafe:
                    encoded = base64.urlsafe_b64encode(raw_bytes)
                else:
                    encoded = base64.b64encode(raw_bytes)

                if self.add_newlines:
                    encoded_str = base64.encodebytes(raw_bytes).decode(self.output_encoding)
                    # respect line_length
                    if self.urlsafe:
                        # manual wrap
                        b64 = base64.urlsafe_b64encode(raw_bytes).decode(self.output_encoding)
                        lines = [b64[i:i+self.line_length] for i in range(0, len(b64), self.line_length)]
                        encoded_str = '\n'.join(lines)
                else:
                    encoded_str = encoded.decode(self.output_encoding)

                self.emit('out', encoded_str)
            except Exception as e:
                self.emit('out', {'error': str(e), 'input': str(item)})
