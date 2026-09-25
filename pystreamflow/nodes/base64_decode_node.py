from ..core.node import BaseNode
import asyncio
import base64

class Base64DecodeNode(BaseNode):
    async def init(self):
        self.input_encoding = self.config.get('input_encoding', 'utf-8')
        self.output_encoding = self.config.get('output_encoding', 'utf-8')
        self.strip_whitespace = bool(self.config.get('strip_whitespace', True))
        self.validate_padding = bool(self.config.get('validate_padding', True))

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
                data = str(item)
                if self.strip_whitespace:
                    data = ''.join(data.split())
                # Add missing padding if needed
                if self.validate_padding:
                    missing = len(data) % 4
                    if missing:
                        data += '=' * (4 - missing)
                decoded_bytes = base64.b64decode(data, validate=self.validate_padding)
                try:
                    decoded = decoded_bytes.decode(self.output_encoding)
                except Exception:
                    decoded = decoded_bytes
                self.emit('out', decoded)
            except Exception as e:
                self.emit('out', {'error': str(e), 'input': str(item)})
