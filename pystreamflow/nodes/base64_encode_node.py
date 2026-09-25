from ..core.media import MediaItem, sniff_mime
from ..core.node import BaseNode
import asyncio
import base64

class Base64EncodeNode(BaseNode):
    """Base64-encodes text, bytes or a ``MediaItem``.

    Media plan phase 2: a ``MediaItem``'s payload is encoded (not its
    ``str()`` description), and with ``data_url: true`` the output is a
    ``data:<mime>;base64,...`` URL - the form LLM vision APIs, browsers and
    many HTTP/MQTT consumers accept an image in. For raw ``bytes`` the
    data-URL MIME type is sniffed, for text it's ``text/plain``.
    """

    async def init(self):
        self.input_encoding = self.config.get('input_encoding', 'utf-8')
        self.output_encoding = self.config.get('output_encoding', 'utf-8')
        self.urlsafe = bool(self.config.get('urlsafe', False))
        self.add_newlines = bool(self.config.get('add_newlines', False))
        self.line_length = int(self.config.get('line_length', 76))
        self.data_url = bool(self.config.get('data_url', False))

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
                mime = 'text/plain'
                if isinstance(item, MediaItem):
                    raw_bytes = await item.aget_bytes()
                    mime = item.mime
                elif isinstance(item, (bytes, bytearray)):
                    raw_bytes = bytes(item)
                    mime = sniff_mime(raw_bytes) or 'application/octet-stream'
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

                if self.data_url:
                    # a data URL is one line, never wrapped
                    encoded_str = f"data:{mime};base64,{base64.b64encode(raw_bytes).decode('ascii')}"

                self.emit('out', encoded_str)
            except Exception as e:
                self.emit('out', {'error': str(e), 'input': str(item)})
