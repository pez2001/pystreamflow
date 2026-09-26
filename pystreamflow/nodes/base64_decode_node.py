from ..core.media import MediaItem
from ..core.node import BaseNode
import asyncio
import base64
import re

_DATA_URL_RE = re.compile(r'^data:([\w.+-]+/[\w.+-]+)?((?:;[\w-]+=[^;,]*)*);base64,', re.IGNORECASE)


class Base64DecodeNode(BaseNode):
    """Decodes base64 text.

    Media plan phase 2: a ``data:<mime>;base64,...`` URL is recognized
    and, with ``output: auto`` (default), decoded into a ``MediaItem`` of
    that type (text types still become a string). ``output`` forces the
    result: ``text`` (decode with ``output_encoding``, fall back to bytes),
    ``bytes`` or ``media`` (a ``MediaItem``, type from the data URL or
    sniffed). Plain base64 with ``auto`` behaves exactly as before: text
    if it decodes, otherwise bytes.
    """

    async def init(self):
        self.input_encoding = self.config.get('input_encoding', 'utf-8')
        self.output_encoding = self.config.get('output_encoding', 'utf-8')
        self.strip_whitespace = bool(self.config.get('strip_whitespace', True))
        self.validate_padding = bool(self.config.get('validate_padding', True))
        self.output = str(self.config.get('output', 'auto')).lower()
        if self.output not in ('auto', 'text', 'bytes', 'media'):
            raise ValueError(f"Base64DecodeNode: output must be auto, text, bytes or media, not {self.output!r}")

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
                mime = None
                m = _DATA_URL_RE.match(data.strip())
                if m:
                    mime = (m.group(1) or 'text/plain').lower()
                    data = data.strip()[m.end():]
                if self.strip_whitespace:
                    data = ''.join(data.split())
                # Add missing padding if needed
                if self.validate_padding:
                    missing = len(data) % 4
                    if missing:
                        data += '=' * (4 - missing)
                decoded_bytes = base64.b64decode(data, validate=self.validate_padding)
                self.emit('out', await self._result(decoded_bytes, mime))
            except Exception as e:
                self.emit('out', {'error': str(e), 'input': str(item)})

    async def _result(self, decoded_bytes: bytes, mime: str | None):
        mode = self.output
        if mode == 'auto':
            mode = 'media' if mime and not mime.startswith('text/') else 'text'
        if mode == 'media':
            return await MediaItem.afrom_bytes(decoded_bytes, mime=mime)
        if mode == 'bytes':
            return decoded_bytes
        try:
            return decoded_bytes.decode(self.output_encoding)
        except Exception:
            return decoded_bytes
