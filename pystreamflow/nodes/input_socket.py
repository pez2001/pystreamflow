from ..core.node import BaseNode
from ..core.stream import Pipe
import asyncio

class SocketInputNode(BaseNode):
    async def init(self):
        self.host = self.config.get('host', '0.0.0.0')
        self.port = int(self.config.get('port', 9001))
        self.proto = self.config.get('proto', 'tcp')
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self._server = None

    async def process(self):
        server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._server = server
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        try:
            while self._running:
                data = await reader.read(4096)
                if not data:
                    break
                text = data.decode('utf-8', errors='replace')
                self.emit('out', {'host': self.host, 'port': self.port, 'peer': addr, 'data': text})
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def stop(self):
        await super().stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
