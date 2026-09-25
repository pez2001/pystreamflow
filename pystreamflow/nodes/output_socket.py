import asyncio
import logging

from ..core.exec_guard import network_send_allowed
from ..core.node import BaseNode

_logger = logging.getLogger('pystreamflow.nodes.output_socket')


class SocketOutputNode(BaseNode):
    """Socket Output Node.

    Two modes, selected by ``config['mode']``:

    - ``'client'`` (default, unchanged from before): opens a real
      *outbound* TCP connection to ``config['host']``/``config['port']``
      for each incoming item, sends it, and closes the connection - not
      a simulated log line. A fresh connection is opened per item
      (rather than one held open across items) so this node keeps
      working even if the destination listener restarts between items.
      In this mode the node never listens on anything itself - if you
      check with ``netstat``/``ss`` (or try to connect to
      ``host:port`` yourself) you correctly won't see a listening port
      here at all, only a brief, ephemeral outbound connection each time
      an item flows through. That surprised a real user (`"socket
      output node is not opening a port"`) who reasonably expected the
      *output* node to be the listening side, symmetric with
      ``SocketInputNode`` - hence mode 'server' below.
    - ``'server'``: the node itself opens (listens on) ``host:port``,
      the same way ``SocketInputNode`` does, and *broadcasts* every
      incoming item to every currently-connected client - so anything
      that connects to this node (``nc host port``, a browser's raw
      TCP client, another PyStreamFlow instance's ``SocketInputNode``,
      etc.) receives a live copy of the stream. Connections are held
      open across items (not reopened per item, since there's nothing
      to reconnect to - clients come to it). A client disconnecting is
      simply dropped from the broadcast set; an item is still emitted
      on ``out`` even with zero clients currently connected (it was
      legitimately "sent" to whoever happened to be listening, which
      may be no one, exactly like a real broadcast).

    Also fixes the same input-port wiring bug described in
    ``ProcessOutputNode``'s docstring (waits for ``'in'`` to be wired
    instead of checking it exactly once before the loop starts).

    TRUST BOUNDARY: ``host``/``port`` come from workflow config, which -
    like every other node's config - can be set via the unauthenticated
    HTTP API (``pystreamflow/api/server.py``) or a saved workflow file.
    In client mode that means this node can be pointed at any
    network-reachable host/port from wherever this process runs,
    including internal-only services (an SSRF-shaped risk), not only at
    a legitimate downstream listener; in server mode it means a
    workflow can bind this process to any local port (and, with
    ``host='0.0.0.0'``, expose the stream to your whole network) with
    no further authentication on who can connect and read it. Both
    modes are gated by the same ``PSF_ALLOW_SOCKET_NODES``
    (``pystreamflow/core/exec_guard.py``); set it to ``0`` to
    hard-disable this node's networking entirely (it emits a clear
    error item instead of connecting/listening, rather than failing
    silently).

    Config:
      mode: ``'client'`` (default) or ``'server'``
      host: destination host in client mode / bind host in server mode
        (default ``'127.0.0.1'``)
      port: destination port in client mode / listen port in server
        mode (default ``9002``)
      timeout: seconds allowed for connect + send, client mode only
        (default ``10.0``)
      append_newline: append ``'\\n'`` to each item's encoded bytes if it
        doesn't already end with one (default ``True``)
    """

    async def init(self):
        self.mode = str(self.config.get('mode', 'client')).strip().lower()
        self.host = self.config.get('host', '127.0.0.1')
        self.port = int(self.config.get('port', 9002))
        self.timeout = float(self.config.get('timeout', 10.0))
        self.append_newline = bool(self.config.get('append_newline', True))
        self._server = None
        self._clients: set = set()

    async def process(self):
        if self.mode == 'server':
            await self._run_server_mode()
        else:
            await self._run_client_mode()

    async def _run_server_mode(self):
        # Deliberately does NOT wait for the 'in' port to be wired before
        # opening the listening socket - this is exactly the behavior gap
        # a real user hit ("socket output node is not opening a port"):
        # the whole point of server mode is that a client can connect and
        # be ready and waiting the moment this node starts, the same way
        # SocketInputNode's own listening socket comes up in init()/
        # process() independent of whether anything is wired to its
        # 'out'. The input pipe is still fetched fresh on every loop
        # iteration below (the late-wire-safe pattern used throughout
        # this codebase), so a wire drawn onto 'in' after this node was
        # already created and started is still picked up.
        if not network_send_allowed():
            msg = 'outbound socket connections disabled (PSF_ALLOW_SOCKET_NODES=0)'
            self._last_error = msg
            self.emit('out', {'sent_to': f'{self.host}:{self.port}', 'error': msg})
            return
        try:
            self._server = await asyncio.start_server(self._accept_client, self.host, self.port)
        except Exception as e:
            self._last_error = str(e)
            self.emit('out', {'sent_to': f'{self.host}:{self.port}', 'error': str(e)})
            return

        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._broadcast(item)

    async def _run_client_mode(self):
        pipe = self.inputs.get('in')
        if not pipe:
            while self._running and 'in' not in self.inputs:
                await asyncio.sleep(0.5)
            pipe = self.inputs.get('in')
        if not pipe:
            return

        while self._running:
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._send(item)

    async def _accept_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._clients.add(writer)
        try:
            # This node only ever writes to a connected client - any
            # bytes a client sends back are discarded - but it still
            # needs to notice a disconnect (a closed read returns b'')
            # so the writer can be dropped from the broadcast set
            # instead of accumulating dead connections forever.
            while self._running:
                data = await reader.read(4096)
                if not data:
                    break
        except (asyncio.CancelledError, ConnectionError):
            pass
        finally:
            self._clients.discard(writer)
            try:
                writer.close()
            except Exception:
                pass

    async def _broadcast(self, item):
        target = f'{self.host}:{self.port}'
        payload = item if isinstance(item, (bytes, bytearray)) else str(item).encode('utf-8')
        if self.append_newline and not payload.endswith(b'\n'):
            payload = payload + b'\n'

        sent_to = 0
        for writer in list(self._clients):
            try:
                writer.write(payload)
                await writer.drain()
                sent_to += 1
            except Exception:
                self._clients.discard(writer)
                try:
                    writer.close()
                except Exception:
                    pass

        self.emit('out', {
            'sent_to': target,
            'data': item,
            'bytes_sent': len(payload),
            'clients': sent_to,
        })

    async def _send(self, item):
        target = f'{self.host}:{self.port}'
        if not network_send_allowed():
            msg = 'outbound socket connections disabled (PSF_ALLOW_SOCKET_NODES=0)'
            self._last_error = msg
            self.emit('out', {'sent_to': target, 'data': item, 'error': msg})
            return

        payload = item if isinstance(item, (bytes, bytearray)) else str(item).encode('utf-8')
        if self.append_newline and not payload.endswith(b'\n'):
            payload = payload + b'\n'

        writer = None
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout
            )
            writer.write(payload)
            await asyncio.wait_for(writer.drain(), timeout=self.timeout)
        except Exception as e:
            self._last_error = str(e)
            self.emit('out', {'sent_to': target, 'data': item, 'error': str(e)})
            return
        finally:
            if writer is not None:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=self.timeout)
                except Exception:
                    # Best-effort close: the data was already sent (or we
                    # wouldn't have reached here) - a slow/uncooperative
                    # peer not finishing the close handshake promptly
                    # shouldn't be reported as a send failure.
                    _logger.debug('node %s: socket close did not complete cleanly', self.id, exc_info=True)

        self.emit('out', {'sent_to': target, 'data': item, 'bytes_sent': len(payload)})

    async def stop(self):
        await super().stop()
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
        for writer in list(self._clients):
            try:
                writer.close()
            except Exception:
                pass
        self._clients.clear()
