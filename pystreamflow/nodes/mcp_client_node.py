from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node
import asyncio
import json
import logging

import httpx2
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client

logger = logging.getLogger("pystreamflow.node.mcp_client")


class MCPClientNode(BaseNode):
    """A real Model Context Protocol client, built on the official `mcp`
    Python SDK: connects to any spec-compliant MCP server - this
    project's own (``pystreamflow/mcp/server.py``), or any other real one
    (LM Studio, Claude Desktop-compatible servers, etc.) - and calls one
    tool for every item received on ``in``, emitting the tool's real
    result on ``out``.

    Config:
      url        - base URL of the target MCP server's transport
                   endpoint. For the default Streamable HTTP transport
                   this is the server's mounted MCP path, e.g.
                   "http://localhost:8000/mcp" (matches this project's
                   own api/server.py mount, and LM Studio's own
                   convention for this project). For the legacy SSE
                   transport (transport: "sse") this is the SSE endpoint
                   itself, e.g. "http://localhost:8000/mcp/sse".
      transport  - "streamable_http" (default) or "sse".
      tool       - name of the tool to call for every incoming item.
      api_key    - optional bearer token, sent as an "Authorization:
                   Bearer <api_key>" header (matches mcp/server.py's own
                   require_auth()/PSF_MCP_API_KEY).
      timeout    - request timeout in seconds (default 30).

    The incoming item becomes the call's arguments: a dict is sent as-is,
    anything else is wrapped as {"input": item} so every call is always a
    well-formed JSON object regardless of what's wired upstream. One MCP
    session is opened per connection and reused across every incoming
    item for as long as it stays healthy (a real `initialize` handshake
    on every single message would be wasteful) - a connection failure
    logs a warning, is reported downstream as one {"error": ...} item so
    whatever's wired to `out` still sees it happen, and the node retries
    the connection after a short pause rather than giving up for good.

    Rewritten from an earlier hand-rolled JSON-RPC-over-HTTP
    implementation (a bespoke "POST {jsonrpc, method: tools/call, ...} to
    a bare URL" convention this project invented) at the same time
    pystreamflow/mcp/server.py itself was rewritten on the real MCP SDK
    for spec compliance (task: LM Studio couldn't connect to that server
    at all - see that module's own docstring for the full story). The old
    hand-rolled endpoint this node depended on (POST .../messages, a
    plain JSON-RPC body with no real MCP handshake) no longer exists on
    that server - and, despite this node's original docstring claiming it
    could talk to "any server exposing that same tools/list/tools/call
    JSON-RPC method pair", no other real MCP server anywhere ever
    implemented that ad-hoc convention, so this node could previously
    only ever really talk to this project's own (soon to be former) shim.
    Speaking the real transports means it now actually can connect to any
    of them, this project's own included.
    """

    async def init(self):
        # Bug-class fix applied up front (see BaseNode.add_input_if_unwired()/
        # add_output_if_unwired()'s own docstrings for the "self-contained
        # node whose init() unconditionally clobbers the Engine's real
        # wiring" bug this pattern avoids, already fixed this same way on
        # LMStudioNode and several other node types): using the
        # `_if_unwired()` variants here means a real upstream/downstream
        # pipe the Engine already wired before start() is never overwritten
        # by this node's own fallback self-pipe.
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self.url = self.config.get('url', 'http://localhost:8000/mcp')
        self.transport = self.config.get('transport', 'streamable_http')
        self.tool = self.config.get('tool', '')
        self.api_key = self.config.get('api_key', '')
        self.timeout = float(self.config.get('timeout', 30.0))
        register_node(self)

    def _headers(self):
        return {'Authorization': f'Bearer {self.api_key}'} if self.api_key else None

    def _transport_cm(self):
        # streamable_http_client() takes a pre-built httpx2.AsyncClient
        # for headers/timeout/auth (its own docstring says so directly),
        # rather than accepting them itself; sse_client() takes them
        # directly. httpx2 is the `mcp` SDK's own dependency (a
        # separately-versioned fork of httpx, distinct from this
        # project's own httpx>=0.24), used here only because it's what
        # these two client functions actually expect.
        if self.transport == 'sse':
            return sse_client(self.url, headers=self._headers(), timeout=self.timeout)
        http_client = httpx2.AsyncClient(
            headers=self._headers(), timeout=httpx2.Timeout(self.timeout),
        )
        return streamable_http_client(self.url, http_client=http_client)

    @staticmethod
    def _extract_payload(result):
        # Every tool on this project's own MCP server returns a JSON-
        # serializable dict/value, which the SDK renders as a single
        # TextContent block whose .text is that value JSON-encoded (e.g.
        # get_version's real result comes back as content=[TextContent
        # (text='{\n  "name": "pystreamflow",\n  ...}')] - not as
        # "structured content"). Decoding it back to a real object here
        # keeps this node's own emitted shape matching what the old
        # hand-rolled implementation emitted (the parsed JSON value, not
        # a JSON string) for calls against this project's own server,
        # while still degrading gracefully (falling back to the raw
        # text) against any other real MCP server that returns plain
        # text instead of JSON.
        texts = [c.text for c in (result.content or []) if getattr(c, 'text', None) is not None]
        if not texts:
            return None
        raw = texts[0] if len(texts) == 1 else texts
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw
        return raw

    async def process(self):
        while self._running:
            try:
                async with self._transport_cm() as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        while self._running:
                            # Re-resolved every iteration rather than captured
                            # once - see LMStudioNode's identical fix/comment
                            # for why (a late wire arriving after this node
                            # auto-started must still be picked up).
                            input_pipe = self.inputs.get('in', self.in_pipe)
                            try:
                                item = await asyncio.wait_for(input_pipe.get(), timeout=1.0)
                            except asyncio.TimeoutError:
                                continue

                            arguments = item if isinstance(item, dict) else {'input': item}
                            try:
                                result = await session.call_tool(self.tool, arguments)
                            except Exception as e:
                                # A single failed call doesn't necessarily mean
                                # the session itself is dead - report it and
                                # keep using the same connection for the next
                                # item, exactly like the old implementation's
                                # per-request try/except.
                                logger.warning(
                                    "node %s (MCPClientNode): call to %s (tool=%r) failed: %s",
                                    self.id, self.url, self.tool, e,
                                )
                                self.emit('out', {'tool': self.tool, 'arguments': arguments, 'error': str(e)})
                                continue

                            payload = self._extract_payload(result)
                            if result.is_error:
                                self.emit('out', {'tool': self.tool, 'arguments': arguments, 'error': payload})
                            else:
                                self.emit('out', {'tool': self.tool, 'arguments': arguments, 'result': payload})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "node %s (MCPClientNode): connection to %s failed: %s; reconnecting",
                    self.id, self.url, e,
                )
                await asyncio.sleep(1.0)
