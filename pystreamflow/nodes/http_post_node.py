from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node
import asyncio
import httpx
import time


class HttpPostNode(BaseNode):
    """Sends each incoming item as a real outbound HTTP request to an
    external web server - the client-side counterpart to
    ApiInputNode/WebInputNode (which only ever *receive* HTTP requests
    into a workflow, never send them out).

    Feature request, quoted verbatim: "add a node to post data to
    external webservers".

    Config:
      url          - target URL. Also updatable live via a regular
                     config/attribute edge (BaseNode.set_attribute()'s
                     existing generic mechanism - no special-casing
                     needed here), so an upstream node can retarget this
                     node between runs without recreating it.
      method       - HTTP method (default "POST"; "PUT"/"PATCH" etc. are
                     accepted too, uppercased automatically, so this one
                     node type covers the common "send this somewhere"
                     cases without needing a separate node per verb).
      content_type - how the incoming item is serialized into the
                     request body: "json" (default - a dict/list is sent
                     as-is via httpx's json=, anything else is wrapped as
                     {"value": item} so the body is always well-formed
                     JSON regardless of what's wired upstream), "form"
                     (application/x-www-form-urlencoded, same
                     dict/{"value": item} wrapping), or "text" (the raw
                     str()/bytes of the item, Content-Type left to
                     `headers` if the caller wants one).
      headers      - dict of extra request headers, merged in as-is.
      auth_token   - optional bearer token; if set and `headers` doesn't
                     already carry its own "Authorization", this adds
                     "Authorization: Bearer <auth_token>" - the same
                     convention MCPClientNode's own `api_key` uses.
      timeout      - read-leg timeout in seconds (default 30.0). Split
                     the same way LMStudioNode's own timeout fix does:
                     connect/write/pool stay fixed at a short 10s (so a
                     genuinely unreachable server fails fast), only the
                     read leg (waiting for the response once the request
                     is already sent) gets the configurable, longer
                     budget - raise it for a target server known to
                     respond slowly.

    A non-2xx response is treated as a failure (via httpx's own
    raise_for_status()), same as a real connection/timeout error, so
    `errors`/`out`'s error branch is the one place to check regardless of
    which kind of failure happened; the response body (JSON-decoded when
    possible, otherwise raw text) is still attached either way so the
    caller can see what the server actually said.

    Output ports:
      request  - echoes exactly what was received on `in`, fires
                 unconditionally (useful for logging/correlating what was
                 sent even before a response comes back) - the same
                 pattern LMStudioNode's own `prompt` port uses.
      response - the response body alone (JSON-decoded if the
                 Content-Type/body parses as JSON, otherwise the raw
                 text), only on a successful (2xx) response.
      stats    - {status_code, latency_s, url}, only on success.
      errors   - just the error string, only on failure (network error,
                 timeout, or a non-2xx status).
      out      - the full picture in one item either way: {request,
                 status_code, response} on success, or {request, error,
                 status_code, response} on failure (status_code/response
                 are included, possibly None, when the failure was itself
                 an HTTP error status rather than a connection failure -
                 so a caller can still see the server's real error body).
    """

    async def init(self):
        # add_input_if_unwired()/add_output_if_unwired(), not the plain
        # add_input()/add_output(): the established fix for the
        # "self-contained node whose init() would otherwise clobber the
        # Engine's real wiring" bug class - see LMStudioNode's/
        # MCPClientNode's identical comment for the full story. Keeps a
        # fallback pipe available for direct/standalone use (e.g. tests)
        # without ever overwriting a real graph wiring.
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self.url = self.config.get('url', '')
        self.method = str(self.config.get('method', 'POST') or 'POST').upper()
        self.content_type = self.config.get('content_type', 'json')
        self.headers = dict(self.config.get('headers') or {})
        self.auth_token = self.config.get('auth_token', '')
        self.timeout = float(self.config.get('timeout', 30.0))
        register_node(self)

    def _request_kwargs(self, item):
        if self.content_type == 'form':
            payload = item if isinstance(item, dict) else {'value': item}
            return {'data': payload}
        if self.content_type == 'text':
            body = item if isinstance(item, (str, bytes)) else str(item)
            return {'content': body}
        # 'json' (the default) and anything unrecognized fall back to it
        # rather than silently dropping the body.
        payload = item if isinstance(item, (dict, list)) else {'value': item}
        return {'json': payload}

    def _headers_for_request(self):
        headers = dict(self.headers)
        if self.auth_token and 'Authorization' not in headers:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        return headers

    @staticmethod
    def _decode_body(resp):
        try:
            return resp.json()
        except Exception:
            return resp.text

    async def process(self):
        client_timeout = httpx.Timeout(10.0, read=self.timeout)
        async with httpx.AsyncClient(timeout=client_timeout) as client:
            while self._running:
                # Re-resolved every iteration rather than captured once -
                # see LMStudioNode's identical fix/comment for why (a late
                # wire arriving after this node auto-started must still be
                # picked up).
                input_pipe = self.inputs.get('in', self.in_pipe)
                try:
                    item = await asyncio.wait_for(input_pipe.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                self.emit('request', item)
                start = time.monotonic()
                try:
                    resp = await client.request(
                        self.method,
                        self.url,
                        headers=self._headers_for_request(),
                        **self._request_kwargs(item),
                    )
                    resp.raise_for_status()
                    elapsed = time.monotonic() - start
                    body = self._decode_body(resp)
                    self.emit('response', body)
                    self.emit('stats', {
                        'status_code': resp.status_code,
                        'latency_s': round(elapsed, 3),
                        'url': self.url,
                    })
                    self.emit('out', {
                        'request': item,
                        'status_code': resp.status_code,
                        'response': body,
                    })
                except Exception as e:
                    # Covers both a real connection/timeout failure (no
                    # response at all) and httpx's raise_for_status()
                    # above (a real response with a non-2xx status) -
                    # httpx.HTTPStatusError carries the real response on
                    # e.response, so the caller can still see what the
                    # server actually said instead of just "it failed".
                    response = getattr(e, 'response', None)
                    status_code = response.status_code if response is not None else None
                    body = self._decode_body(response) if response is not None else None
                    self.emit('errors', str(e))
                    self.emit('out', {
                        'request': item,
                        'error': str(e),
                        'status_code': status_code,
                        'response': body,
                    })
