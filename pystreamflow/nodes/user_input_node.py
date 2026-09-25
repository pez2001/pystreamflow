from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node, register_route
from fastapi import Request
from fastapi.responses import JSONResponse
import asyncio
import json

class UserInputNode(BaseNode):
    async def init(self):
        self.path = self.config.get('path', f'/user_input/{self.id}')
        self.prompt = self.config.get('prompt', 'Enter value:')
        self.allow_get = bool(self.config.get('allow_get', False))
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self._queue = asyncio.Queue(maxsize=1000)
        register_node(self)
        self._register_routes()

    def _register_routes(self):
        if not self.path.startswith('/'):
            self.path = '/' + self.path
        route_name = f"userinput_{self.id}"

        async def handle_post(request: Request):
            # See the identical fix + comment in web_server.py's
            # register_input(): stop() never used to have any effect on
            # this route at all, since it's registered once in init() and
            # runs independently of whatever task stop() cancels - a
            # "stopped" UserInputNode kept accepting and forwarding every
            # submission exactly as before.
            if not self._running:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "stopped",
                        "node": self.id,
                        "error": "node is stopped and not accepting input",
                    },
                )
            try:
                body = await request.json()
            except Exception:
                body = {"value": (await request.body()).decode('utf-8', errors='ignore')}
            value = body.get('value', body)
            await self._queue.put(value)
            self.emit('out', value)
            return JSONResponse(content={"status": "accepted", "value": value})

        async def handle_get():
            if not self.allow_get:
                return JSONResponse(content={"error": "GET not allowed"}, status_code=405)
            last = self.get_last(1)
            if last:
                item = last[-1].get('item', last[-1]) if isinstance(last[-1], dict) else last[-1]
            else:
                item = None
            return JSONResponse(content={"latest": item, "prompt": self.prompt})

        async def sse_generator():
            # initial prompt
            yield f"event: prompt\ndata: {json.dumps({'prompt': self.prompt})}\n\n"
            while True:
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=30.0)
                    yield f"event: input\ndata: {json.dumps({'value': item})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"

        # Used to branch on a config['sse'] toggle between two mutually
        # exclusive registration schemes. Feedback: "user input node has
        # a sse attribute which is making no sense" - it's an HTTP
        # wire-protocol choice with no meaningful analog on the canvas
        # (unlike ApiOutputNode's own 'sse' field, a real behavioral
        # choice for that endpoint's consumers - see output_api.py), so
        # rather than ask a person dropping this node onto a workflow to
        # understand it, both capabilities are just always available now:
        # nothing is lost, there's simply no config field to puzzle over.
        # (This also fixes a small pre-existing inconsistency: the old
        # non-SSE branch put its GET poll handler at the bare path itself
        # rather than at "/get" the way the SSE branch did - now both
        # register the same three routes the same way regardless.)
        from fastapi.responses import StreamingResponse

        async def sse_handler():
            return StreamingResponse(sse_generator(), media_type="text/event-stream")

        register_route("get", self.path, route_name, sse_handler)
        register_route("post", self.path, f"{route_name}_post", handle_post)
        # Distinct name from the bare (now always-streaming) path, so a
        # plain client with no event-stream parser can still submit.
        register_route("post", f"{self.path}/submit", f"{route_name}_submit", handle_post)
        register_route("get", f"{self.path}/get", f"{route_name}_get", handle_get)

    def manual_emit_seed(self, port: str):
        """See BaseNode.manual_emit_seed()'s docstring. Feedback: "user
        input node is not able to emit the prompt value (no prior
        output)" - a fresh UserInputNode has never had anything submitted
        to it, so manual_emit()'s default replay-the-last-item behavior
        has nothing to replay and silently no-ops, which is a
        particularly sharp edge for this node type specifically: its
        entire purpose is a human manually triggering it, which is
        exactly what the title-bar emit button is for. ``self.prompt``
        isn't a made-up stand-in value - it's the one thing this node
        instance is already configured to say - so a manual emit before
        any real submission sends that through, matching every other
        node's "the button does the node's one obvious thing" behavior.
        """
        return self.prompt if port == 'out' else None

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `path` is the
        one config field this node type's 4 already-registered routes
        are derived from - re-deriving and re-registering all 4 (each
        under the same stable name, so register_route() safely replaces
        the stale one) is what makes editing it on a live node actually
        take effect.
        """
        if 'path' not in changed_keys:
            return
        self.path = self.config.get('path', f'/user_input/{self.id}')
        self._register_routes()

    async def process(self):
        # Keep node alive, input comes via HTTP
        while self._running:
            await asyncio.sleep(0.5)

    async def stop(self):
        await super().stop()
