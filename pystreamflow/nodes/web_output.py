from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node, register_route
import asyncio
import json
from fastapi.responses import StreamingResponse

class WebOutputNode(BaseNode):
    async def init(self):
        # Bug fix: this used to default to '127.0.0.1', unreachable
        # through the Docker-published port for this node's shared web
        # server - see ApiInputNode.init()'s comment for the full
        # explanation and how this was confirmed.
        self.host = self.config.get('host', '0.0.0.0')
        self.port = self.config.get('port', 8080)
        self.path = self.config.get('path', f'/out/{self.id}')
        self.format = self.config.get('format', 'text')
        self.sse_enabled = bool(self.config.get('sse', True))
        # add_input_if_unwired(), not add_input(): Engine._wire_edges()
        # always wires a node's real upstream output into this node's
        # 'in' port *before* calling start() (which is what runs init()
        # and reaches this line) - so an unconditional add_input() here
        # used to silently overwrite that real wiring with a fresh,
        # disconnected Pipe() that no upstream node ever writes to. In any
        # workflow run through the Engine, this node's process() loop was
        # therefore reading from a pipe nothing ever put() into, so it
        # never received any real data at all - only a direct test/script
        # calling `.in_pipe.put()` itself could ever see output. Using
        # add_input_if_unwired() keeps the internal pipe as a fallback for
        # exactly that direct/standalone use, without clobbering a real
        # graph wiring when one exists.
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        register_node(self)
        self._queue = asyncio.Queue(maxsize=1000)
        self._register_route()
        self._running_stream = True

    def _register_route(self):
        if not self.path.startswith('/'):
            self.path = '/' + self.path
        route_name = f"webout_{self.id}"

        if self.sse_enabled:
            async def stream_generator():
                # send existing last items first
                for item in self.get_last(20):
                    yield f"data: {item}\n\n"
                # then live
                while True:
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                        if isinstance(item, str):
                            payload = item
                        else:
                            payload = str(item)
                        yield f"data: {payload}\n\n"
                    except asyncio.TimeoutError:
                        # keep connection alive
                        yield ": keepalive\n\n"

            async def handler():
                return StreamingResponse(stream_generator(), media_type="text/event-stream")

            register_route("get", self.path, route_name, handler)
        else:
            from fastapi.responses import PlainTextResponse

            async def handler():
                # Return latest item as plain text
                last = self.get_last(1)
                if last:
                    payload = str(last[-1].get('item', '')) if isinstance(last[-1], dict) else str(last[-1])
                else:
                    payload = ''
                return PlainTextResponse(payload)

            register_route("get", self.path, route_name, handler)

    async def process(self):
        while self._running:
            try:
                # self.inputs.get('in', self.in_pipe), not a bare
                # self.in_pipe.get(): bug fix found while auditing the
                # ad-hoc "POST /nodes then POST /nodes/connect" node
                # creation path (api/server.py's create_node now
                # auto-starts a node the instant it's created - see that
                # change's own comment - instead of leaving it inert
                # until a separate manual start). self.in_pipe is
                # captured once in init(); a real wire arriving later via
                # /nodes/connect updates self.inputs['in'] but never
                # touches that already-captured reference, so this loop
                # was reading from a permanently stale/disconnected pipe
                # forever whenever wiring happened after start() instead
                # of before it (the Engine's own full-workflow-run path
                # always wires before start(), which is why this never
                # surfaced there). Re-resolving from self.inputs every
                # iteration picks up a late-arriving real wire within one
                # poll interval instead of never.
                pipe = self.inputs.get('in', self.in_pipe)
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                # put into queue for streaming
                try:
                    self._queue.put_nowait(item)
                except asyncio.QueueFull:
                    try:
                        self._queue.get_nowait()
                        self._queue.put_nowait(item)
                    except Exception:
                        pass
                # also emit for compatibility
                self.emit('out', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `path` and `sse`
        both feed the route this node registers once in init() - see the
        identical fix in output_api.py.
        """
        if not ({'path', 'sse'} & changed_keys):
            return
        self.path = self.config.get('path', f'/out/{self.id}')
        self.sse_enabled = bool(self.config.get('sse', True))
        self._register_route()

    async def stop(self):
        await super().stop()
        self._running_stream = False
