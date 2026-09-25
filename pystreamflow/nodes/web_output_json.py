from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node, register_route
import asyncio
import json
from fastapi.responses import StreamingResponse

class WebOutputJSONNode(BaseNode):
    async def init(self):
        # Bug fix: this used to default to '127.0.0.1', unreachable
        # through the Docker-published port for this node's shared web
        # server - see ApiInputNode.init()'s comment for the full
        # explanation and how this was confirmed.
        self.host = self.config.get('host', '0.0.0.0')
        self.port = self.config.get('port', 8080)
        self.path = self.config.get('path', f'/out_json/{self.id}')
        self.sse_enabled = bool(self.config.get('sse', True))
        # See the identical note in web_output.py: add_input_if_unwired(),
        # not add_input(), so a real upstream wiring set up by
        # Engine._wire_edges() before this init() runs isn't silently
        # replaced by a disconnected internal Pipe() that nothing upstream
        # ever writes to.
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        register_node(self)
        self._queue = asyncio.Queue(maxsize=1000)
        self._register_route()

    def _register_route(self):
        if not self.path.startswith('/'):
            self.path = '/' + self.path
        route_name = f"weboutjson_{self.id}"

        if self.sse_enabled:
            async def stream_generator():
                for item in self.get_last(20):
                    try:
                        payload = json.dumps(item)
                    except Exception:
                        payload = json.dumps({'data': str(item)})
                    yield f"data: {payload}\n\n"
                while True:
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                        try:
                            payload = json.dumps(item)
                        except Exception:
                            payload = json.dumps({'data': str(item)})
                        yield f"data: {payload}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"

            async def handler():
                return StreamingResponse(stream_generator(), media_type="text/event-stream")

            register_route("get", self.path, route_name, handler)
        else:
            from fastapi.responses import JSONResponse

            async def handler():
                last = self.get_last(1)
                if last:
                    item = last[-1].get('item', last[-1]) if isinstance(last[-1], dict) else last[-1]
                else:
                    item = None
                return JSONResponse(content={"latest": item})

            register_route("get", self.path, route_name, handler)

    async def process(self):
        while self._running:
            try:
                # See the identical fix + comment in web_output.py's
                # process(): re-resolve from self.inputs every iteration
                # (instead of a bare self.in_pipe.get(), which is captured
                # once in init() and never updated) so a real wire arriving
                # after start() via /nodes/connect isn't read from a
                # permanently stale/disconnected pipe forever.
                pipe = self.inputs.get('in', self.in_pipe)
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                try:
                    self._queue.put_nowait(item)
                except asyncio.QueueFull:
                    try:
                        self._queue.get_nowait()
                        self._queue.put_nowait(item)
                    except Exception:
                        pass
                self.emit('out', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `path` and `sse`
        both feed the route this node registers once in init() - see the
        identical fix in output_api.py/web_output.py.
        """
        if not ({'path', 'sse'} & changed_keys):
            return
        self.path = self.config.get('path', f'/out_json/{self.id}')
        self.sse_enabled = bool(self.config.get('sse', True))
        self._register_route()

    async def stop(self):
        await super().stop()
