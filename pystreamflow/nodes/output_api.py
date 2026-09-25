from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node, register_route, start_server
import asyncio
import json
from fastapi.responses import StreamingResponse


class ApiOutputNode(BaseNode):
    """A REST-style output node: exposes the latest/streamed value of
    whatever flows into its ``in`` port under the shared web server's
    ``/api`` prefix.

    Modeled on ``WebOutputJSONNode`` (same shared ``core/web_server.py``
    app, same internal queue + SSE/latest-value route pattern), with two
    differences:

    1. It calls ``start_server()`` itself from ``process()``, the same
       way ``WebInputNode``/``ApiInputNode`` do. ``WebOutputNode`` and
       ``WebOutputJSONNode`` never do this - they only work in a workflow
       that happens to *also* contain some other node (typically a
       ``WebInputNode``) that started the shared server first, which is a
       real, previously-unfixed gap for an output-only API workflow.
       ``start_server()`` is idempotent, so this is safe even when an
       ``ApiInputNode``/``WebInputNode`` in the same graph also calls it.
    2. Alongside the main JSON-encapsulated HTTP route (``/api/<uri>``,
       matching how ``ApiInputNode`` reads its request bodies as JSON),
       it registers a second "raw" HTTP route (``/api/<uri>/raw``) that
       serves the exact same data unencapsulated - the plain value as
       text, not JSON-encoded - for external consumers that want the raw
       payload without unwrapping a JSON envelope first.

    Both HTTP routes serve the same underlying stream of items (the one
    queue fed by ``process()``); they differ only in how each item is
    encoded on the wire when it's written to the HTTP response.

    Separately - and distinctly from those two HTTP routes - this node
    also exposes a real graph-level ``raw`` *output port*, alongside its
    ``out`` port, so the unencapsulated stream is wireable to other nodes
    directly on the canvas, not just reachable over HTTP. (Feedback: an
    earlier pass added only the ``/api/<uri>/raw`` HTTP route and called
    that "raw output edge points" - but in this codebase "edge point"
    specifically means a real litegraph.js port, not an HTTP endpoint, so
    the UI correctly showed nothing new. ``out`` and ``raw`` currently
    carry identical items, since neither this node's ``out`` port nor its
    HTTP JSON route mutates the item before this point - JSON-encoding
    only happens when a response body is serialized in
    ``_register_routes()`` below, never on the graph/pipe level - but they
    are still kept as two distinct, separately-wireable ports so a
    downstream consumer can depend on "the unencapsulated tap" (``raw``)
    by name regardless of what ``out`` carries in the future.)
    """

    async def init(self):
        # Bug fix: this used to default to '127.0.0.1', unreachable
        # through the Docker-published port for this node's shared web
        # server - see ApiInputNode.init()'s comment for the full
        # explanation and how this was confirmed.
        self.host = self.config.get('host', '0.0.0.0')
        self.port = self.config.get('port', 8080)
        self.node_id = self.id or f"api_out_{id(self)}"
        # Falls back to the node's own id on an empty string too, not
        # just a missing key - see the identical note in input_api.py -
        # since the editor seeds `uri` as a visible '' text field.
        self.uri = str(self.config.get('uri') or self.node_id).strip('/')
        self.path = f"/api/{self.uri}"
        self.raw_path = f"{self.path}/raw"
        self.sse_enabled = bool(self.config.get('sse', True))
        # See the identical note in web_output.py/web_output_json.py:
        # add_input_if_unwired(), not add_input(), so a real upstream
        # wiring set up by Engine._wire_edges() before this init() runs
        # isn't silently replaced by a disconnected internal Pipe().
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        # Graph-level counterpart to the /api/<uri>/raw HTTP route - a
        # real, wireable output port (see the class docstring above).
        self.raw_pipe = self.add_output_if_unwired('raw', Pipe())
        register_node(self)
        self._queue = asyncio.Queue(maxsize=1000)
        self._register_routes()

    @staticmethod
    def _raw_text(item):
        if isinstance(item, dict) and 'item' in item:
            item = item['item']
        return item if isinstance(item, str) else str(item)

    def _register_routes(self):
        route_name = f"apiout_{self.id}"
        raw_route_name = f"apiout_raw_{self.id}"

        if self.sse_enabled:
            async def json_stream_generator():
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

            async def json_handler():
                return StreamingResponse(json_stream_generator(), media_type="text/event-stream")

            register_route("get", self.path, route_name, json_handler)

            async def raw_stream_generator():
                for item in self.get_last(20):
                    yield f"data: {self._raw_text(item)}\n\n"
                while True:
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                        yield f"data: {self._raw_text(item)}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"

            async def raw_handler():
                return StreamingResponse(raw_stream_generator(), media_type="text/event-stream")

            register_route("get", self.raw_path, raw_route_name, raw_handler)
        else:
            from fastapi.responses import JSONResponse, PlainTextResponse

            async def json_handler():
                last = self.get_last(1)
                item = last[-1].get('item', last[-1]) if last and isinstance(last[-1], dict) else (last[-1] if last else None)
                return JSONResponse(content={"latest": item})

            register_route("get", self.path, route_name, json_handler)

            async def raw_handler():
                last = self.get_last(1)
                payload = self._raw_text(last[-1]) if last else ''
                return PlainTextResponse(payload)

            register_route("get", self.raw_path, raw_route_name, raw_handler)

    async def process(self):
        await start_server(self.host, self.port)
        while self._running:
            try:
                # Re-resolved every iteration, not a bare self.in_pipe -
                # see the identical fix + comment in web_output.py's
                # process(): a real wire arriving after start() (e.g. via
                # POST /nodes/connect against an already auto-started
                # node) updates self.inputs['in'] but never the reference
                # captured once in init(), so reading that dynamically is
                # what lets this node actually pick up a live rewire.
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
                # Phase 2 of the wire-kind-unification design (see
                # claude/design_unified_wire_kinds_plan.md) retired
                # BaseNode.emit()'s generic auto-replay onto a 'raw' port -
                # "raw" is now a per-wire delivery *kind* chosen on a
                # node's one real output port, not a second port every
                # node gets for free. This class's own 'raw' port predates
                # that generalization and is still a real, separately
                # hand-declared output port (see get_port_schema()'s
                # ApiOutputNode override) - it doesn't disappear just
                # because the generic mechanism it inspired did, so it
                # still needs its own explicit emit() here to keep
                # carrying the same unencapsulated item as 'out'.
                self.emit('out', item)
                self.emit('raw', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `uri` and `sse`
        both feed the routes this node registers once in init() - `uri`
        picks the path itself, `sse` picks which of two entirely
        different handler pairs (streaming vs latest-value) gets bound to
        it. Re-deriving both and calling _register_routes() again (its
        two route names are stable/id-derived, so register_route()
        safely replaces whichever handlers were there before, even
        across an sse_enabled flip) is what makes editing either field on
        a live node actually take effect.
        """
        if not ({'uri', 'sse'} & changed_keys):
            return
        self.uri = str(self.config.get('uri') or self.node_id).strip('/')
        self.path = f"/api/{self.uri}"
        self.raw_path = f"{self.path}/raw"
        self.sse_enabled = bool(self.config.get('sse', True))
        self._register_routes()

    async def stop(self):
        await super().stop()
