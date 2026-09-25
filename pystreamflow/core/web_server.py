import logging

from fastapi import FastAPI
import uvicorn
import asyncio

app = FastAPI(title="PyStreamFlow Web Inputs")
_running = False
_server_task = None
# The live uvicorn.Server instance created by the most recent
# start_server() call. Previously nothing outside start_server()'s own
# local scope ever held a reference to this - only `_server_task` (the
# asyncio.Task wrapping server.serve()) was kept, which is enough to
# cancel the server but not enough to ask it anything, such as "what TCP
# port did you actually bind to". That made `port=0` (ask the OS for any
# free ephemeral port) unusable for callers/tests that need to connect to
# the real socket afterward - see get_bound_port() below, added to
# support a genuine real-socket verification of WebInputNode/ApiInputNode
# rather than only in-process TestClient/direct-emit() checks.
_server: "uvicorn.Server | None" = None
# The (host, port) actually passed to the currently-running start_server()
# call, if any - see start_server()'s host/port-mismatch warning below.
_bound_host_port: "tuple[str, int] | None" = None
_nodes: dict[str, object] = {}
# Per-session view of the same node objects held in `_nodes` above, keyed
# session_id -> {node_id: node}. See register_node()'s docstring for why
# this exists alongside the flat `_nodes` dict rather than replacing it.
_session_nodes: dict[str, dict[str, object]] = {}

logger = logging.getLogger("pystreamflow.web_server")

@app.get("/health")
def health():
    return {"status": "ok"}

def register_node(node):
    """Register a node instance for the flat `_nodes` registry that every
    node-by-id endpoint (`/nodes/*`, `/reflection/*`, the equivalent MCP
    tools) and every node type that self-registers from its own init()
    (ScriptInputNode, WebOutputNode, WebOutputJSONNode, LMStudioNode,
    WebInputNode, UserInputNode) reads from - plus, when the node belongs
    to a Session (``node.session_id`` is set - see Engine._instantiate_nodes()
    and BaseNode.session_id), a session-scoped copy in `_session_nodes`
    that ``get_session_nodes()``/``unregister_session()`` below use.

    Bug this closes (found while verifying multi-session behavior):
    `_nodes` is a single process-wide dict keyed only by the workflow's
    own node id (e.g. "gen", "log") with no session namespacing at all.
    Two Sessions running workflows that happen to share a node id - the
    ordinary case for running the *same* workflow file twice, or two
    workflows authored by copy-paste - used to silently overwrite each
    other's entry here: whichever session's node instantiated most
    recently "won" the bare id, and the other session's node became
    completely invisible to every id-based endpoint/tool, with no error
    or warning anywhere. The actual data processing was never affected
    (each Session/Engine owns entirely separate node/pipe objects - see
    Engine.__init__ and Session.load()), only introspection/control by
    node id. This is now at least made visible via a warning (silent
    collisions are worse than logged ones) and no longer permanently
    leaks: `unregister_session()` cleans a session's nodes back out when
    it's deleted, and `/sessions/{id}/nodes[/​{node_id}]` (see
    api/server.py) give an unambiguous way to reach a specific session's
    node even while its id collides with another session's in `_nodes`.
    """
    node_id = node.id
    session_id = getattr(node, 'session_id', None)
    existing = _nodes.get(node_id)
    if existing is not None and existing is not node:
        existing_session = getattr(existing, 'session_id', None)
        if existing_session != session_id:
            logger.warning(
                "node id %r registered by session %r overwrites a node "
                "previously registered by session %r in the global node "
                "registry - /nodes, /reflection/* and MCP node tools will "
                "only see the one registered most recently; use "
                "/sessions/<id>/nodes/%s to reach a specific session's "
                "copy unambiguously",
                node_id, session_id, existing_session, node_id,
            )
        # Leak fix found while verifying the node editor's "starting a
        # node while running is not working" fix (see createPsfNode()'s
        # comment in editor.js): a node dropped on the canvas now gets a
        # real ad-hoc backend instance (session_id=None) immediately, and
        # pressing "Run" afterward instantiates a *second*, session-owned
        # instance under the same id - which lands here as exactly this
        # collision. The old docstring claim that this "no longer
        # permanently leaks" was only ever true for a session-vs-session
        # collision (unregister_session() cleans those up when a session
        # is deleted) - an ad-hoc, non-session instance superseded this
        # way was never owned by any session's cleanup path, so it just
        # kept running forever: still holding whatever it started (an
        # ApiInputNode/WebInputNode's HTTP route registration, a
        # subprocess, an open socket, ...), invisible to every id-based
        # endpoint the instant it's overwritten here, with nothing left
        # anywhere that could ever stop it. Stopping the superseded
        # instance here - not just logging its disappearance - closes
        # that leak generically, for every cause of this collision, not
        # only the specific one that surfaced it.
        stop = getattr(existing, 'stop', None)
        if callable(stop) and getattr(existing, '_running', False):
            try:
                asyncio.get_event_loop().create_task(stop())
            except RuntimeError:
                logger.warning(
                    "node id %r: could not schedule cleanup stop() for "
                    "the superseded instance (no running event loop)",
                    node_id,
                )
    _nodes[node_id] = node
    if session_id is not None:
        _session_nodes.setdefault(session_id, {})[node_id] = node

def get_session_nodes(session_id: str) -> dict[str, object]:
    """Return {node_id: node} for exactly the nodes owned by `session_id`,
    unaffected by any bare-id collision with another session or the
    ad-hoc (no-session) node editor's own nodes in `_nodes`."""
    return dict(_session_nodes.get(session_id, {}))

def unregister_session(session_id: str) -> None:
    """Remove every node owned by `session_id` from both `_session_nodes`
    and (only where it hasn't already been overwritten by a *different*
    session/ad-hoc node reusing the same id - checked via identity, not
    just id, so this can never delete something it doesn't own) `_nodes`.
    Call this when a session is deleted so its nodes don't linger in the
    global registry forever (they used to - nothing ever removed them)."""
    nodes = _session_nodes.pop(session_id, {})
    for node_id, node in nodes.items():
        if _nodes.get(node_id) is node:
            del _nodes[node_id]

def register_route(method: str, path: str, name: str, endpoint) -> None:
    """Register (or re-register) a single HTTP route on the shared web
    server app, first removing any existing route already registered
    under this exact `name`.

    Bug this closes (found while investigating: "workflow is running, web
    input node is started, message was not received and nothing shown in
    the UI"): every node type that exposes its own HTTP endpoint here
    (WebInputNode/ApiInputNode via register_input() below, ApiOutputNode,
    UserInputNode, WebOutputNode, WebOutputJSONNode) used to call
    app.get()/app.post() directly from its own init(), with a name/path
    derived from its (stable, workflow-authored) node id. Starlette's
    router never deduplicates by name or path - every call just appends a
    brand new Route object, and the router matches routes in registration
    order, first match wins. So every time a session restarts (stop then
    start, delete then recreate, or simply running the same workflow a
    second time) - which instantiates a brand new node object under the
    *same* id - the OLD node's route stayed first in the list and kept
    matching every future request forever: silently forwarding to a dead
    node instance from a session that no longer exists (its emit()
    "succeeding" into a pipe nobody drains anymore), while the new,
    currently-running node's own route sat permanently unreachable behind
    it. HTTP callers saw a completely normal 200 response the whole
    time - no error anywhere, no 404, no exception - the message just
    vanished. Filtering out any existing route registered under this
    exact `name` before adding the new one is what actually closes this,
    generically, for every node type that routes through here instead of
    calling app.get()/app.post() directly.
    """
    app.router.routes[:] = [
        r for r in app.router.routes if getattr(r, "name", None) != name
    ]
    getattr(app, method)(path, name=name)(endpoint)

def register_input(node_id: str, path: str, node):
    """Register ``POST <path>`` for an input node (WebInputNode,
    ApiInputNode). A JSON object body is emitted as-is, exactly as before;
    media plan phase 2 adds file uploads - ``multipart/form-data`` and raw
    ``application/octet-stream``/``image/*``/``audio/*``/``video/*``
    bodies - which are emitted as one ``MediaItem`` per file (see
    ``core/media_http.py``). Uploads are limited to the node's
    ``max_upload_mb`` config (default ``PSF_MAX_UPLOAD_MB``, 100)."""
    from fastapi import Request
    from fastapi.responses import JSONResponse

    from .media_http import BadRequest, UploadTooLarge, error_response, node_upload_limit, parse_request

    if not path.startswith("/"):
        path = "/" + path

    async def handler(request: Request):
        if not getattr(node, "_running", True):
            return JSONResponse(
                status_code=503,
                content={
                    "status": "stopped",
                    "node": node_id,
                    "error": "node is stopped and not accepting input",
                },
            )
        try:
            kind, value = await parse_request(request, node_upload_limit(node))
        except (UploadTooLarge, BadRequest) as e:
            return error_response(e)
        if kind == "media":
            for item in value:
                node.emit('out', item)
            return {"status": "ok", "node": node_id, "media": [item.summary() for item in value]}
        if not isinstance(value, dict):
            return JSONResponse(
                status_code=422,
                content={"error": "expected a JSON object body, or a file upload"},
            )
        node.emit('out', value)
        return {"status": "ok", "node": node_id}

    route_name = f"input_{node_id}"
    register_route("post", path, route_name, handler)
    return path

@app.get("/nodes")
def list_nodes():
    return {nid: type(node).__name__ for nid, node in _nodes.items()}

@app.get("/nodes/{node_id}/last")
def node_last(node_id: str, n: int = 50):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    from .media import to_jsonable

    last = node.get_last(n) if hasattr(node, "get_last") else []
    return to_jsonable({"node": node_id, "last": last})

def http_endpoint_info(node) -> "dict | None":
    """Derived HTTP-reachability info for a node's config/reflection
    output, for the node types whose whole job is exposing a real
    inbound/outbound HTTP route on this shared web server (WebInputNode,
    ApiInputNode, WebOutputNode, WebOutputJSONNode, ApiOutputNode) -
    `None` for every other node type. Shared by both `api/server.py`'s
    and `mcp/server.py`'s own config/reflection endpoints/tools, so a
    caller gets the same answer whichever surface it asks through.

    Bug fix (reported: an MCP client tried a real HTTP POST against an
    ApiInputNode before falling back to the `send_to_node` tool, and had
    no way to succeed - the config/reflection responses on every surface
    only ever returned the node's raw config, e.g. just
    `{"uri": "demo/in"}` for an ApiInputNode. Nothing surfaced that the
    real route lives under an `/api/` prefix (`/api/demo/in`, not
    `/demo/in`), let alone what port it's actually served on. This is
    exactly the kind of default/derived value covered elsewhere in this
    codebase by a node's own `self.path`/`self.host`/`self.port` (set in
    `init()`, not just echoing raw config) - nothing surfaced any of it.

    This deliberately does NOT try to guess this daemon's own externally
    -visible hostname - it can't know that in general (a Docker host's
    real LAN name, a reverse proxy's hostname, `localhost` for a local
    run are all equally plausible and this process has no way to tell
    which one a given caller will need), and guessing wrong would be
    worse than not answering, given how much of this project's own bug
    history is exactly a component confidently assuming a hostname that
    turned out to be wrong for the caller's actual situation. Instead,
    `note` points the caller at the one thing that's always true: this
    node's shared web server is the same machine/container serving
    whatever connection the caller is already using to ask this
    question, just a different port - so whatever host reaches that is
    the right host to substitute here too.

    Node-class imports are deferred to call time (not module load time)
    specifically to avoid a circular import: every node type this
    function checks against imports register_node()/start_server() etc.
    from this same module, so importing any of them back at this
    module's own top level would be circular.
    """
    from ..nodes.input_api import ApiInputNode
    from ..nodes.input_web import WebInputNode
    from ..nodes.output_api import ApiOutputNode
    from ..nodes.web_output import WebOutputNode
    from ..nodes.web_output_json import WebOutputJSONNode

    if not isinstance(node, (ApiInputNode, WebInputNode, ApiOutputNode, WebOutputNode, WebOutputJSONNode)):
        return None
    method = "GET" if isinstance(node, (ApiOutputNode, WebOutputNode, WebOutputJSONNode)) else "POST"
    info = {
        "method": method,
        "path": node.path,
        "port": node.port,
        "note": (
            f"Reachable at http://<host>:{node.port}{node.path} - substitute "
            "for <host> whatever hostname/IP you used to reach this daemon "
            "itself (it can't know its own externally visible hostname); "
            "this is the node's own shared web server, a different port "
            "from the main daemon/MCP port."
        ),
    }
    if hasattr(node, "raw_path"):
        info["raw_path"] = node.raw_path
    if hasattr(node, "media_path"):
        info["media_path"] = node.media_path
    if hasattr(node, "sse_enabled"):
        info["sse"] = node.sse_enabled
    return info

async def start_server(host="0.0.0.0", port=8080):
    """Start the one shared HTTP server every WebInputNode/ApiInputNode/
    ApiOutputNode/UserInputNode/WebOutputNode/WebOutputJSONNode registers
    its routes on and asks to be listening, via this same function, from
    its own process().

    This server is a **process-wide singleton bound to a single host:port
    pair** - whichever node's process() coroutine happens to call this
    first "wins" that pair for the lifetime of the process (until
    stop_server() is called); the `if _running: return` guard makes every
    later call from a *different* node a no-op, silently, even when that
    node asked for a different host/port than what's already running.

    Bug found while investigating a report of "web input node is
    started... message was not received and nothing is shown in the UI":
    a node whose own init()/register_input() call succeeded (its route
    really is registered on `app`) can still never actually receive a
    single request, with zero error or warning anywhere, if some *other*
    node in the same workflow already won the singleton on a different
    port before this one's process() ran - every route this node
    registered simply never gets served, because nothing is listening on
    the port this node was configured for at all. This isn't fixed here
    (genuinely serving N independent host:port pairs from N independent
    uvicorn.Server instances is a real architecture change, out of scope
    for this pass), but it's no longer silent: a mismatched request now
    logs a clear warning naming both the port that was requested and the
    one actually in use, so this failure mode is at least diagnosable
    instead of a mystery.
    """
    global _running, _server_task, _server, _bound_host_port
    if _running:
        if (host, port) != _bound_host_port:
            logger.warning(
                "start_server(host=%r, port=%r) requested, but the shared "
                "web server is already running on host=%r, port=%r (from "
                "an earlier node's start_server() call) - this server is "
                "a process-wide singleton bound to one host:port pair, so "
                "any route registered by a node expecting host=%r, "
                "port=%r will never actually be served; nothing is "
                "listening there. Use the same host/port across every "
                "node in a workflow that relies on this shared server.",
                host, port, *(_bound_host_port or (None, None)), host, port,
            )
        return
    _running = True
    _bound_host_port = (host, port)
    config = uvicorn.Config(app, host=host, port=port, log_level="error")
    server = uvicorn.Server(config)
    _server = server
    _server_task = asyncio.create_task(server.serve())

async def get_bound_port(timeout: float = 5.0) -> int | None:
    """Return the real TCP port the running server is listening on.

    Needed because `port=0` (ask the OS to pick any free ephemeral port,
    the standard way to avoid hardcoding/colliding on a fixed port in
    tests) otherwise leaves no way to discover which port was actually
    bound - `start_server()`'s `port` argument is just the *requested*
    port, and uvicorn only knows the real one once the server's asyncio
    socket has actually been opened.

    Waits for `server.started` (uvicorn sets this after its listening
    sockets are bound, before it starts accepting the serve() loop
    forever) rather than assuming any fixed delay, so this is not
    flaky/racy under load. Returns None if no server was ever started, or
    if it doesn't finish starting within `timeout` seconds.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    # Wait for _server itself to exist first, not just for it to finish
    # starting: a node's process() typically calls start_server() from a
    # freshly-scheduled asyncio task (e.g. BaseNode.start()), so at the
    # moment a caller awaits get_bound_port() right after starting a node,
    # that task may not have run yet at all and `_server` can still be
    # None. Only bailing out here on an already-None `_server` (rather
    # than polling for it too) was the bug in an earlier version of this
    # function: it made get_bound_port() spuriously return None whenever
    # it won the race against the node's own startup.
    while _server is None:
        if asyncio.get_event_loop().time() >= deadline:
            return None
        await asyncio.sleep(0.01)
    server = _server
    while not getattr(server, "started", False):
        if asyncio.get_event_loop().time() >= deadline:
            return None
        await asyncio.sleep(0.01)
    try:
        return server.servers[0].sockets[0].getsockname()[1]
    except (IndexError, AttributeError, OSError):
        return None

async def stop_server():
    global _running, _server_task, _server, _bound_host_port
    # Regression fix: this used to only flip the `_running` guard flag and
    # leave the actual uvicorn Server task running forever - `start_server`
    # schedules `server.serve()` as a background task and never keeps a
    # reference to the `Server` object itself outside that closure, so
    # there was no way to ask it to shut down; calling stop_server() then
    # start_server() again would try to bind the same port a second time
    # while the original server was still very much alive underneath.
    # Cancelling the tracked task actually tears the server down.
    _running = False
    if _server_task is not None:
        _server_task.cancel()
        try:
            await _server_task
        except asyncio.CancelledError:
            pass
        _server_task = None
    _server = None
    _bound_host_port = None
