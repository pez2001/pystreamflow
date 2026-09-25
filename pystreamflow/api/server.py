import asyncio
import logging
import pathlib
import re
from contextlib import asynccontextmanager

import yaml
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.routing import get_route_path

from ..core.blob_store import ensure_cleanup_task, get_blob_store, stop_cleanup_task
from ..core.media import sniff_mime, to_jsonable
from ..core.models import Edge, Graph, Node
from ..core.persistence import load_workflow, save_workflow
from ..core.plugin_manager import PluginManager
from ..core.session_manager import Session, session_manager
from ..core.auth import check_api_key, get_or_create_api_key
from ..core.web_server import _nodes, http_endpoint_info
from ..core.web_server import app as web_server_app
from ..mcp.server import app as mcp_app
from ..mcp.server import mcp_lifespan


@asynccontextmanager
async def _lifespan(_app):
    # Starlette/FastAPI don't cascade the "lifespan" ASGI event down into
    # a mounted sub-application automatically - mounting mcp_app below
    # (app.mount("/mcp", mcp_app)) does NOT, by itself, start the real MCP
    # Streamable HTTP transport's session manager, which needs its own
    # background task group running for the app's whole lifetime. Without
    # this, the very first real MCP request against a daemon started
    # through this module (i.e. any real deployment - mcp/server.py's own
    # `if __name__ == "__main__":` block is only for running that module
    # standalone) would fail with "RuntimeError: Task group is not
    # initialized. Make sure to use run()." - reproduced and confirmed
    # while building this fix. mcp_lifespan (defined once in mcp/server.py
    # and imported here) is exactly what mcp_app's own standalone
    # `uvicorn.run(app, ...)` path relies on too, so both ways of running
    # this server enter the identical lifespan.
    async with mcp_lifespan(mcp_app):
        # Periodic expiry/eviction of media blobs (media plan phase 1.2) -
        # also needed for ad-hoc editor nodes that never run through an
        # Engine, which would otherwise be the only thing starting it.
        ensure_cleanup_task()
        try:
            yield
        finally:
            await stop_cleanup_task()


app = FastAPI(lifespan=_lifespan)

# Ensure an API key exists - and gets logged - the moment this module
# loads, rather than lazily on whatever request happens to be first. An
# operator starting the daemon needs to see a freshly-generated key in
# the startup log immediately, not discover later (after some client
# already got a confusing 401) that one was silently created.
get_or_create_api_key()

logger = logging.getLogger("pystreamflow.api")


# Real bug, reported live as "Failed to run workflow: Unexpected token 'I',
# "Internal S"... is not valid JSON" - the node editor's fetchJSON() (see
# editor.js) always calls res.json() on every response with no regard for
# its status/content-type, and several routes below (POST /sessions in
# particular - it awaits Session.load() -> Engine.validate(), which raises
# a plain ValueError for a cyclic or otherwise structurally invalid graph,
# with no try/except anywhere between that raise and this app's default
# exception handling) had no route-level handling for an unexpected
# exception at all. FastAPI/Starlette's own default behavior for an
# unhandled exception is a bare `text/plain` "Internal Server Error" body -
# not JSON - so res.json() throws a SyntaxError on the *client*, which is
# exactly the confusing, contentless message that surfaced: the real
# server-side error (e.g. "Graph contains cycle") never reached the user
# at all, only a JSON-parse failure one further step removed from it.
#
# Fixed at the root, for every route in this app at once, rather than
# patch-adding a try/except to each handler one at a time (this file has
# dozens, and the next one added would just reintroduce the same gap):
# a global handler that turns *any* unhandled exception into the same
# `{"error": "..."}` JSON shape this file's own routes already return for
# every other error condition (e.g. "node not found"), so the frontend's
# existing `if (data.error) ...` handling covers this case too, and the
# real underlying message (not just "Internal Server Error") reaches the
# person who needs to act on it. Registered for bare `Exception`, not
# Starlette's `HTTPException` - FastAPI already handles that one via its
# own default handler (which runs first for it), so a deliberate
# `raise HTTPException(404, ...)` elsewhere keeps its real status code;
# only a genuinely unexpected exception falls through to this.
@app.exception_handler(Exception)
async def _unhandled_exception_to_json(request: Request, exc: Exception):
    logger.exception("unhandled exception in %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})

# Static assets for the node editor UI (pystreamflow/api/ui.html): the
# vendored litegraph.js library/CSS and our own editor.js. Mounted before
# any other route registration below matters only relative to other
# mounts (there's just the one other, /mcp, on a disjoint prefix), since
# Starlette matches a Mount by prefix rather than registration order among
# plain @app.get routes.
_static_dir = pathlib.Path(__file__).parent / 'static'
if _static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

# Shared API-key authentication (task: "add authentication"), covering
# this app's whole HTTP surface - including the editor UI's own fetch()
# calls - on by default: unlike /mcp/*'s pre-existing, deliberately
# opt-in PSF_MCP_API_KEY (see mcp/server.py and core/auth.py's module
# docstring for why that one stays opt-in), leaving PSF_API_KEY unset
# here does NOT mean "no auth" - core/auth.get_or_create_api_key() will
# have already generated and logged a random one at import time above.
#
# Exempt from the check, deliberately narrow:
#  - "/", "/ui": the editor page's own HTML shell - it has to load
#    *before* a user has anywhere to type the key in.
#  - "/static/*": the JS/CSS/vendor assets that page needs to render
#    (including the login prompt itself, added to editor.js below).
#  - "/health", "/version": routine liveness/version checks - no
#    sensitive data, and requiring a key here would break the exact
#    kind of external uptime monitoring these exist for.
#  - "/mcp" and "/mcp/*": a completely separate sub-application with its
#    own require_auth()/PSF_MCP_API_KEY - left untouched by this change.
#  - "/api" and "/api/*": NOT this app's own routes at all - these are
#    endpoints a *workflow's* ApiInputNode/ApiOutputNode register at
#    runtime (see the app.mount("/", web_server_app) comment far below)
#    specifically so external systems can call into a running graph
#    without any relationship to this admin/editor surface, the same
#    deliberately-standalone design as WebInputNode/WebOutputNode's own
#    private server. Gating those behind this app's admin key would
#    silently break every workflow using them for exactly the inbound-
#    webhook use case they exist for.
# Everything else - /nodes, /sessions, /parse_yaml, /connect, /docs,
# /showcase, /metrics, etc - requires the key.
_AUTH_EXEMPT_PATHS = {"/", "/ui", "/health", "/version"}
_AUTH_EXEMPT_PREFIXES = ("/static/", "/mcp/", "/api/")


def _is_auth_exempt(path: str) -> bool:
    if path in _AUTH_EXEMPT_PATHS or path in ("/mcp", "/api"):
        return True
    return path.startswith(_AUTH_EXEMPT_PREFIXES)


# Bug found while chasing an *unrelated* report ("LM Studio: SSE error:
# Non-200 status code (404)"): once that 404 was fixed and /mcp/sse was
# actually reachable, live testing against a daemon started through THIS
# module (i.e. any real deployment - see the _lifespan comment above)
# turned up a second, previously-invisible failure - every real SSE
# session crashed a moment after connecting:
#
#   AssertionError: Unexpected message: {'type': 'http.response.start', ...}
#   (raised from starlette/middleware/base.py's body_stream())
#
# It never showed up testing mcp/server.py's `app` standalone (python -m
# pystreamflow.mcp.server) - only when mounted under this module's `app`.
# The one structural difference is this very middleware. `@app.middleware
# ("http")` is sugar for starlette.middleware.base.BaseHTTPMiddleware,
# which - regardless of whether a given path is exempted from the auth
# *check* below - still routes every request's response through its
# call_next() relay: an in-process anyio memory stream that forwards ASGI
# messages between the wrapped app and the real client. That relay is
# built around a single "http.response.start" followed by a finite run of
# "http.response.body" chunks; a long-lived SSE stream (open-ended body,
# and the MCP SDK's SSE transport has its own internal response
# lifecycle) doesn't fit that shape, which is what the assertion above is
# tripping on. This is a well-known, long-standing Starlette/FastAPI
# limitation (BaseHTTPMiddleware is not safe to combine with streaming
# responses), not something specific to this project's auth logic - but
# unless every middleware in the stack is pure ASGI, mounting a real SSE
# endpoint anywhere underneath it will hit this.
#
# Fixed by rewriting this middleware as a plain ASGI middleware class
# (add_middleware(_ApiKeyMiddleware) below) instead of the request/
# call_next decorator form - mirroring mcp/server.py's own
# _MCPAuthMiddleware, which never had this problem for exactly this
# reason. A pure ASGI middleware that simply calls `await self._app(scope,
# receive, send)` for the "let it through" case never intercepts or
# relays the response at all, so a streaming response passing through it
# is untouched. Confirmed live: the same client that used to crash
# (mcp.client.sse.sse_client against /mcp/sse through a daemon started via
# this module) now completes initialize -> tools/list -> tools/call
# without error.
class _ApiKeyMiddleware:
    def __init__(self, app):
        self._app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or _is_auth_exempt(get_route_path(scope)):
            await self._app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        authorization = headers.get(b"authorization", b"").decode("latin-1") or None
        x_api_key = headers.get(b"x-api-key", b"").decode("latin-1") or None
        if not check_api_key(authorization, x_api_key):
            response = JSONResponse(
                status_code=401,
                content={"error": "Missing or invalid API key. Provide it via an "
                                   "'Authorization: Bearer <key>' or 'X-API-Key' header."},
            )
            await response(scope, receive, send)
            return
        await self._app(scope, receive, send)


app.add_middleware(_ApiKeyMiddleware)


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    try:
        from importlib.metadata import version as get_version
        v = get_version("pystreamflow")
    except Exception:
        import pystreamflow
        v = getattr(pystreamflow, "__version__", "unknown")
    return {"name": "pystreamflow", "version": v}

# Real bug found this pass, reported as "the UI isn't showing changes I
# was told were made" (specifically: newly-added raw-output ports that
# verified correctly in a fresh browser profile): StaticFiles (mounted
# above) serves /static/editor.js with no Cache-Control header at all, and
# ui.html references it as a bare `/static/editor.js` with no version/hash
# in the URL. Most browsers apply *heuristic* caching to a same-URL static
# response with no explicit cache directive (roughly proportional to the
# file's age since Last-Modified), so a browser that already had an older
# editor.js cached can keep serving those stale bytes for a long time after
# the file on disk changes - a full reload, even, isn't guaranteed to
# re-fetch it. Every prior update to editor.js in this project could have
# been silently invisible to a browser that had loaded the UI even once
# before. Fixed by stamping the script URL with a short hash of the file's
# *current* content, computed fresh on every request: the URL itself
# changes whenever the file's bytes do, so a stale cached copy can never
# satisfy it - normal caching still applies between edits, since the URL
# (and therefore the cache key) stays the same until the content does.
def _versioned_static_html(html_text: str) -> str:
    import hashlib
    js_path = _static_dir / 'editor.js'
    try:
        digest = hashlib.sha256(js_path.read_bytes()).hexdigest()[:10]
    except OSError:
        return html_text
    return html_text.replace(
        'src="/static/editor.js"', f'src="/static/editor.js?v={digest}"'
    )

# Belt-and-suspenders alongside _versioned_static_html() above: that fix
# makes a *changed* editor.js impossible to keep serving stale (the URL
# itself changes), but it still depends on the browser actually asking
# this server for a fresh copy of the *HTML page* that contains that URL.
# Reported again after the first fix shipped ("raw output still not
# showing in ui") - so rather than assume the fix didn't work, this closes
# the one gap it didn't cover: nothing here ever told the browser it
# couldn't cache `/`/`/ui` themselves. Neither had a Cache-Control header
# before, which usually means "don't apply heuristic freshness" for a
# plain dynamic response with no Last-Modified/ETag - but "usually" is
# exactly the kind of gap that already bit this project once. An explicit
# `Cache-Control: no-store` removes any ambiguity: every load of the
# editor page is now guaranteed to hit this handler and get the
# current-hash script URL, rather than relying on the *absence* of a
# caching signal to mean the same thing on every browser.
_NO_STORE_HEADERS = {"Cache-Control": "no-store, must-revalidate"}

@app.get("/", response_class=HTMLResponse)
def root():
    path = pathlib.Path(__file__).parent / 'ui.html'
    return HTMLResponse(_versioned_static_html(path.read_text(encoding='utf-8')), headers=_NO_STORE_HEADERS)

@app.get("/example_workflow.yaml", response_class=PlainTextResponse)
def example_workflow():
    # Serve demo workflow from workflows folder.
    #
    # Bug fix (UI rewrite pass): this handler used to return the file's
    # text from a plain function with no response_class, which makes
    # FastAPI fall back to its default JSON encoding - so the response
    # was actually `application/json` containing the whole YAML file as
    # one big JSON *string* (quoted, with every real newline escaped to
    # a literal "\n"). ui.html's loadDemo() (both the old hand-rolled
    # version and this rewrite) does `await fetch(...).then(r => r.text())`
    # and feeds that straight to POST /parse_yaml expecting real YAML
    # text - which a JSON-quoted string is not, so yaml.safe_load() was
    # actually being handed something like the *nine-character* text
    # `"nodes:\n ...` (literal backslash-n, still wrapped in quotes) and
    # parsing it as a single YAML scalar string rather than a mapping,
    # so `data.get('nodes')`/`data.get('edges')` on the frontend saw
    # nothing and "Load Demo" silently loaded an empty canvas. Verified
    # with curl: the endpoint's content-type was `application/json` and
    # the body was JSON-quoted before this fix. response_class=
    # PlainTextResponse (matching how / and /ui already use
    # response_class=HTMLResponse for the same reason) makes this return
    # the file's real text/plain content unmodified.
    path = pathlib.Path(__file__).parent.parent.parent / 'workflows' / 'example_workflow.yaml'
    if not path.is_file():
        # fallback to root
        path = pathlib.Path(__file__).parent.parent.parent / 'example_workflow.yaml'
    return path.read_text(encoding='utf-8')

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = pathlib.Path(__file__).parent / 'ui.html'
    return HTMLResponse(_versioned_static_html(path.read_text(encoding='utf-8')), headers=_NO_STORE_HEADERS)

@app.get("/showcase", response_class=HTMLResponse)
def showcase():
    path = pathlib.Path(__file__).parent / 'showcase.html'
    return path.read_text(encoding='utf-8')

@app.get("/docs/{path:path}", response_class=HTMLResponse)
def docs(path: str):
    # Serve docs from docs/ folder
    base = pathlib.Path(__file__).parent.parent.parent / 'docs'
    file_path = base / path
    if file_path.is_file():
        return file_path.read_text(encoding='utf-8')
    # fallback to index
    idx = base / 'index.md'
    if idx.is_file():
        return idx.read_text(encoding='utf-8')
    return "Docs not found"

@app.get("/nodes")
def list_nodes():
    return to_jsonable({nid: {"type": type(node).__name__, "config": node.config, "health": node.health()} for nid, node in _nodes.items()})

@app.get("/node-schema")
def node_schema():
    """Per-node-type input/output port names (core/port_schema.py).

    This is the Phase 2 backend foundation for Phase 3's node-editor
    rework: today's UI always wires new edges with a hardcoded, uniform
    in0/in1/out0/out1 numbering that doesn't match most node types' real
    port names (see port_schema.py's module docstring), so a wire drawn
    on the canvas usually connects to nothing any node reads from. This
    endpoint is what a fixed/rebuilt UI would call once at load time to
    learn each node type's real ports instead of guessing.
    """
    from ..core.port_schema import all_port_schemas
    return all_port_schemas()

@app.get("/config-schema")
def config_schema():
    """Per-node-type config-field overrides (core/config_schema.py).

    Phase 3's inline config editor uses this alongside /node-schema: where
    /node-schema says what a node type can be *wired* to, this says how a
    few specific, verified config fields should be *edited* - e.g.
    TriggerIfNode.condition is one of four fixed strings, not free text,
    and every trigger node's target_node_id names another node in this
    same graph rather than accepting an arbitrary hand-typed id. Fields
    not listed here fall back to a widget inferred from the live value's
    own JSON type, same as before this endpoint existed.
    """
    from ..core.config_schema import all_field_schemas
    return all_field_schemas()

class ConfigUpdate(BaseModel):
    config: dict

@app.put("/nodes/{node_id}/config")
def update_config(node_id: str, upd: ConfigUpdate):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    # Route each field through set_attribute() - the same mechanism
    # attribute-wiring already uses - instead of a bare
    # `node.config.update(...)`, so a live-edited field also mirrors onto
    # a same-named self.<attr> the node's own init() may have cached (the
    # existing behavior set_attribute() already provides), rather than
    # only ever updating the config dict.
    #
    # Bug fix (found live, from a direct report): mirroring the attribute
    # alone still isn't enough for node types that derive something
    # *from* that attribute once at init() time and use it right then -
    # most notably an HTTP path/uri already registered as a live route on
    # the shared web server. Editing e.g. an ApiInputNode's `uri` field
    # used to update the config dict with no visible effect whatsoever:
    # the node's already-registered route kept pointing at the old uri
    # forever, so curling the *new* one 404'd until the node was deleted
    # and recreated (or the whole workflow was run, which builds a fresh
    # node from the current config). on_config_updated() is the new,
    # generic extension point for exactly this - each affected node type
    # overrides it to re-derive its cached path and re-register.
    if hasattr(node, 'set_attribute'):
        for k, v in upd.config.items():
            node.set_attribute(k, v)
    else:
        node.config.update(upd.config)
    if hasattr(node, 'on_config_updated'):
        node.on_config_updated(set(upd.config.keys()))
    return {"status": "updated", "node": node_id}

@app.get("/nodes/{node_id}/config")
def get_config(node_id: str, hidden: bool = False):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    cfg = node.config
    if not hidden:
        cfg = {k:v for k,v in cfg.items() if not k.startswith('_')}
    result = {"node": node_id, "config": to_jsonable(cfg)}
    # See http_endpoint_info()'s own docstring (core/web_server.py): raw
    # config alone doesn't tell a caller the node's real, effective HTTP
    # route (e.g. the "/api/" prefix an ApiInputNode's route actually
    # lives under), so this is included here too for the same reason
    # it's included in the MCP server's equivalent tool.
    endpoint = http_endpoint_info(node)
    if endpoint is not None:
        result["http_endpoint"] = endpoint
    return result

@app.get("/nodes/{node_id}/last")
def node_last(node_id: str, n: int = 50):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    # to_jsonable(): binary payloads become {"$binary": size, "head": ...}
    # and MediaItems a summary + preview_url, instead of FastAPI's own
    # encoder calling bytes.decode() and failing with a 500 (media plan
    # phase 1.4).
    return to_jsonable({"node": node_id, "last": node.get_last(n) if hasattr(node, "get_last") else []})

# MIME types GET /media/{ref} will serve as given in its `mime` query
# parameter. Anything else (text/html, image/svg+xml, ...) is served as
# application/octet-stream so a stored blob can never be rendered as an
# active document in the API's own origin.
_MEDIA_MIME_RE = re.compile(r"^(image/(png|jpeg|gif|webp|bmp|tiff)|audio/[\w.+-]+|video/[\w.+-]+|application/octet-stream)$")


@app.get("/media/{ref}")
def get_media(ref: str, mime: str | None = None):
    """Serve one blob from the media blob store (core/blob_store.py) - the
    target of a MediaItem's `preview_url` in the live view (media plan
    phase 1.4). FileResponse handles `Range` requests, so <audio>/<video>
    elements can seek. Behind the same API-key middleware as every other
    non-exempt route."""
    store = get_blob_store()
    if not store.exists(ref):
        return JSONResponse(status_code=404, content={"error": "media not found (unknown or expired ref)"})
    if not mime or not _MEDIA_MIME_RE.match(mime):
        mime = sniff_mime(store.read_head(ref, 64)) or "application/octet-stream"
        if not _MEDIA_MIME_RE.match(mime):
            mime = "application/octet-stream"
    return FileResponse(
        store.path(ref), media_type=mime,
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=600"},
    )

@app.get("/nodes/{node_id}/stats")
def node_stats(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    h = node.health()
    return {"node": node_id, "stats": h.get("stats", {}), "uptime_s": h.get("uptime_s"), "health": h.get("health")}

@app.post("/nodes/{node_id}/start")
async def node_start(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    import asyncio
    asyncio.create_task(node.start())
    return {"status": "started", "node": node_id}

@app.post("/nodes/{node_id}/stop")
async def node_stop(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    await node.stop()
    return {"status": "stopped", "node": node_id}

@app.post("/nodes/{node_id}/pause")
async def node_pause(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    # pause is implemented as stop with paused flag
    if hasattr(node, '_paused'):
        node._paused = not node._paused
    else:
        node._paused = True
    # if paused, stop processing
    if node._paused:
        await node.stop()
    else:
        import asyncio
        asyncio.create_task(node.start())
    return {"status": "paused" if node._paused else "resumed", "node": node_id}

@app.post("/nodes/{node_id}/step")
async def node_step(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    # Send single step control message
    if hasattr(node, 'handle_control'):
        import asyncio
        asyncio.create_task(node.handle_control({'action': 'step'}))
        return {"status": "stepped", "node": node_id}
    else:
        return {"error": "node does not support step"}

@app.post("/nodes/{node_id}/reset")
async def node_reset(node_id: str):
    """Clear a node's own accumulated state (stats, error info, and any
    node-specific accumulator like a StackNode's stack or a TableNode's
    rows - see BaseNode.reset()'s docstring) without stopping/restarting
    it. Mirrors POST /nodes/{id}/step exactly - same 'send a control
    message, fire-and-forget the coroutine' shape - since this is the
    same kind of lightweight control action, not a lifecycle transition.
    """
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    if hasattr(node, 'handle_control'):
        import asyncio
        asyncio.create_task(node.handle_control({'action': 'reset'}))
        return {"status": "reset", "node": node_id}
    else:
        return {"error": "node does not support reset"}

@app.post("/nodes/{node_id}/emit")
async def node_emit(node_id: str):
    """The node editor's title-bar "manual emit" button: re-emit each of
    this node's output ports' most recently emitted item again, right
    now, via BaseNode.manual_emit() (see its docstring for why this is a
    replay of the last known value rather than forcing a fresh
    process() step). Called synchronously and directly, unlike /step
    above - a manual button click wants an immediate, in-this-response
    answer of what was actually replayed, not a fire-and-forget control
    message whose effect the caller can't observe.
    """
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    if not hasattr(node, 'manual_emit'):
        return {"error": "node does not support manual emit"}
    replayed = node.manual_emit()
    return {"status": "emitted", "node": node_id, "replayed": to_jsonable(replayed)}

class NodeCreateReq(BaseModel):
    node_id: str
    node_type: str
    config: dict = {}

@app.post("/nodes")
async def create_node(req: NodeCreateReq):
    from ..core.registry import build_node_registry
    registry = build_node_registry()
    cls = registry.get(req.node_type)
    if not cls:
        return {"error": f"unknown node type {req.node_type}"}
    if req.node_id in _nodes:
        return {"error": "node id already exists"}
    node = cls(req.node_id, req.config)
    _nodes[req.node_id] = node
    from ..core.web_server import register_node
    register_node(node)
    # A node added to a live graph this way (dragged from the palette /
    # duplicated onto a running session, rather than loaded as part of a
    # whole workflow run) used to sit inert - created and registered, but
    # never started - until something separately called POST
    # /nodes/{id}/start (the node editor's own "▶ Start" context-menu
    # action). That left every freshly-added node showing idle/grey
    # (STATUS_COLORS.idle in editor.js) and doing nothing until a manual
    # extra step, which is surprising: dropping a node onto a graph reads
    # as "add this to the running flow", not "stage it, inactive, for
    # later". Auto-starting it here (same asyncio.create_task(node.start())
    # pattern node_start() above uses) makes "started/active" the default
    # state a newly added node lands in.
    import asyncio
    asyncio.create_task(node.start())
    return {"status": "created", "node_id": req.node_id}

class ConnectReq(BaseModel):
    source_id: str
    source_port: str
    target_id: str
    target_port: str
    # Added alongside the Phase 2 port-schema validation below - this
    # endpoint previously had no way to say what kind of wire it was
    # making at all, unlike Edge/the /workflows path. Defaults to "data"
    # so existing callers that don't send it behave exactly as before.
    type: str = "data"
    # Optional queue settings for this edge, same as a workflow edge's
    # `buffer` ({maxsize, drop_policy}; media plan phase 1.5).
    buffer: dict | None = None

# _attribute_pump_tasks/_cancel_attribute_pump used to be defined here as
# this module's own private state. Moved into core/engine.py (alongside
# _pump_attribute, which was already shared) so mcp/server.py's own
# connect_nodes/disconnect_nodes/delete_node tools can see and clean up the
# exact same pump tasks this endpoint creates - see that module's own
# comment for why a second, independent dict here would leak tasks across
# the two surfaces. Imported (not re-implemented) so every existing
# reference below, and every test that imports `_attribute_pump_tasks`
# from `pystreamflow.api.server` directly, keeps working unchanged.
from ..core.engine import _attribute_pump_tasks, _cancel_attribute_pump, _edge_pipes

# Regression fix: /nodes/connect (both the POST connect_nodes and,
# especially, the DELETE disconnect_nodes below) must be registered
# *before* the generic DELETE /nodes/{node_id} route (delete_node). FastAPI/
# Starlette matches routes in registration order, and "/nodes/{node_id}"
# happily matches the literal path "/nodes/connect" (with node_id="connect")
# - so with delete_node registered first, every DELETE /nodes/connect
# request used to be swallowed by delete_node before disconnect_nodes ever
# ran: it looked up (and would delete, if one existed) a node literally
# named "connect" and otherwise just returned {"error": "node not found"},
# silently masking the fact that the real disconnect endpoint was
# completely unreachable from any HTTP client. Declaring the specific
# "/nodes/connect" path first makes Starlette match it before falling
# through to the parameterized route.

@app.post("/nodes/connect")
async def connect_nodes(req: ConnectReq):
    from ..core.engine import edge_pipe, validate_buffer
    from ..core.port_schema import validate_edge
    src = _nodes.get(req.source_id)
    tgt = _nodes.get(req.target_id)
    if not src or not tgt:
        return {"error": "source or target not found"}
    # Validate against what these two *live* node instances' classes
    # actually support, the same way /workflows validates a saved Graph -
    # so a bad wire made through this endpoint is rejected with a clear
    # error instead of silently connecting a pipe nothing reads from (see
    # core/port_schema.py's module docstring for why that used to happen).
    err = validate_edge(
        type(src).__name__, req.source_port, type(tgt).__name__, req.target_port, req.type,
    ) or validate_buffer(req.buffer)
    if err:
        return {"error": err}
    pipe = edge_pipe(src, req.source_port, req.buffer)
    # Phase 2 of the wire-kind-unification design: a 'raw' edge is wired
    # exactly like a 'data' edge below - the only difference is the
    # delivery kind recorded against this consumer, which is what makes
    # BaseNode.emit() unwrap the item for it instead of delivering it as-is.
    src.add_output(req.source_port, pipe, kind='raw' if req.type == 'raw' else 'data')
    # Phase 1 fan-out change: remember exactly which pipe this edge added
    # so disconnect_nodes() below can remove only this one consumer later,
    # even if other edges are also fanned out from the same source_port -
    # see core/engine.py's _edge_pipes docstring.
    _edge_pipes[(req.source_id, req.source_port, req.target_id, req.target_port)] = pipe
    if req.type == 'control':
        # See Engine._wire_edges()'s matching fix and port_schema.py's
        # validate_edge() comment for why: a control edge always lands on
        # the target's single reserved 'control' pipe (BaseNode.add_input()'s
        # `name == 'control'` special case), never on req.target_port -
        # this endpoint used to call `tgt.add_input(req.target_port, pipe)`
        # unconditionally, just like the Engine's own wiring did, which
        # meant connecting a control edge through this live-node API could
        # silently replace whatever real data pipe was already wired into
        # that target port (typically 'in') with the trigger's own output
        # pipe - the same corruption bug, reachable through a second code
        # path.
        tgt.add_input('control', pipe)
        src._graph_control_targets.append(req.target_id)
    elif req.type == 'attribute':
        # Bug fix (feedback: "only by clicking the run button some states
        # are updated (attribute changes including wiring changes)"): an
        # attribute edge needs a live background pump task feeding the
        # pipe into the target's set_attribute() (see core/engine.py's
        # Engine._wire_edges()/_pump_attribute()) - nothing in a node's own
        # process() loop ever reads self.inputs under an arbitrary
        # attribute-port name, so the old unconditional
        # `tgt.add_input(req.target_port, pipe)` below silently did
        # nothing for this edge type. This endpoint didn't spawn that task
        # at all before, so an attribute wire drawn directly onto two
        # already-running ad-hoc nodes had zero effect until "Run"
        # rebuilt the whole graph as a real Session (whose Engine always
        # did this correctly). Cancels any previous pump task already
        # registered under this exact edge first, so redrawing the same
        # wire (e.g. after a disconnect that didn't reach this endpoint)
        # can't leave two tasks feeding the same attribute.
        from ..core.engine import _pump_attribute
        key = (req.source_id, req.target_id, req.target_port)
        _cancel_attribute_pump(key)
        _attribute_pump_tasks[key] = asyncio.create_task(_pump_attribute(pipe, tgt, req.target_port))
    else:
        tgt.add_input(req.target_port, pipe)
    return {"status": "connected"}

@app.delete("/nodes/connect")
async def disconnect_nodes(req: ConnectReq):
    src = _nodes.get(req.source_id)
    tgt = _nodes.get(req.target_id)
    if not src or not tgt:
        return {"error": "source or target not found"}
    # Remove output/input references
    # Phase 1 fan-out change: a source_port can now have more than one live
    # consumer, so a blanket `del src.outputs[req.source_port]` would kill
    # every one of them instead of just this edge. Look up exactly which
    # pipe this edge added (recorded in connect_nodes() above) and remove
    # only that one.
    edge_pipe = _edge_pipes.pop(
        (req.source_id, req.source_port, req.target_id, req.target_port), None,
    )
    if edge_pipe is not None:
        src.remove_output(req.source_port, edge_pipe)
    elif req.source_port in src.outputs:
        # Fallback for an edge that predates this per-edge tracking (e.g.
        # wired via Engine._wire_edges() rather than this ad-hoc endpoint).
        # Only safe to guess when exactly one consumer is left - otherwise
        # leave every consumer alone rather than risk removing the wrong
        # one.
        pipes = src.outputs[req.source_port]
        if len(pipes) == 1:
            src.remove_output(req.source_port, pipes[0][0])
    if req.type == 'control':
        # Mirrors connect_nodes() above: BaseNode.add_input('control', ...)
        # never puts the control pipe into tgt.inputs at all - it appends
        # to tgt._control_pipes (see core/node.py) - so disconnecting a
        # control edge means removing exactly that one pipe from there,
        # not deleting req.target_port from tgt.inputs (the pre-fix
        # behavior: either a no-op if that name happened to be unwired,
        # or, worse, deleting an unrelated real data port that happened to
        # share the name the control edge was nominally "targeting").
        #
        # This used to be `tgt._control_pipe = None` - a blanket wipe of
        # *every* control source the target had, not just this one edge,
        # the exact same "silently kills every other consumer too" bug
        # class Phase 1 already fixed on the data-output disconnect path.
        # `edge_pipe` (looked up above) is the literal same Pipe object
        # that was passed to both `src.add_output()` and
        # `tgt.add_input('control', pipe)` in connect_nodes(), so it's
        # exactly what remove_control_input() needs to remove only this
        # edge's source and leave any other wired control source alone.
        if edge_pipe is not None:
            await tgt.remove_control_input(edge_pipe)
        elif len(tgt._control_pipes) == 1:
            # Fallback for an edge that predates this per-edge tracking
            # (e.g. wired via Engine._wire_edges() rather than this ad-hoc
            # endpoint) - mirrors the identical output-side fallback
            # above: only safe to guess which pipe to remove when exactly
            # one control source is left.
            await tgt.remove_control_input(tgt._control_pipes[0])
    elif req.type == 'attribute':
        # Matches connect_nodes()'s new attribute branch above: the pump
        # task, not tgt.inputs (an attribute edge's pipe was never put
        # there), is what needs cleaning up here.
        _cancel_attribute_pump((req.source_id, req.target_id, req.target_port))
    elif edge_pipe is not None:
        # Fan-in fix: a data input port can now have more than one live
        # source (see BaseNode.add_input()/remove_input() and
        # core/stream.py's FanInPipe), so the old blanket
        # `del tgt.inputs[req.target_port]` below would kill every other
        # source fanned into the same input along with the one edge
        # actually being removed - the identical bug class already fixed
        # on the output-disconnect path above and the control-disconnect
        # path in the branch above this one. `edge_pipe` is the literal
        # same Pipe object passed to both `src.add_output()` and
        # `tgt.add_input()` in connect_nodes(), so remove_input() can
        # remove exactly this edge's source and leave any other one
        # fanned into the same input alone.
        tgt.remove_input(req.target_port, edge_pipe)
    elif req.target_port in tgt.inputs:
        # Fallback for an edge that predates per-edge pipe tracking (e.g.
        # wired via Engine._wire_edges() rather than this ad-hoc
        # endpoint) - mirrors the identical output/control-side fallbacks
        # above: only safe to guess which source to remove when exactly
        # one is left; otherwise leave the input's sources alone rather
        # than risk removing the wrong one.
        sources = tgt._input_sources.get(req.target_port)
        if sources and len(sources) == 1:
            tgt.remove_input(req.target_port, sources[0])
        elif not sources:
            del tgt.inputs[req.target_port]
    if req.target_id in src._graph_control_targets:
        src._graph_control_targets.remove(req.target_id)
    return {"status": "disconnected"}

@app.delete("/nodes/{node_id}")
async def delete_node(node_id: str):
    node = _nodes.pop(node_id, None)
    if not node:
        return {"error": "node not found"}
    await node.stop()
    # Deleting a node without first calling DELETE /nodes/connect on every
    # attribute edge touching it (the normal case - the editor deletes a
    # node and its links together, not edge-by-edge first) would otherwise
    # leak that edge's _pump_attribute() task forever: nothing else ever
    # cancels it, and the task itself loops on `await pipe.get()` with no
    # way to notice its target node is gone. Sweep both directions here.
    for key in [k for k in _attribute_pump_tasks if node_id in (k[0], k[1])]:
        _cancel_attribute_pump(key)
    return {"status": "deleted", "node_id": node_id}

class WorkflowModel(BaseModel):
    nodes: list
    edges: list

@app.post("/workflows")
def save_workflow_api(wf: WorkflowModel):
    from ..core.port_schema import validate_edge
    graph = Graph()
    for n in wf.nodes:
        graph.add_node(Node(**n))
    node_types = {n.id: n.type for n in graph.nodes}
    for e in wf.edges:
        edge = Edge(**e)
        # Reject a wire that doesn't land on a port either endpoint's node
        # type actually has, instead of saving it and having it silently
        # do nothing once the workflow runs (see core/port_schema.py).
        src_type = node_types.get(edge.source)
        tgt_type = node_types.get(edge.target)
        if src_type is None:
            return {"error": f"edge source {edge.source!r} not found among the workflow's nodes"}
        if tgt_type is None:
            return {"error": f"edge target {edge.target!r} not found among the workflow's nodes"}
        err = validate_edge(src_type, edge.source_port, tgt_type, edge.target_port, edge.type)
        if err:
            return {"error": f"invalid edge {edge.source}->{edge.target}: {err}"}
        graph.add_edge(edge)
    import uuid
    fname = f"/tmp/workflow_{uuid.uuid4().hex}.yaml"
    save_workflow(graph, fname)
    return {"status":"saved","path":fname}

@app.get("/workflows")
def list_workflows():
    import os
    files = [f for f in os.listdir('/tmp') if f.startswith('workflow_') and f.endswith('.yaml')]
    return {"workflows": files}

@app.get("/plugins")
def list_plugins():
    pm = PluginManager(pathlib.Path(__file__).parent.parent / 'plugins')
    pm.load_plugins()
    return {"plugins": pm.list_nodes()}

@app.get("/subgraph-file")
def read_subgraph_file(path: str):
    """Read a SubgraphNode's own embedded workflow file (its
    config['workflow_path'] - see nodes/subgraph.py) as editor-shaped
    {nodes, edges} JSON, so the node editor's "open subgraph" action
    (Request C9: "there should be some way to view/edit subgraphs") can
    load it into a nested graph view the same way GET
    /sessions/{id}/workflow lets it load a running session's graph.

    Node x/y/label aren't part of core/models.py's Node dataclass, and
    are deliberately never written into the plain `nodes:` list a
    SubgraphNode itself parses at runtime via load_workflow() (see
    graphToRunPayload()'s matching stripping, for the same reason: a bare
    Node(**n) call raises on an unexpected keyword argument) - they
    round-trip through Graph.meta['editor_layout'] instead (see
    write_subgraph_file() below), a plain dict any workflow YAML can
    already carry untouched.
    """
    p = pathlib.Path(path)
    if not p.is_file():
        return {"error": f"no such file: {path}"}
    try:
        graph = load_workflow(str(p))
    except Exception as e:
        return {"error": f"failed to parse {path}: {e}"}
    layout = (getattr(graph, 'meta', None) or {}).get('editor_layout', {})
    nodes_out = []
    for n in graph.nodes:
        pos = layout.get(n.id, {})
        nodes_out.append({
            "id": n.id,
            "type": n.type,
            "config": n.config,
            "label": pos.get("label", n.id),
            "x": pos.get("x", 0),
            "y": pos.get("y", 0),
        })
    edges_out = [
        {
            "source": e.source, "target": e.target,
            "source_port": e.source_port, "target_port": e.target_port,
            "type": e.type,
        }
        for e in graph.edges
    ]
    return {"nodes": nodes_out, "edges": edges_out}

class SubgraphFileWriteReq(BaseModel):
    path: str
    nodes: list
    edges: list

@app.post("/subgraph-file")
def write_subgraph_file(req: SubgraphFileWriteReq):
    """Write the node editor's in-memory view of a subgraph's contents
    back out to its own workflow YAML file - the save half of
    GET /subgraph-file above. Validates edges the same way POST
    /workflows does (see validate_edge()), so a bad wire inside a
    subgraph is rejected here instead of only surfacing as a silent
    no-op the next time some outer workflow actually runs it."""
    from ..core.port_schema import validate_edge
    graph = Graph()
    node_types: dict[str, str] = {}
    layout: dict[str, dict] = {}
    for n in req.nodes:
        node_id = n["id"]
        graph.add_node(Node(id=node_id, type=n["type"], config=n.get("config", {})))
        node_types[node_id] = n["type"]
        layout[node_id] = {
            "x": n.get("x", 0), "y": n.get("y", 0), "label": n.get("label", node_id),
        }
    for e in req.edges:
        edge = Edge(
            source=e["source"], target=e["target"],
            source_port=e.get("source_port", "out"), target_port=e.get("target_port", "in"),
            type=e.get("type", "data"), buffer=e.get("buffer"),
        )
        src_type = node_types.get(edge.source)
        tgt_type = node_types.get(edge.target)
        if src_type is None:
            return {"error": f"edge source {edge.source!r} not found among the subgraph's nodes"}
        if tgt_type is None:
            return {"error": f"edge target {edge.target!r} not found among the subgraph's nodes"}
        err = validate_edge(src_type, edge.source_port, tgt_type, edge.target_port, edge.type)
        if err:
            return {"error": f"invalid edge {edge.source}->{edge.target}: {err}"}
        graph.add_edge(edge)
    graph.meta = {"editor_layout": layout}
    try:
        pathlib.Path(req.path).parent.mkdir(parents=True, exist_ok=True)
        save_workflow(graph, req.path)
    except Exception as e:
        return {"error": f"failed to save {req.path}: {e}"}
    return {"status": "saved", "path": req.path}

class YamlParseReq(BaseModel):
    yaml_text: str

@app.post("/parse_yaml")
def parse_yaml(req: YamlParseReq):
    try:
        data = yaml.safe_load(req.yaml_text)
        return {"ok": True, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# Session management API
class SessionCreateReq(BaseModel):
    workflow_path: str

@app.post("/sessions")
async def create_session(req: SessionCreateReq):
    sess = session_manager.create(req.workflow_path)
    try:
        await sess.load()
    except Exception as e:
        # Bug fix found while wiring the CLI's session_create command
        # (previously broken - see cli/main.py's own comment) up to this
        # real endpoint: sess.load() (persistence.load_workflow() +
        # Engine.validate()) was never wrapped here, so a missing file or
        # an invalid workflow YAML raised straight out of this route and
        # FastAPI turned it into a bare 500 with a generic HTML/plain-text
        # body - not the `{"error": ...}` shape every other endpoint in
        # this file uses for an expected failure, and not something the
        # CLI (or the editor) could show the person anything useful for.
        # Also removes the half-created session rather than leaving a
        # permanently broken entry (graph=None, engine=None) sitting in
        # session_manager.sessions forever.
        session_manager.delete(sess.id)
        return {"error": f"failed to load workflow: {e}"}
    return {"id": sess.id, "status": sess.status}

@app.get("/sessions")
def list_sessions():
    return {"sessions": session_manager.list_sessions()}

@app.get("/sessions/{session_id}")
def get_session(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    return sess.info()

@app.post("/sessions/{session_id}/start")
async def start_session(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    await sess.start()
    return {"id": sess.id, "status": sess.status}

@app.post("/sessions/{session_id}/stop")
async def stop_session(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    await sess.stop()
    return {"id": sess.id, "status": sess.status}

@app.post("/sessions/{session_id}/pause")
async def pause_session(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    await sess.pause()
    return {"id": sess.id, "status": sess.status}

@app.post("/sessions/{session_id}/resume")
async def resume_session(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    await sess.resume()
    return {"id": sess.id, "status": sess.status}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    # Bug fix (multi-session verification pass): this used to be a plain
    # `def` endpoint, so FastAPI/Starlette ran it in a worker thread via
    # anyio.to_thread.run_sync() rather than on the app's own event loop.
    # SessionManager.delete() fire-and-forget-schedules a running
    # session's shutdown with `asyncio.create_task(sess.stop())` - which
    # requires a *running* event loop in the calling thread and raised
    # `RuntimeError: no running event loop` every time, on every worker
    # thread, for every attempt to delete a still-running session (a
    # freshly-created, never-started session deleted fine, since that
    # branch was never reached - which is exactly why this was easy to
    # miss testing only the empty/not-found path). Verified via a live
    # server: DELETE on a running session 500'd unconditionally before
    # this fix. Now this endpoint runs on the real event loop (an `async
    # def` FastAPI route is awaited directly there, never off-loaded to a
    # worker thread) and stops the session itself first with a real
    # `await`, so by the time SessionManager.delete() runs its own
    # (unchanged, still-synchronous) `sess.task.done()` check, there's
    # nothing left for it to fire-and-forget.
    sess = session_manager.sessions.get(session_id)
    if sess is not None and sess.task and not sess.task.done():
        await sess.stop()
    ok = session_manager.delete(session_id)
    return {"deleted": ok}

@app.get("/sessions/{session_id}/workflow")
def get_session_workflow(session_id: str):
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    if not sess.graph:
        return {"error": "no graph"}
    return {
        "nodes": to_jsonable([vars(n) for n in sess.graph.nodes]),
        "edges": [vars(e) for e in sess.graph.edges]
    }

# Session-scoped node introspection.
#
# Bug fix (multi-session verification pass): /nodes/* and /reflection/*
# below key purely by bare node id in one process-wide registry
# (core/web_server.py's `_nodes`), with no notion of which Session a node
# belongs to. Two Sessions running workflows that share a node id - the
# ordinary case for running the same workflow file twice - silently
# collide there: only the most-recently-registered one is reachable by
# id, the other becomes invisible to every id-based endpoint. These two
# routes give an unambiguous way to reach a specific session's node
# regardless of id collisions elsewhere (see web_server.get_session_nodes()
# / register_node()'s docstring for the full explanation); they're
# additive - every existing /nodes and /reflection endpoint's behavior is
# unchanged.
@app.get("/sessions/{session_id}/nodes")
def list_session_nodes(session_id: str):
    from ..core.web_server import get_session_nodes
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    nodes = get_session_nodes(session_id)
    return {
        nid: {
            "type": type(node).__name__,
            "config": {k: v for k, v in node.config.items() if not k.startswith('_')},
            "health": node.health(),
        }
        for nid, node in nodes.items()
    }

@app.get("/sessions/{session_id}/nodes/{node_id}")
def get_session_node(session_id: str, node_id: str):
    from ..core.web_server import get_session_nodes
    sess = session_manager.get(session_id)
    if not sess:
        return {"error": "not found"}
    node = get_session_nodes(session_id).get(node_id)
    if not node:
        return {"error": "node not found in this session"}
    return {
        "id": node_id,
        "session_id": session_id,
        "type": type(node).__name__,
        "config": to_jsonable(node.config),
        "inputs": {k: {"stats": p.stats()} for k, p in node.inputs.items()},
        # Phase 1 fan-out change: an output port can now have several live
        # consumer pipes, so its stats are a list (one per consumer) instead of
        # a single dict - see core/node.py's BaseNode.outputs docstring.
        "outputs": {k: {"stats": [{**p.stats(), "kind": kind} for p, kind in pipes]} for k, pipes in node.outputs.items()},
        "health": node.health(),
        "last_items": to_jsonable(node.get_last() if hasattr(node, "get_last") else []),
    }

# Reflection API
@app.get("/reflection/nodes")
def reflection_nodes():
    result = {}
    for nid, node in _nodes.items():
        h = node.health()
        result[nid] = {
            "type": type(node).__name__,
            "config": to_jsonable({k:v for k,v in node.config.items() if not k.startswith('_')}),
            "inputs": list(node.inputs.keys()),
            "outputs": list(node.outputs.keys()),
            "health": h.get("health"),
            "running": h.get("running"),
            "paused": h.get("paused"),
            "stats": h.get("stats", {}),
            "uptime_s": h.get("uptime_s"),
        }
    return {"nodes": result}

@app.get("/reflection/nodes/{node_id}")
def reflection_node(node_id: str):
    node = _nodes.get(node_id)
    if not node:
        return {"error": "node not found"}
    h = node.health()
    result = {
        "id": node_id,
        "type": type(node).__name__,
        "config": to_jsonable(node.config),
        "inputs": {k: {"stats": p.stats()} for k,p in node.inputs.items()},
        # Phase 1 fan-out change: see the matching comment above.
        "outputs": {k: {"stats": [{**p.stats(), "kind": kind} for p, kind in pipes]} for k, pipes in node.outputs.items()},
        "health": h,
        "last_items": to_jsonable(node.get_last() if hasattr(node, "get_last") else []),
    }
    endpoint = http_endpoint_info(node)
    if endpoint is not None:
        result["http_endpoint"] = endpoint
    return result

@app.get("/reflection/graph")
def reflection_graph():
    # Build a simple graph view from current nodes
    nodes_view = []
    for nid, node in _nodes.items():
        inputs = []
        for in_name, pipe in node.inputs.items():
            # Find upstream node via reverse mapping – best effort
            inputs.append({"port": in_name})
        outputs = []
        for out_name in node.outputs.keys():
            outputs.append({"port": out_name})
        nodes_view.append({
            "id": nid,
            "type": type(node).__name__,
            "inputs": inputs,
            "outputs": outputs,
            "health": node.health().get("health")
        })
    return {"nodes": nodes_view}

# Bug fix (reported: LM Studio couldn't connect to the MCP server at
# exactly this URL - "SSE error: Non-200 status code (404)"): a bare
# "/mcp" (no trailing slash - what LM Studio was actually configured
# with, and what a real MCP client naturally requests) 404'd even after
# mcp/server.py's own rewrite onto a real, spec-compliant MCP transport
# put a genuine endpoint at mcp_app's root. Root cause is specific to
# this app's own route layout: Starlette's Mount(path="/mcp", ...) below
# can only match a path that already has a slash after "mcp" (its own
# matching regex is built as "/mcp" + "/{path:path}") - a bare "/mcp"
# normally falls through to Starlette's own redirect_slashes fallback
# (a 307 to "/mcp/"), but this app also mounts web_server_app at "/" as
# a catch-all (further below, for the node-editor SPA) - a Mount whose
# own path is "" and whose match-anything regex ("/{path:path}") DOES
# match a bare "/mcp" (capturing "mcp" as its own sub-path) - so that
# catch-all silently swallowed the request before the redirect fallback
# ever got a chance, returning web_server_app's own "no such route" 404
# instead of ever reaching mcp_app. Confirmed live: "/mcp/" (with the
# slash) and "/mcp/sse" already worked fine; only the bare prefix needed
# this explicit redirect, registered here (before either mount) so it's
# tried first for exactly this one path.
@app.api_route("/mcp", methods=["GET", "POST", "DELETE", "HEAD", "OPTIONS"], include_in_schema=False)
async def _mcp_root_redirect(request: Request):
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=str(request.url) + "/", status_code=307)

# Mount MCP tools under /mcp
app.mount("/mcp", mcp_app)

# Bug fix (found during a codebase-wide audit for unimplemented/inert
# functionality): core/metrics.py built and unit-tested a whole Prometheus
# `/metrics` endpoint (APIRouter, node_health/node_errors/
# node_items_processed) that was never actually wired into any app
# anywhere - this app included - so GET /metrics 404'd in every real
# deployment; only tests that called metrics()/update_metrics() directly
# ever exercised it. include_router(), not another app.mount(), since
# metrics.router is a plain APIRouter (a set of routes to merge into this
# app), not a separate ASGI sub-application the way mcp_app/web_server_app
# are.
from ..core.metrics import router as metrics_router

app.include_router(metrics_router)

# Real bug found this pass, reported as "the api input/output nodes don't
# actually work" (curl POST http://localhost:8000/api/<uri>/... -> 404):
# ApiInputNode/ApiOutputNode (pystreamflow/nodes/input_api.py,
# output_api.py) register their "/api/<uri>" routes onto
# `core.web_server.app` - a second, completely separate FastAPI
# application from this module's `app` - and only that second app ever
# gets served, by a *private* uvicorn.Server each of those nodes spins up
# itself (core.web_server.start_server(), default 0.0.0.0:8080) from its
# own process() coroutine. That private server is a deliberate, correct
# design for WebInputNode/WebOutputNode/WebOutputJSONNode, which keep
# their own host/port config precisely because they're meant to be
# reachable standalone, with no editor/API server involved at all. But
# ApiInputNode/ApiOutputNode just had their host/port config fields
# removed from the UI on exactly the reasoning that they live under the
# *shared* server's "/api" namespace - i.e. whatever this app is already
# serving (port 8000 by default) - not a private port of their own. So
# every "/api/<uri>" route a node registers has only ever existed on the
# private 8080 server nobody was curling, while this app's real, running
# port answered every "/api/*" request with a bare 404 (no such route
# here at all) - the private server was never wrong, it just was not the
# server anyone reasonably expected to ask.
#
# Fixed by mounting `core.web_server.app` into this app as a catch-all
# fallback at "/" - registered dead last, after every explicit route/mount
# above, so Starlette only ever falls through to it for a path nothing
# above already claimed (an exact prefix/route match earlier in the list
# always wins - insertion order is what Starlette's router matches in).
# Mounting delegates to the *live* sub-application object at request time,
# not a frozen snapshot, so routes ApiInputNode/ApiOutputNode add to
# `core.web_server.app` at node-start time - which happens well after this
# module (and this mount call) has already been imported - are still
# picked up correctly. This makes "/api/<uri>" reachable on this app's own
# port with zero change to how those two node types build their paths,
# and it's additive: the private 8080 server such a node also still starts
# for itself keeps working unchanged for anyone running a bare workflow
# via `pystreamflow_cli.py run` with no editor/API server involved at all.
app.mount("/", web_server_app)
