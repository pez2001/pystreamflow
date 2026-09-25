"""
PyStreamFlow MCP Server
Full Model Context Protocol implementation for AI agent interaction.
Exposes graph tools via FastAPI with Bearer / X-API-Key authentication.

Bug fix (reported: LM Studio couldn't connect - "SSE error: Non-200
status code (404)" against http://<host>:8000/mcp): this module used to
hand-roll its own approximation of the MCP wire protocol - a `/sse` GET
that only ever sent `event: connected` (never the `event: endpoint`
message the real SSE transport's handshake requires) and a `/messages`
POST that had no `initialize` method at all (every real MCP client's
very first call). `/mcp` itself (the URL actually typed into LM Studio)
had no route whatsoever - only `/mcp/sse`/`/mcp/messages` existed - which
is exactly the 404 reported. None of this was ever validated against a
real third-party MCP client; it only ever worked for callers (including
Claude, via docs/ai_guide.md) who read the docs and hand-crafted a
`/call`/`/messages` request in this module's own bespoke shape.

Rewritten on the official `mcp` Python SDK (MCPServer, formerly
FastMCP) so this is now a genuinely spec-compliant server, reachable by
any real MCP client - LM Studio, Claude Desktop, Cursor, etc. - over
both transports the spec defines:
  - Streamable HTTP (current, recommended) at exactly `/mcp` (the URL
    LM Studio was already configured with).
  - SSE (legacy, still what some clients default to) at `/mcp/sse`
    (GET, opens the stream) + `/mcp/messages/` (POST).
The ~30 existing tools and their actual dispatch logic (_execute_tool(),
below - previously this function's body lived directly under
call_tool()) are completely unchanged; only the wire protocol they're
served over is new. The pre-existing `/call`, `/tools`, `/health` REST
convenience endpoints (and PSF_MCP_API_KEY's Bearer/X-API-Key auth) are
also unchanged, for any caller who prefers a plain HTTP request over
implementing a real MCP client.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import logging
import os
import asyncio
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from starlette.routing import get_route_path
from ..core.media import to_jsonable
from ..core.web_server import _nodes, http_endpoint_info

logger = logging.getLogger("pystreamflow.mcp")

MCP_API_KEY = os.getenv("PSF_MCP_API_KEY", "")

# Bug fix (reported: "mcp still not working" - GET /mcp/tools 403 Forbidden
# from a real client that was, per the user, using "the access token").
# Root cause: unlike PSF_API_KEY (core/auth.py's main-API key), which is
# auto-generated AND announced in a clear startup log line if the operator
# never set one, PSF_MCP_API_KEY has always been silent - nothing anywhere
# ever logged what value this server actually expects. Made worse by the
# project's own docker-compose.yml (see that file's own comment) shipping
# a *live, non-empty* default of "changeme" for this variable rather than
# leaving it unset - so a docker-compose deployment silently requires a
# key ("changeme") the operator likely never noticed setting, while the
# actual value they put in their MCP client (typically the main API key
# from data/api_key.txt or the server's own startup log, since that's the
# one this project documents as "auto-generated, check the log") doesn't
# match it at all. Every request then gets exactly this 403 - a real,
# non-empty token was sent, it's just the wrong one. Logging the exact
# expected value (or that no key is required) the moment this module
# loads means an operator can always answer "what key does this server
# actually want right now" by looking at its own startup output, instead
# of having to go spelunking through env vars/compose files by hand.
if MCP_API_KEY:
    logger.warning(
        "PSF_MCP_API_KEY is set - every /mcp/* request (both real MCP "
        "transports and the /tools, /call REST convenience endpoints) "
        "must send this exact value as 'Authorization: Bearer %s' or "
        "'X-API-Key: %s'. A mismatch here (not a missing key) comes back "
        "as 403 Forbidden, not 401 - if you're seeing 403s, this is the "
        "value to check against whatever your MCP client has configured, "
        "not the separate main PSF_API_KEY.",
        MCP_API_KEY, MCP_API_KEY,
    )
else:
    # Bug fix (found live: an operator's own `docker compose logs | grep
    # PSF_MCP_API_KEY` came back completely empty even after this line
    # was added and PSF_MCP_API_KEY was genuinely unset). This was
    # `logger.info(...)` - but uvicorn's default logging setup (no
    # `--log-level` override, which is how this project's own Dockerfile
    # runs it) never configures a handler for arbitrary app loggers like
    # this module's "pystreamflow.mcp" at INFO level; only `logging`'s
    # built-in "last resort" handler catches anything, and that one only
    # fires at WARNING or above. The "PSF_MCP_API_KEY is set" branch above
    # already used `logger.warning(...)` and so was never affected -
    # only this "not set" branch was silently swallowed. `warning` here
    # isn't really "something's wrong" so much as "this is
    # security-relevant startup info that must actually reach the
    # console regardless of how (or whether) an operator has configured
    # logging elsewhere" - reproduced and confirmed fixed by running
    # uvicorn exactly as the Dockerfile's own CMD does (no --log-level)
    # and grepping its output.
    logger.warning(
        "PSF_MCP_API_KEY is not set - /mcp/* is reachable with no "
        "authentication at all. Set PSF_MCP_API_KEY to require a key."
    )


def _extract_bearer_or_api_key(authorization: Optional[str], x_api_key: Optional[str]) -> Optional[str]:
    """Shared 'Authorization: Bearer <token>' / 'X-API-Key: <token>'
    parsing, used by both require_auth() (the /tools, /call REST routes'
    FastAPI dependency) and _MCPAuthMiddleware (the real transports'
    ASGI-level check) below - previously each had its own independent
    copy of this exact same extraction logic, which worked (both stayed
    in sync by coincidence) but meant a future change to one could
    silently drift from the other. Returns None if no token was sent, in
    whichever header form either code path recognizes.
    """
    if authorization and authorization.startswith("Bearer "):
        return authorization.split(" ", 1)[1]
    if x_api_key:
        return x_api_key
    return None


def require_auth(authorization: str = Header(None), x_api_key: str = Header(None, alias="X-API-Key")):
    if not MCP_API_KEY:
        return
    token = _extract_bearer_or_api_key(authorization, x_api_key)
    if not token:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    if token != MCP_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

class Tool(BaseModel):
    name: str
    description: str
    input_schema: Dict[str, Any]

class ToolCall(BaseModel):
    tool: str
    arguments: Dict[str, Any] = {}

TOOLS: List[Tool] = [
    Tool(
        name="get_version",
        description="Get PyStreamFlow version information",
        input_schema={"type": "object", "properties": {}}
    ),
    Tool(
        name="list_nodes",
        description="List all registered nodes in the graph",
        input_schema={"type": "object", "properties": {}}
    ),
    Tool(
        name="get_node_last",
        description="Get last N items passed through a node",
        input_schema={
            "type": "object",
            "properties": {
                "node_id": {"type": "string"},
                "n": {"type": "integer", "default": 50}
            },
            "required": ["node_id"]
        }
    ),
    Tool(
        name="get_node_stats",
        description="Get health and stats for a node",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="get_node_config",
        description="Get node configuration",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="update_node_config",
        description="Update node configuration",
        input_schema={
            "type": "object",
            "properties": {
                "node_id": {"type": "string"},
                "config": {"type": "object"}
            },
            "required": ["node_id", "config"]
        }
    ),
    Tool(
        name="send_to_node",
        description="Send data to a node's input pipe",
        input_schema={
            "type": "object",
            "properties": {
                "node_id": {"type": "string"},
                "payload": {"type": "object"}
            },
            "required": ["node_id", "payload"]
        }
    ),
    Tool(
        name="node_start",
        description="Start a node",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="node_stop",
        description="Stop a node",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="node_pause",
        description="Pause/resume a node",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="node_step",
        description="Single step a node via control trigger",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="node_emit",
        description="Manually re-emit each of a node's output ports' most recently emitted item again",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="node_reset",
        description="Clear a node's own accumulated state (stats, error info, and any node-specific accumulator like a stack/queue/table) via control trigger, without stopping/restarting it",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="reflect_nodes",
        description="Reflect all nodes with type, config, inputs, outputs, health and stats",
        input_schema={"type": "object", "properties": {}}
    ),
    Tool(
        name="reflect_node",
        description="Reflect a single node with full introspection",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="reflect_graph",
        description="Reflect graph topology of nodes and ports",
        input_schema={"type": "object", "properties": {}}
    ),
    Tool(
        name="create_node",
        description="Create a new node by type and config",
        input_schema={
            "type": "object",
            "properties": {
                "node_id": {"type": "string"},
                "node_type": {"type": "string"},
                "config": {"type": "object"}
            },
            "required": ["node_id", "node_type"]
        }
    ),
    Tool(
        name="delete_node",
        description="Delete a node by id",
        input_schema={
            "type": "object",
            "properties": {"node_id": {"type": "string"}},
            "required": ["node_id"]
        }
    ),
    Tool(
        name="connect_nodes",
        description="Connect source node output port to target node input port",
        input_schema={
            "type": "object",
            "properties": {
                "source_id": {"type": "string"},
                "source_port": {"type": "string"},
                "target_id": {"type": "string"},
                "target_port": {"type": "string"},
                # Matches POST /nodes/connect's ConnectReq.type (api/server.py):
                # one of "data" (default), "control", "attribute", "endpoint",
                # "raw" (Phase 2 of the wire-kind-unification design - same
                # wiring as "data", but the consumer receives the item run
                # through the same unwrap helper an "attribute" edge uses).
                # Added alongside this tool's edge-kind support below - see
                # connect_nodes' own comment in call_tool() for why a plain
                # data-shaped wire was never enough for the other kinds.
                "type": {"type": "string", "default": "data"}
            },
            "required": ["source_id", "source_port", "target_id", "target_port"]
        }
    ),
    Tool(
        name="disconnect_nodes",
        description="Disconnect source and target ports",
        input_schema={
            "type": "object",
            "properties": {
                "source_id": {"type": "string"},
                "source_port": {"type": "string"},
                "target_id": {"type": "string"},
                "target_port": {"type": "string"},
                "type": {"type": "string", "default": "data"}
            },
            "required": ["source_id", "source_port", "target_id", "target_port"]
        }
    ),
    Tool(
        name="create_session",
        description="Create a new workflow session from a YAML file path",
        input_schema={
            "type": "object",
            "properties": {"workflow_path": {"type": "string"}},
            "required": ["workflow_path"]
        }
    ),
    Tool(
        name="list_sessions",
        description="List all active sessions",
        input_schema={"type": "object", "properties": {}}
    ),
    Tool(
        name="get_session",
        description="Get session info",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="start_session",
        description="Start a session",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="stop_session",
        description="Stop a session",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="pause_session",
        description="Pause a session",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="resume_session",
        description="Resume a paused session",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="delete_session",
        description="Delete a session",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="list_session_nodes",
        description=(
            "List the nodes owned by one session, unambiguously - unlike "
            "list_nodes/reflect_nodes (which read the single process-wide "
            "node registry), this can't be confused by another session or "
            "the ad-hoc node editor reusing the same node id"
        ),
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
        }
    ),
    Tool(
        name="get_session_node",
        description="Get one node's health/config/stats, scoped to a specific session (see list_session_nodes)",
        input_schema={
            "type": "object",
            "properties": {"session_id": {"type": "string"}, "node_id": {"type": "string"}},
            "required": ["session_id", "node_id"]
        }
    ),
]

async def _execute_tool(tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # The actual tool dispatch - unchanged from before this module's
    # rewrite onto the official MCP SDK (see the module docstring above).
    # This used to be the body of call_tool() (the /call route handler)
    # directly; it's now a plain function so both /call (below, a thin
    # wrapper that adds the require_auth() check) and every real MCP tool
    # registered on mcp_server (further below) can share it, without the
    # MCP-SDK-registered tools re-triggering /call's own auth check a
    # second time (they're already behind their own transport-level auth -
    # see _MCPTransportAuth below).
    from ..core.session_manager import session_manager
    import asyncio

    args = args or {}

    if tool == "get_version":
        try:
            from importlib.metadata import version as get_version
            v = get_version("pystreamflow")
        except Exception:
            import pystreamflow
            v = getattr(pystreamflow, "__version__", "unknown")
        return {"result": {"name": "pystreamflow", "version": v}}

    if tool == "list_nodes":
        return {"result": {nid: {"type": type(n).__name__, "config": n.config} for nid, n in _nodes.items()}}

    if tool == "get_node_last":
        node_id = args.get("node_id")
        n = int(args.get("n", 50))
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        last = node.get_last(n) if hasattr(node, "get_last") else []
        return {"result": {"node": node_id, "last": last}}

    if tool == "get_node_stats":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        h = node.health()
        return {"result": {"node": node_id, "health": h}}

    if tool == "get_node_config":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        cfg = {k: v for k, v in node.config.items() if not k.startswith("_")}
        result = {"node": node_id, "config": cfg}
        # See http_endpoint_info()'s own docstring (core/web_server.py): this node's raw
        # config alone (e.g. just {"uri": "demo/in"}) doesn't tell a
        # caller the real, effective route (the "/api/" prefix), so a
        # caller reading only `config` here has no way to construct a
        # working URL - included here too, not just in reflect_node,
        # since either tool is a reasonable place to look for it.
        endpoint = http_endpoint_info(node)
        if endpoint is not None:
            result["http_endpoint"] = endpoint
        return {"result": result}

    if tool == "update_node_config":
        node_id = args.get("node_id")
        config = args.get("config") or {}
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        # Bug fix (found during a codebase-wide audit for unimplemented/
        # inert functionality): this used to do a raw `node.config.update()`
        # that never went through set_attribute()/on_config_updated() the
        # way the HTTP PUT /nodes/{id}/config endpoint (api/server.py)
        # already does - see that endpoint's own comment for the two
        # concrete gaps this left: (1) a node type that also caches a
        # config value as a same-named self.<attr> in init() (the
        # established pattern across nodes/) never saw the update reflected
        # there, so an already-running node's own process() loop (which
        # typically reads self.<attr>, not self.config[...]) kept acting on
        # the stale value; (2) node types that derive something *from* a
        # config field once at init() and use it right then (most notably
        # an HTTP route registered from a `uri`/`path` field - WebInputNode,
        # ApiInputNode, ApiOutputNode, UserInputNode, WebOutputNode,
        # WebOutputJSONNode) never got a chance to re-derive and
        # re-register it, so editing that field through the MCP tool had no
        # visible effect at all until the node was deleted and recreated.
        # Routing through the same two mechanisms the HTTP endpoint uses
        # closes both gaps for the MCP path too.
        if hasattr(node, 'set_attribute'):
            for k, v in config.items():
                node.set_attribute(k, v)
        else:
            node.config.update(config)
        if hasattr(node, 'on_config_updated'):
            node.on_config_updated(set(config.keys()))
        return {"result": {"status": "updated", "node_id": node_id}}

    if tool == "send_to_node":
        node_id = args.get("node_id")
        payload = args.get("payload")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        if node.inputs:
            pipe = next(iter(node.inputs.values()))
            asyncio.create_task(pipe.put(payload))
            return {"result": {"status": "sent", "node": node_id}}
        else:
            node.emit('out', payload)
            return {"result": {"status": "emitted", "node": node_id}}

    if tool == "node_start":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        asyncio.create_task(node.start())
        return {"result": {"status": "started", "node": node_id}}

    if tool == "node_stop":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        await node.stop()
        return {"result": {"status": "stopped", "node": node_id}}

    if tool == "node_pause":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        if hasattr(node, "_paused"):
            node._paused = not node._paused
        else:
            node._paused = True
        if node._paused:
            await node.stop()
        else:
            asyncio.create_task(node.start())
        return {"result": {"status": "paused" if node._paused else "resumed", "node": node_id}}

    if tool == "node_step":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        if hasattr(node, "handle_control"):
            asyncio.create_task(node.handle_control({"action": "step"}))
            return {"result": {"status": "stepped", "node": node_id}}
        return {"error": "node does not support step"}

    if tool == "node_emit":
        # Mirrors POST /nodes/{id}/emit in api/server.py - the "manual
        # emit" action (BaseNode.manual_emit(): re-emit each output
        # port's most recently emitted item again). Added for the same
        # reason every other node lifecycle action (start/stop/pause/
        # step) has both an HTTP and an MCP form.
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        if not hasattr(node, "manual_emit"):
            return {"error": "node does not support manual emit"}
        replayed = node.manual_emit()
        return {"result": {"status": "emitted", "node": node_id, "replayed": replayed}}

    if tool == "node_reset":
        # Mirrors POST /nodes/{id}/reset in api/server.py - see
        # BaseNode.reset()'s docstring for exactly what gets cleared.
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        if hasattr(node, "handle_control"):
            asyncio.create_task(node.handle_control({"action": "reset"}))
            return {"result": {"status": "reset", "node": node_id}}
        return {"error": "node does not support reset"}

    if tool == "reflect_nodes":
        result = {}
        for nid, node in _nodes.items():
            h = node.health()
            result[nid] = {
                "type": type(node).__name__,
                "config": {k: v for k, v in node.config.items() if not k.startswith("_")},
                "inputs": list(node.inputs.keys()),
                "outputs": list(node.outputs.keys()),
                "health": h.get("health"),
                "running": h.get("running"),
                "paused": h.get("paused"),
                "stats": h.get("stats", {}),
                "uptime_s": h.get("uptime_s"),
            }
        return {"result": {"nodes": result}}

    if tool == "reflect_node":
        node_id = args.get("node_id")
        node = _nodes.get(node_id)
        if not node:
            return {"error": "node not found"}
        h = node.health()
        result = {
            "id": node_id,
            "type": type(node).__name__,
            "config": node.config,
            "inputs": {k: {"stats": p.stats()} for k, p in node.inputs.items()},
            # Phase 1 fan-out change: an output port can now have several
            # live consumer pipes, so its stats are a list (one per
            # consumer) instead of a single dict - see core/node.py's
            # BaseNode.outputs docstring.
            "outputs": {k: {"stats": [{**p.stats(), "kind": kind} for p, kind in pipes]} for k, pipes in node.outputs.items()},
            "health": h,
            "last_items": node.get_last() if hasattr(node, "get_last") else []
        }
        endpoint = http_endpoint_info(node)
        if endpoint is not None:
            result["http_endpoint"] = endpoint
        return {"result": result}

    if tool == "reflect_graph":
        nodes_view = []
        for nid, node in _nodes.items():
            nodes_view.append({
                "id": nid,
                "type": type(node).__name__,
                "inputs": list(node.inputs.keys()),
                "outputs": list(node.outputs.keys()),
                "health": node.health().get("health")
            })
        return {"result": {"nodes": nodes_view}}

    if tool == "create_node":
        node_id = args.get("node_id")
        node_type = args.get("node_type")
        config = args.get("config") or {}
        from ..core.registry import build_node_registry
        registry = build_node_registry()
        cls = registry.get(node_type)
        if not cls:
            return {"error": f"unknown node type {node_type}"}
        if node_id in _nodes:
            return {"error": "node id already exists"}
        node = cls(node_id, config)
        _nodes[node_id] = node
        from ..core.web_server import register_node
        register_node(node)
        # Match api/server.py's create_node: a node added to a live graph
        # should default to started/active rather than sitting inert until
        # something separately calls start on it.
        asyncio.create_task(node.start())
        return {"result": {"status": "created", "node_id": node_id}}

    if tool == "delete_node":
        node_id = args.get("node_id")
        node = _nodes.pop(node_id, None)
        if not node:
            return {"error": "node not found"}
        asyncio.create_task(node.stop())
        # Matches DELETE /nodes/{node_id} (api/server.py): a node can be
        # the source or target of a live attribute-edge pump task (see
        # connect_nodes() above and core/engine.py's shared
        # _attribute_pump_tasks), and deleting it out from under that task
        # without cancelling first leaked it forever - the task just kept
        # calling set_attribute() on (for the target side) or reading from
        # (for the source side) a node no longer reachable from `_nodes`,
        # with nothing left able to ever cancel it. Both surfaces now share
        # the same tracking dict, so this cleans up an edge wired through
        # either the HTTP API or this MCP tool.
        from ..core.engine import _attribute_pump_tasks, _cancel_attribute_pump
        for key in [k for k in _attribute_pump_tasks if node_id in (k[0], k[1])]:
            _cancel_attribute_pump(key)
        return {"result": {"status": "deleted", "node_id": node_id}}

    if tool == "connect_nodes":
        # Bug fix (found during a codebase-wide audit for unimplemented/
        # inert functionality): this used to unconditionally do a plain
        # `src.add_output(...)` / `tgt.add_input(...)` pair with no `type`
        # field at all and no validate_edge() call - i.e. always exactly
        # what POST /nodes/connect (api/server.py) does for a plain "data"
        # edge, and NOTHING ELSE. Concretely that meant: (1) a bad wire
        # (unknown port name for either node type) connected silently
        # instead of being rejected, unlike the HTTP endpoint; (2) a
        # "control" edge landed on `tgt.inputs[target_port]` - an ordinary
        # named input slot most control-consuming code never reads (see
        # BaseNode.add_input()'s dedicated `name == 'control'` handling and
        # api/server.py's own connect_nodes() comment for the exact same
        # bug fixed there) - and never registered in
        # `src._graph_control_targets`, so a Trigger* node's control wire
        # made through MCP could never actually fire its target; (3) an
        # "attribute" edge got no live pump task at all (see
        # core.engine._pump_attribute) - nothing in a node's own process()
        # loop reads self.inputs under an arbitrary attribute-port name -
        # so wiring e.g. a ConstantValueNode's output into another node's
        # `path` attribute via this tool had zero effect, forever. Mirrors
        # api/server.py's connect_nodes() exactly (including sharing its
        # `_attribute_pump_tasks` bookkeeping via core/engine.py - see that
        # module's own comment for why a second, independent dict here
        # would leak tasks), so an edge wired through this MCP tool behaves
        # identically to the same edge wired through the HTTP API or a
        # loaded workflow YAML.
        from ..core.engine import (
            _attribute_pump_tasks,
            _cancel_attribute_pump,
            _edge_pipes,
            _pump_attribute,
            edge_pipe as make_edge_pipe,
        )
        from ..core.port_schema import validate_edge

        source_id = args.get("source_id")
        source_port = args.get("source_port")
        target_id = args.get("target_id")
        target_port = args.get("target_port")
        edge_type = args.get("type", "data")
        src = _nodes.get(source_id)
        tgt = _nodes.get(target_id)
        if not src or not tgt:
            return {"error": "source or target not found"}
        err = validate_edge(type(src).__name__, source_port, type(tgt).__name__, target_port, edge_type)
        if err:
            return {"error": err}
        pipe = make_edge_pipe(src, source_port)
        # Phase 2 of the wire-kind-unification design: a 'raw' edge is
        # wired exactly like a 'data' edge below - only the recorded
        # delivery kind differs, which is what makes BaseNode.emit()
        # unwrap the item for this consumer instead of delivering it as-is.
        src.add_output(source_port, pipe, kind='raw' if edge_type == 'raw' else 'data')
        # Phase 1 fan-out change: remember exactly which pipe this edge
        # added so disconnect_nodes() below can remove only this one
        # consumer later, even if other edges are also fanned out from the
        # same source_port - see core/engine.py's _edge_pipes docstring.
        _edge_pipes[(source_id, source_port, target_id, target_port)] = pipe
        if edge_type == 'control':
            tgt.add_input('control', pipe)
            src._graph_control_targets.append(target_id)
        elif edge_type == 'attribute':
            key = (source_id, target_id, target_port)
            _cancel_attribute_pump(key)
            _attribute_pump_tasks[key] = asyncio.create_task(_pump_attribute(pipe, tgt, target_port))
        else:
            tgt.add_input(target_port, pipe)
        # Port-dtype mismatch (media plan phase 6b): connected anyway, reported.
        from ..core.port_schema import edge_warning
        warning = edge_warning(type(src).__name__, source_port, type(tgt).__name__, target_port, edge_type)
        return {"result": {"status": "connected", **({"warning": warning} if warning else {})}}

    if tool == "disconnect_nodes":
        # Mirrors connect_nodes() above and api/server.py's
        # disconnect_nodes(): each edge type needs to be unwound the same
        # way it was wired, not just a blanket `del tgt.inputs[target_port]`
        # (which is a no-op for control/attribute edges - neither one ever
        # put its pipe there in the first place - and, worse, would delete
        # an unrelated real data port that happened to share the name a
        # control/attribute edge was nominally "targeting").
        from ..core.engine import _cancel_attribute_pump, _edge_pipes

        source_id = args.get("source_id")
        source_port = args.get("source_port")
        target_id = args.get("target_id")
        target_port = args.get("target_port")
        edge_type = args.get("type", "data")
        src = _nodes.get(source_id)
        tgt = _nodes.get(target_id)
        if not src or not tgt:
            return {"error": "source or target not found"}
        # Phase 1 fan-out change: a source_port can now have more than one
        # live consumer, so a blanket `del src.outputs[source_port]` would
        # kill every one of them instead of just this edge. Look up
        # exactly which pipe this edge added (recorded in connect_nodes()
        # above) and remove only that one.
        edge_pipe = _edge_pipes.pop((source_id, source_port, target_id, target_port), None)
        if edge_pipe is not None:
            src.remove_output(source_port, edge_pipe)
        elif source_port in src.outputs:
            # Fallback for an edge that predates this per-edge tracking
            # (e.g. wired via Engine._wire_edges() rather than this ad-hoc
            # tool). Only safe to guess when exactly one consumer is left.
            pipes = src.outputs[source_port]
            if len(pipes) == 1:
                src.remove_output(source_port, pipes[0][0])
        if edge_type == 'control':
            # Mirrors api/server.py's disconnect_nodes() fix: this used to
            # be a blanket `tgt._control_pipe = None`, wiping *every*
            # control source the target had rather than just this one
            # edge - the same "kills every other consumer too" bug class
            # already fixed on the data-output disconnect path above.
            # `edge_pipe` is the literal same Pipe object connect_nodes()
            # passed to both `src.add_output()` and
            # `tgt.add_input('control', pipe)`, so it's exactly what
            # remove_control_input() needs to remove only this edge's
            # source.
            if edge_pipe is not None:
                await tgt.remove_control_input(edge_pipe)
            elif len(tgt._control_pipes) == 1:
                # Fallback for an edge that predates per-edge tracking -
                # only safe to guess when exactly one control source is
                # left, mirroring the identical output-side fallback above.
                await tgt.remove_control_input(tgt._control_pipes[0])
        elif edge_type == 'attribute':
            _cancel_attribute_pump((source_id, target_id, target_port))
        elif edge_pipe is not None:
            # Fan-in fix (mirrors api/server.py's disconnect_nodes()): a
            # data input port can now have more than one live source (see
            # BaseNode.add_input()/remove_input() and core/stream.py's
            # FanInPipe), so the old blanket `del tgt.inputs[target_port]`
            # below would kill every other source fanned into the same
            # input along with the one edge actually being removed.
            # `edge_pipe` is the literal same Pipe object connect_nodes()
            # passed to both `src.add_output()` and `tgt.add_input()`, so
            # remove_input() can remove exactly this edge's source.
            tgt.remove_input(target_port, edge_pipe)
        elif target_port in tgt.inputs:
            # Fallback for an edge that predates per-edge pipe tracking -
            # only safe to guess which source to remove when exactly one
            # is left, mirroring the identical output/control-side
            # fallbacks above.
            sources = tgt._input_sources.get(target_port)
            if sources and len(sources) == 1:
                tgt.remove_input(target_port, sources[0])
            elif not sources:
                del tgt.inputs[target_port]
        if target_id in src._graph_control_targets:
            src._graph_control_targets.remove(target_id)
        return {"result": {"status": "disconnected"}}

    if tool == "create_session":
        workflow_path = args.get("workflow_path")
        if not workflow_path:
            return {"error": "workflow_path required"}
        sess = session_manager.create(workflow_path)
        await sess.load()
        return {"result": {"session_id": sess.id, "status": sess.status}}

    if tool == "list_sessions":
        return {"result": {"sessions": session_manager.list_sessions()}}

    if tool == "get_session":
        sid = args.get("session_id")
        sess = session_manager.get(sid)
        if not sess:
            return {"error": "session not found"}
        return {"result": sess.info()}

    if tool == "start_session":
        sid = args.get("session_id")
        sess = session_manager.get(sid)
        if not sess:
            return {"error": "session not found"}
        await sess.start()
        return {"result": {"session_id": sess.id, "status": sess.status}}

    if tool == "stop_session":
        sid = args.get("session_id")
        sess = session_manager.get(sid)
        if not sess:
            return {"error": "session not found"}
        await sess.stop()
        return {"result": {"session_id": sess.id, "status": sess.status}}

    if tool == "pause_session":
        sid = args.get("session_id")
        sess = session_manager.get(sid)
        if not sess:
            return {"error": "session not found"}
        await sess.pause()
        return {"result": {"session_id": sess.id, "status": sess.status}}

    if tool == "resume_session":
        sid = args.get("session_id")
        sess = session_manager.get(sid)
        if not sess:
            return {"error": "session not found"}
        await sess.resume()
        return {"result": {"session_id": sess.id, "status": sess.status}}

    if tool == "delete_session":
        sid = args.get("session_id")
        ok = session_manager.delete(sid)
        return {"result": {"deleted": ok, "session_id": sid}}

    if tool == "list_session_nodes":
        from ..core.web_server import get_session_nodes
        sid = args.get("session_id")
        if not session_manager.get(sid):
            return {"error": "session not found"}
        nodes = get_session_nodes(sid)
        return {"result": {
            nid: {"type": type(n).__name__, "config": n.config, "health": n.health()}
            for nid, n in nodes.items()
        }}

    if tool == "get_session_node":
        from ..core.web_server import get_session_nodes
        sid = args.get("session_id")
        nid = args.get("node_id")
        if not session_manager.get(sid):
            return {"error": "session not found"}
        node = get_session_nodes(sid).get(nid)
        if not node:
            return {"error": "node not found in this session"}
        return {"result": {
            "id": nid,
            "session_id": sid,
            "type": type(node).__name__,
            "config": node.config,
            "health": node.health(),
            "last_items": node.get_last() if hasattr(node, "get_last") else [],
        }}

    return {"error": "unknown tool"}


def _unwrap(result: Dict[str, Any]) -> Any:
    # Every real MCP tool below returns this straight to the SDK, which
    # serializes it as the tool's structured result. Raising here (rather
    # than returning the old {"error": ...} shape as-is) is what makes an
    # error surface as a spec-compliant MCP tool error (a CallToolResult
    # with is_error=True) instead of a silently "successful" call whose
    # payload just happens to be {"error": "..."} - a real MCP client has
    # no reason to specially look for that shape.
    #
    # Bug fix (found live, not by inspection): raising a plain ValueError
    # here made every one of these - "node not found", "unknown tool",
    # "source or target not found", etc, all clearly deliberate, expected
    # errors this dispatch already produces on purpose - come out on the
    # client as the generic "Error executing tool <name>" with NO further
    # detail. The SDK's Tool.run() (mcp/server/mcpserver/tools/base.py)
    # deliberately strips the real message from any exception that isn't
    # its own ToolError/ResourceError - by design, so a genuine crash
    # never leaks internal details to the model - which was silently
    # swallowing the actual, useful error text these tools have always
    # returned (the old JSON-RPC shape included it verbatim:
    # {"error": {"code": -32603, "message": result["error"]}}). Raising
    # the SDK's own ToolError instead is what keeps that real message
    # intact end to end.
    if "error" in result:
        raise ToolError(result["error"])
    # Binary payloads/MediaItems -> JSON-safe summaries (media plan phase
    # 1.4) - the SDK would otherwise fail serializing raw bytes, and a
    # model has no use for megabytes of base64 in its context anyway.
    return to_jsonable(result.get("result"))


_TOOL_DESCRIPTIONS = {t.name: t.description for t in TOOLS}

# The real, spec-compliant MCP server (see the module docstring's "Bug
# fix" note for why this replaced a hand-rolled approximation). Every
# tool below is a thin wrapper around _execute_tool() above - identical
# names/descriptions/required-vs-optional arguments to this module's own
# pre-existing TOOLS list (used verbatim for descriptions, so the two
# can't drift out of sync), just with real Python type hints so the SDK
# generates a correct JSON schema and validates/coerces arguments before
# ever reaching _execute_tool(), instead of the old free-form dict.
mcp_server = MCPServer(name="pystreamflow", instructions=(
    "Tools for inspecting and controlling a running PyStreamFlow node "
    "graph: node lifecycle (start/stop/pause/step/emit/reset), graph "
    "editing (create/delete/connect/disconnect nodes), introspection "
    "(list/reflect nodes and the graph), and workflow sessions."
))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_version"])
async def get_version() -> Any:
    return _unwrap(await _execute_tool("get_version", {}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["list_nodes"])
async def list_nodes() -> Any:
    return _unwrap(await _execute_tool("list_nodes", {}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_node_last"])
async def get_node_last(node_id: str, n: int = 50) -> Any:
    return _unwrap(await _execute_tool("get_node_last", {"node_id": node_id, "n": n}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_node_stats"])
async def get_node_stats(node_id: str) -> Any:
    return _unwrap(await _execute_tool("get_node_stats", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_node_config"])
async def get_node_config(node_id: str) -> Any:
    return _unwrap(await _execute_tool("get_node_config", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["update_node_config"])
async def update_node_config(node_id: str, config: Dict[str, Any]) -> Any:
    return _unwrap(await _execute_tool("update_node_config", {"node_id": node_id, "config": config}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["send_to_node"])
async def send_to_node(node_id: str, payload: Dict[str, Any]) -> Any:
    return _unwrap(await _execute_tool("send_to_node", {"node_id": node_id, "payload": payload}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_start"])
async def node_start(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_start", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_stop"])
async def node_stop(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_stop", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_pause"])
async def node_pause(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_pause", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_step"])
async def node_step(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_step", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_emit"])
async def node_emit(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_emit", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["node_reset"])
async def node_reset(node_id: str) -> Any:
    return _unwrap(await _execute_tool("node_reset", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["reflect_nodes"])
async def reflect_nodes() -> Any:
    return _unwrap(await _execute_tool("reflect_nodes", {}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["reflect_node"])
async def reflect_node(node_id: str) -> Any:
    return _unwrap(await _execute_tool("reflect_node", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["reflect_graph"])
async def reflect_graph() -> Any:
    return _unwrap(await _execute_tool("reflect_graph", {}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["create_node"])
async def create_node(node_id: str, node_type: str, config: Optional[Dict[str, Any]] = None) -> Any:
    return _unwrap(await _execute_tool("create_node", {
        "node_id": node_id, "node_type": node_type, "config": config or {},
    }))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["delete_node"])
async def delete_node(node_id: str) -> Any:
    return _unwrap(await _execute_tool("delete_node", {"node_id": node_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["connect_nodes"])
async def connect_nodes(source_id: str, source_port: str, target_id: str, target_port: str, type: str = "data") -> Any:
    return _unwrap(await _execute_tool("connect_nodes", {
        "source_id": source_id, "source_port": source_port,
        "target_id": target_id, "target_port": target_port, "type": type,
    }))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["disconnect_nodes"])
async def disconnect_nodes(source_id: str, source_port: str, target_id: str, target_port: str, type: str = "data") -> Any:
    return _unwrap(await _execute_tool("disconnect_nodes", {
        "source_id": source_id, "source_port": source_port,
        "target_id": target_id, "target_port": target_port, "type": type,
    }))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["create_session"])
async def create_session(workflow_path: str) -> Any:
    return _unwrap(await _execute_tool("create_session", {"workflow_path": workflow_path}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["list_sessions"])
async def list_sessions() -> Any:
    return _unwrap(await _execute_tool("list_sessions", {}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_session"])
async def get_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("get_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["start_session"])
async def start_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("start_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["stop_session"])
async def stop_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("stop_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["pause_session"])
async def pause_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("pause_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["resume_session"])
async def resume_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("resume_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["delete_session"])
async def delete_session(session_id: str) -> Any:
    return _unwrap(await _execute_tool("delete_session", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["list_session_nodes"])
async def list_session_nodes(session_id: str) -> Any:
    return _unwrap(await _execute_tool("list_session_nodes", {"session_id": session_id}))


@mcp_server.tool(description=_TOOL_DESCRIPTIONS["get_session_node"])
async def get_session_node(session_id: str, node_id: str) -> Any:
    return _unwrap(await _execute_tool("get_session_node", {"session_id": session_id, "node_id": node_id}))


class _MCPAuthMiddleware:
    """Raw ASGI middleware (not Starlette's BaseHTTPMiddleware, which
    buffers/replays the body and would break the SSE transport's
    streamed response) applying the same optional PSF_MCP_API_KEY check
    require_auth() applies to the legacy /tools, /call endpoints -
    needed separately here because the real MCP transport routes below
    are plain Starlette routes generated by the SDK, not FastAPI routes,
    so they can't take require_auth() as a Header-injected dependency
    the way the legacy endpoints do. Applied to the whole app (see
    app.add_middleware() below) rather than only the new routes, since
    that's the only way to guard a plain Starlette Route/Mount at all -
    /tools and /call end up checked twice (once here, once by their own
    require_auth() call), which is redundant but harmless, since both
    checks read the exact same key the exact same way. /health is
    explicitly exempted below to preserve its existing, deliberately
    always-public behavior (a liveness check, matching the main API's
    identical exemption for its own /health). Reads MCP_API_KEY fresh on
    every request (not a value captured at construction time) so it
    keeps working if a test monkeypatches the module attribute, exactly
    like require_auth() itself already does.

    Bug fix (found live, not by inspection): comparing against the raw
    scope["path"] worked when this app runs standalone, but not once
    api/server.py mounts it at "/mcp" - Starlette's Mount rewrites
    root_path/routing for its sub-app, but never touches scope["path"]
    itself, so a request to the outer /mcp/health arrives here with
    scope["path"] still literally "/mcp/health", never matching the bare
    "/health" this was comparing against - meaning /mcp/health was
    incorrectly demanding a key once PSF_MCP_API_KEY was set. Using
    get_route_path() (the same helper Starlette's own router uses to
    resolve a path relative to root_path) makes this exemption resolve
    correctly under any mount prefix, standalone or not.
    """

    _PUBLIC_PATHS = {"/health"}

    def __init__(self, app):
        self._app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not MCP_API_KEY or get_route_path(scope) in self._PUBLIC_PATHS:
            await self._app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        auth_header = headers.get(b"authorization", b"").decode("latin-1")
        api_key_header = headers.get(b"x-api-key", b"").decode("latin-1")
        token = _extract_bearer_or_api_key(auth_header or None, api_key_header or None)
        if not token:
            response = JSONResponse({"error": "Missing Authorization header"}, status_code=401)
        elif token != MCP_API_KEY:
            response = JSONResponse({"error": "Invalid API key"}, status_code=403)
        else:
            await self._app(scope, receive, send)
            return
        await response(scope, receive, send)


# Bug fix (reported: after fixing the docker-compose "changeme" 403,
# LM Studio's error changed to "SSE error: Non-200 status code (421)" -
# 421 Misdirected Request). Root cause: neither streamable_http_app() nor
# sse_app() was ever given an explicit `host=`/`transport_security=`, so
# both fell back to the SDK's own default (host="127.0.0.1") - and both
# of the SDK's own methods auto-enable DNS-rebinding protection whenever
# `transport_security is None` and `host` looks like loopback, hardcoding
# `allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"]`. That's a
# real, sensible default for a browser-facing local dev server (its whole
# purpose is stopping a malicious webpage from DNS-rebinding its way into
# a service that's only *supposed* to be reachable from the same machine)
# - but this server is deliberately reachable from elsewhere on the LAN
# by a real hostname (exactly the reported setup: LM Studio on a
# different machine, talking to "http://mcp.lan:8000/mcp"), so a `Host:
# mcp.lan:8000` header can never match that hardcoded allowlist and every
# request gets rejected with 421, regardless of whether the caller's key
# is right. This was never caught in this project's own testing because
# every test/live-verification pass so far talked to the server over
# `127.0.0.1`/`localhost` - the one case this protection doesn't block.
#
# Since this server already has its own purpose-built access control
# (PSF_MCP_API_KEY, checked independently by _MCPAuthMiddleware/
# require_auth() above) that isn't tied to what hostname the caller used,
# the SDK's browser-oriented DNS-rebinding defense is redundant here and,
# left at its auto-enabled default, actively breaks the documented LAN
# deployment case. Disabled by default; PSF_MCP_ALLOWED_HOSTS lets an
# operator who wants this extra layer back turn it on scoped to their own
# real host(s) instead of the hardcoded loopback-only list.
def _build_transport_security():
    """TransportSecuritySettings for both transport apps below. Unset
    PSF_MCP_ALLOWED_HOSTS (the default) disables the SDK's DNS-rebinding
    Host-header check entirely - this app is reachable by any hostname,
    same as literally every other route this project serves, and relies
    on PSF_MCP_API_KEY (when set) for real access control instead. Set
    PSF_MCP_ALLOWED_HOSTS to a comma-separated list (e.g.
    "mcp.lan:8000,localhost:8000") to re-enable the Host-header check
    scoped to exactly those values, for an operator who wants that extra
    layer on top of the API key.
    """
    allowed = [h.strip() for h in os.getenv("PSF_MCP_ALLOWED_HOSTS", "").split(",") if h.strip()]
    if not allowed:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed,
        allowed_origins=[f"http://{h}" for h in allowed] + [f"https://{h}" for h in allowed],
    )


# streamable_http_path="/" (rather than the SDK's own default, "/mcp")
# puts the endpoint at the *root* of whatever mounts this - and this
# whole module's `app` is itself mounted at "/mcp" by api/server.py, so
# the effective external path comes out to exactly "/mcp": the URL LM
# Studio was already configured with. sse_path/message_path keep the
# SDK's own defaults, coming out to "/mcp/sse" and "/mcp/messages/".
def _build_transport_apps():
    transport_security = _build_transport_security()
    return (
        mcp_server.streamable_http_app(streamable_http_path="/", transport_security=transport_security),
        mcp_server.sse_app(sse_path="/sse", message_path="/messages/", transport_security=transport_security),
    )


_streamable_http_app, _sse_app = _build_transport_apps()
_mcp_transport_routes = list(_streamable_http_app.routes) + list(_sse_app.routes)


@asynccontextmanager
async def mcp_lifespan(_app):
    # Starlette/FastAPI do NOT cascade the "lifespan" ASGI event down into
    # a mounted sub-application automatically - only the outermost app
    # that actually receives the raw lifespan scope runs its own
    # lifespan. Since `app` below is, in turn, mounted onto api/server.py's
    # own outer app, BOTH `app` and api/server.py's own app need to
    # explicitly enter this same context manager for the Streamable HTTP
    # transport's session manager task group to ever actually start -
    # api/server.py imports this exact function and does the same thing.
    # Skipping this is what produces "RuntimeError: Task group is not
    # initialized. Make sure to use run()." on the very first Streamable
    # HTTP request, confirmed while building this fix.
    #
    # Rebuilds `_streamable_http_app`/`_sse_app` (and swaps their routes
    # into `app.router.routes` in place) on every single entry, rather
    # than reusing whatever was built once at import time. The SDK's own
    # StreamableHTTPSessionManager.run() explicitly documents "can only
    # be called once per instance - create a new instance if you need to
    # run again", and never resets that guard even after a clean
    # shutdown. A real deployment only ever enters this lifespan once for
    # the whole life of the process, so that guard is invisible there -
    # but this project's own test suite has several *different*,
    # unrelated tests (each needing to keep a session's background engine
    # task alive across calls) that legitimately do `with TestClient(app)
    # as c:` against this module's `app` directly, or against
    # api/server.py's outer app that mounts it - and since both paths
    # enter this exact function, a second such test anywhere in the same
    # pytest run used to crash with the SDK's RuntimeError the moment it
    # tried to start, no matter which one ran first. Rebuilding fresh
    # instances here means every lifespan cycle gets its own, never-yet-
    # started manager, so this app can be started and stopped any number
    # of times in the same process - a strictly more robust posture than
    # a one-shot-only app even outside of tests (nothing about a real
    # deployment relies on the old singleton never changing).
    global _streamable_http_app, _sse_app, _mcp_transport_routes
    streamable_http_app, sse_app = _build_transport_apps()
    new_routes = list(streamable_http_app.routes) + list(sse_app.routes)
    for old_route in _mcp_transport_routes:
        try:
            app.router.routes.remove(old_route)
        except ValueError:
            pass
    app.router.routes.extend(new_routes)
    _streamable_http_app, _sse_app, _mcp_transport_routes = streamable_http_app, sse_app, new_routes

    async with _streamable_http_app.router.lifespan_context(_streamable_http_app):
        yield


app = FastAPI(title="PyStreamFlow MCP Server", lifespan=mcp_lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "mcp"}


@app.get("/tools")
def list_tools(authorization: str = Header(None), x_api_key: str = Header(None, alias="X-API-Key")):
    require_auth(authorization, x_api_key)
    return {"tools": [t.model_dump() for t in TOOLS]}


@app.post("/call")
async def call_tool(req: ToolCall, authorization: str = Header(None), x_api_key: str = Header(None, alias="X-API-Key")):
    require_auth(authorization, x_api_key)
    return to_jsonable(await _execute_tool(req.tool, req.arguments or {}))


# The real MCP transport routes, merged directly into this app's own
# route list rather than mounted as a nested sub-application. Mounting
# them (app.mount("/", ...)) was the first approach tried here, and it
# is what api/server.py itself does with this whole module's `app` - but
# doing that *again* one level further in nested a Mount inside a Mount,
# and the bare "/mcp" URL LM Studio was actually configured with (no
# trailing slash) 404'd through that double nesting even though
# "/mcp/mcp/" (i.e. `/mcp/` on this app) and `/mcp/sse` both worked -
# confirmed live against a real running daemon while building this fix.
# Extending this app's own route list directly - exactly one Mount deep
# once api/server.py mounts this whole `app` at "/mcp" - is what actually
# makes the bare "/mcp" URL resolve.
#
# This is the import-time copy (so `app.router.routes` has real MCP
# routes registered even before the app's lifespan has ever run once -
# e.g. for introspecting `app.routes` without starting anything). Every
# actual lifespan start (mcp_lifespan(), above) replaces these with a
# freshly-built pair, since the ones built here share their session
# manager with nothing else and would otherwise never be enterable.
app.router.routes.extend(_mcp_transport_routes)
app.add_middleware(_MCPAuthMiddleware)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
