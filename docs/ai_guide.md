# PyStreamFlow AI Guide

## Purpose
Guide for AI agents to interact with PyStreamFlow via MCP and REST.

## System Overview
PyStreamFlow is a node graph stream router. Nodes process items asynchronously via pipes. Graphs are DAGs defined in YAML.

## Interaction Model
* **Observe**: list nodes, get stats, get last items
* **Control**: start/stop/pause nodes or sessions
* **Inject**: send data to input nodes
* **Configure**: update node parameters
* **Compose**: create sessions from workflows

## Key Concepts
* Node ID is stable string
* Each node has config dict
* Control actions: `start`, `stop`, `pause`, `resume` (start again on a paused node), `step`, `emit` (one-shot actions, not lifecycle changes), `reset` (clears a node's own stats/error bookkeeping - and, for a few node types with real accumulated state, their contents too - without stopping or reinitializing it)
* A node's `control` input accepts more than one wired source at once - two different controllers can each independently drive the same target node
* A regular data input port also accepts more than one wired source at once: calling `connect_nodes` a second time with the same `target`/`target_port` adds a second source rather than replacing the first, and the target receives items from both, interleaved in arrival order. There's no way to tell, from a single received item, which of several wired sources it came from - if that distinction matters, wire each source to a differently-named input instead of fanning two into one, or route them through a **MergeNode** built for combining sources under one visible node. `disconnect_nodes` removes only the one edge it's called for; any other edges into the same input keep delivering.
* Stats: items_in/out, bytes_in/out, rates, uptime
* Last items buffer for debugging

## Authentication
Every REST endpoint below except `GET /version` requires the daemon's main API key, sent as an `Authorization: Bearer <key>` or `X-API-Key: <key>` header. If the operator running the daemon didn't set `PSF_API_KEY`, one was auto-generated on startup - an agent that doesn't already have a key should ask its operator for it (it's logged on daemon startup and saved to `data/api_key.txt`) rather than guessing or retrying blindly; a `401` response means the key is missing or wrong, not that the target doesn't exist.

Everything under `/mcp/*` (the real MCP transports below, and the `/mcp/call` REST convenience endpoint) is exempt from this main key (it's a separate mounted app), but has its own, independent, **optional** key - `PSF_MCP_API_KEY` - which the daemon's operator may or may not have set. If they have, send it the same way (`Authorization: Bearer <key>` or `X-API-Key`) on the request itself; if they haven't, no header is needed to reach `/mcp/*` at all. `/mcp/health` always stays public either way. The two keys never need to be the same value and setting one has no effect on the other.

## Connecting a real MCP client (LM Studio, Claude Desktop, etc.)
`pystreamflow/mcp/server.py` is built on the official `mcp` Python SDK, so it's a genuinely spec-compliant MCP server - point a real MCP client at either of these transports and the usual `initialize` → `tools/list` → `tools/call` handshake just works:
* **Streamable HTTP** (the modern transport, and what most clients including LM Studio default to): `http://<host>:<port>/mcp` - a bare URL with no trailing slash works too, it 307-redirects to `/mcp/`.
* **Legacy SSE**: `http://<host>:<port>/mcp/sse` (opens the event stream; the client's outgoing calls then go to the `/mcp/messages/` endpoint the server's own handshake tells it to use - a real MCP client's transport handles this automatically, nothing to configure separately).

If `PSF_MCP_API_KEY` is set, configure it as the client's access token/bearer token; LM Studio's own "Access Token" field is exactly this. Both transports expose the full tool list below (`tools/list`), and errors from a failed tool call (e.g. calling a node that doesn't exist) come back as a real MCP tool-error result with the actual message, not a generic failure.

**A `403 Forbidden` on any `/mcp/*` request means a token *was* sent but doesn't match `PSF_MCP_API_KEY`** (a missing token is `401` instead) - almost always because the wrong key is configured client-side, not because auth is broken. The two most common causes: pasting the *main* `PSF_API_KEY` (the one this project auto-generates and logs) into the MCP client by mistake instead of the separate `PSF_MCP_API_KEY` - they are never the same value; or, for a docker-compose deployment, an un-customized `PSF_MCP_API_KEY` env var left at whatever placeholder the compose file shipped with. The daemon logs the exact `PSF_MCP_API_KEY` value it's currently enforcing (or that none is required) once at MCP-module startup - check that log line first before assuming anything else is wrong.

**A `421 Misdirected Request` on any `/mcp/*` request** means the underlying MCP SDK rejected the request's `Host` header before it ever reached this app's own auth check - this happens for any client connecting via a real LAN hostname (e.g. `mcp.lan`) or a reverse-proxy hostname, rather than `127.0.0.1`/`localhost`. By default this server disables the SDK's DNS-rebinding Host-header check entirely (it already has its own separate key-based auth via `PSF_MCP_API_KEY`, and every other route this project serves is reachable by any hostname too), so a stock deployment should never see a 421. If you've explicitly set `PSF_MCP_ALLOWED_HOSTS` to re-enable that check, a 421 means the hostname your client actually sent isn't in that comma-separated list - add it (matching host:port exactly) and restart.

## MCP Tools
**Media.** `get_node_last` and the reflection tools never return raw media: an image/audio/video item appears as a summary (`$media`, `mime`, `size`, `meta`, `ref`, `preview_url`). To actually look at one, call `get_media` with that `ref` (or the `preview_url`), or with a `node_id` for that node's newest media item - images (including video frames) come back as image content, shrunk to `max_side` (default 1024) to save tokens, audio as audio content. To feed media in, `send_to_node` accepts `{"$media": {"path": "files/x.jpg"}}` (only under the files/data directories or `PSF_MCP_MEDIA_ROOTS`), `{"$media": {"base64": "...", "mime": "image/png"}}` or `{"$media": {"ref": "..."}}`.

The tool set below is available three ways: over either real MCP transport above (for a real MCP client), or as a plain REST convenience endpoint at `POST /mcp/call` with a JSON body of `{"tool": "<name>", "arguments": {...}}` (not JSON-RPC - just `{"tool", "arguments"}` in, and either `{"result": ...}` or `{"error": ...}` back), which is the simpler option for a script or an HTTP-only agent that doesn't want to speak the full MCP protocol. Grouped by what the tools do:
* **Info**: `get_version`, `list_nodes`, `get_node_last`, `get_node_stats`, `get_node_config`
* **Inject / configure**: `send_to_node`, `update_node_config`
* **Media**: `get_media`
* **Node control**: `node_start`, `node_stop`, `node_pause`, `node_step`, `node_emit`, `node_reset`
* **Reflection**: `reflect_nodes`, `reflect_node`, `reflect_graph`
* **Graph editing**: `create_node`, `delete_node`, `connect_nodes`, `disconnect_nodes`
* **Sessions**: `create_session`, `list_sessions`, `get_session`, `start_session`, `stop_session`, `pause_session`, `resume_session`, `delete_session`, `list_session_nodes`, `get_session_node`

**Trying a real HTTP request against a workflow's own inbound/outbound node (`WebInputNode`, `ApiInputNode`, `WebOutputNode`, `WebOutputJSONNode`, `ApiOutputNode`) before falling back to `send_to_node`/`get_node_last`?** `get_node_config` and `reflect_node` both include an `http_endpoint` field for these five node types - `{"method", "path", "port", "note"}` (plus `raw_path`/`sse` where relevant) - giving you the real, effective route (e.g. `/api/demo/in`, not just the raw `uri: "demo/in"` config value) and the port it's actually served on. There's deliberately no `host` in that field: this daemon has no reliable way to know its own externally-visible hostname, so use whatever hostname/IP you used to reach *this* MCP connection - that's the same machine/container, just a different port (this node's own shared web server, not the main daemon/MCP port). Every other node type omits `http_endpoint` entirely.

## REST Endpoints
* `GET /nodes` – list with health
* `GET /nodes/{id}/stats` – throughput stats
* `GET /nodes/{id}/last?n=50` – recent items
* `PUT /nodes/{id}/config` – update config
* `POST /nodes/{id}/start|stop|pause|step|emit|reset`
* `GET /sessions` – list sessions
* `POST /sessions` – create session from workflow path
* `POST /sessions/{id}/start|stop|pause|resume`, `DELETE /sessions/{id}`

## Best Practices
* Always check node health before injecting
* Use `get_node_last` to verify data flow
* Use Trigger nodes for conditional control
* Batch config updates
* Respect backpressure – Pipe maxsize

## Example Workflow for AI
1. `list_nodes` → find input node id
2. `send_to_node` with test payload
3. `get_node_last` on downstream nodes to verify processing
4. `get_node_last` on output node for results
5. Adjust node config via `PUT /nodes/{id}/config` if needed

## Error Handling
* Node not found → check `list_nodes`
* Config validation → node will log error, health becomes `error`
* Graph cycles → engine validation fails on session load

This guide enables AI agents to safely operate PyStreamFlow workflows.
