# PyStreamFlow Developer Guide

## Architecture Overview
PyStreamFlow is a Python node graph engine for stream processing.

* **Core**: `pystreamflow/core`
  * `node.py` – `BaseNode` with lifecycle, stats, control
  * `stream.py` – `Pipe`, `Fork`, `Merge` primitives
  * `models.py` – `Node`, `Edge`, `Graph`
  * `engine.py` – validation, topological sort, instantiation
  * `persistence.py` – YAML load/save
  * `session_manager.py` – headless sessions
  * `plugin_manager.py` – dynamic node plugins
  * `web_server.py` – shared web input server

* **Nodes**: `pystreamflow/nodes`
  * Input, Output, Modifier, Logic, Numeric, Text, Trigger, etc.
  * All inherit `BaseNode`

* **API**: `pystreamflow/api/server.py`
  * FastAPI REST + UI
  * MCP server mounted at `/mcp`
  * A middleware requires an API key (`pystreamflow/core/auth.py`) on every route except the editor's own page/static assets, `/health`, `/version`, `/mcp/*` (its own separate, opt-in key), and a workflow's own `/api/*` routes - see "Authentication" below

* **CLI**: `pystreamflow/cli/main.py`
  * Typer commands: run, daemon, pipe, tail, nodes, validate, session-create/list/start/pause/resume/stop/delete
  * `pystreamflow <command> --help` is the source of truth for each command's flags; most commands that hit the daemon's HTTP API take `--api-key` (see "Authentication" below)

## Adding a Node
1. Create `pystreamflow/nodes/my_node.py`
```python
from ..core.node import BaseNode
class MyNode(BaseNode):
    async def process(self):
        while self._running:
            pipe = next(iter(self.inputs.values()), None)
            if not pipe: await asyncio.sleep(0.2); continue
            item = await pipe.get()
            self.emit('out', item)
```
2. Export in `nodes/__init__.py`
3. Add to engine registry in `core/engine.py`
4. Add UI entry in `api/ui.html` nodeTypes / nodeGroups / nodeDocs

## Node Lifecycle
* `init()` – one-time setup, re-run by `start()` whenever `_initialized` is `False`: true for a brand new instance, and true again after `stop()` (see below) - so restarting a *stopped* node genuinely starts fresh, while `resume()`-ing a *paused* one (which never clears `_initialized`) picks up exactly where `process()` left off, state and all
* `start()` – begins processing, sets `_start_ts` for stats
* `process()` – main loop, must `await pipe.get()` and `emit()`
* `stop()` – cancels the loop and clears `_initialized`, so a later `start()` redoes `init()` from scratch rather than silently resuming with whatever state was left behind (a real, previously-shipped bug: a node whose `process()` naturally finishes on its own, e.g. `GeneratorInputNode` after emitting its configured `count`, could never be usefully restarted via stop()+start() - only a full graph "Run", which always builds fresh node instances, actually worked - and `reset()` didn't help either, since it's deliberately scoped to generic bookkeeping, not one-time setup or node-specific state like a generator's own counter)
* `handle_control(msg)` – responds to `{'action': ...}` messages: `start`/`stop`/`pause`/`resume` (lifecycle transitions), `step`/`emit` (one-shot actions that don't change lifecycle state), and `reset` (clears `BaseNode`'s own stats/error bookkeeping in place, without stopping/restarting or re-running `init()` - override `reset()` and call `await super().reset()` first if your node type accumulates its own state beyond the generic counters, e.g. `StackNode`/`TableNode`/`LineBufferNode`)
* A node's `control` input supports fan-in: `self._control_pipes` is a list, with one independent listener per wired source, so more than one Trigger-family node can control the same target at once without either silently stealing the connection from the other
* Regular data inputs (`self.inputs`) support fan-in the same way: `add_input(name, pipe)` appends to `self._input_sources[name]` rather than overwriting. A name with exactly one source is still wrapped and stored directly in `self.inputs[name]` (zero overhead, unchanged from before this feature); once a second source is wired to the same name, `self.inputs[name]` becomes a `core/stream.FanInPipe` that transparently merges every wired source behind the same `get()`/`put()`/`stats()` surface a plain `Pipe` exposes - so an existing node's `process()` loop (almost all of which just do `pipe = self.inputs.get('in'); item = await pipe.get()`) needs zero changes to correctly receive from more than one source. `remove_input(name, pipe)` removes exactly one source (used by `disconnect_nodes` in both the HTTP and MCP servers) and collapses back down to the un-merged fast path the moment a name drops back to one remaining source. `add_input_if_unwired()`'s own placeholder pipe (for node types that create their own pipe in `init()` before real wiring arrives) is evicted, not fanned in alongside, the first real wire that arrives.

## Stats
BaseNode tracks `items_in`, `items_out`, `bytes_in`, `bytes_out`, uptime and rates. Exposed via `/nodes/{id}/stats` and `health()`.

## Media Items (Images, Audio, Video)
Phase 1 of `docs/plans/media_types_plan.md`. Media travels through the graph as a `MediaItem` (`pystreamflow/core/media.py`), never as bare `bytes`:

```python
from pystreamflow.core.media import MediaItem

item = await MediaItem.afrom_bytes(data, meta={"source_path": path})  # MIME sniffed from magic bytes
self.emit("out", item)
...
payload = await item.aget_bytes()   # works for inline and blob-stored items
```

- Payloads up to `PSF_MEDIA_INLINE_MAX_MB` (default 2) stay inline; larger ones go to the content-addressed blob store (`core/blob_store.py`, `<PSF_DATA_DIR>/blobs` or `PSF_BLOB_DIR`), and only the SHA-256 `ref` travels through pipes. Blobs expire `PSF_BLOB_TTL_S` (600) seconds after their last read/write. The store is capped at `PSF_BLOB_MAX_MB` (1024) with LRU eviction, and a background task cleans it up every `PSF_BLOB_CLEANUP_INTERVAL_S` (60) seconds.
- `str(item)` is a short description like `<image/png 1920x1080 3.1MB>`, so text nodes stay readable.
- `BaseNode` counts `bytes_in`/`bytes_out` by payload size (never via `str(bytes)`). Its live-view history keeps raw binary above `max_last_item_bytes` (node config, or `PSF_LAST_ITEM_MAX_BYTES`, default 64 KiB) only as `{kind: binary, size, head}`, while `manual_emit()` still replays the real last item per port.
- Everything returned by `/nodes/{id}/last`, `/reflection/*`, `/sessions/{id}/nodes/{id}` and the MCP tools goes through `to_jsonable()`: bytes become `{"$binary": size, "head": "<hex>"}`, and MediaItems become their `summary()` plus a `preview_url` pointing at `GET /media/{ref}` (API-key protected, supports `Range`).
- The editor's live view (sidebar and the "Live view…" modal) finds these summaries anywhere in a node's last items and previews them as `<img>`/`<audio>`/`<video>`. It fetches `preview_url` through its API-key-injecting `fetch` wrapper and shows the result as a `blob:` URL, because an `<img src>` can't send the auth header. `video_frame`/`audio_chunk` streams follow the newest item on every poll, with a pause button.
- Nodes that read or write media use the helpers rather than re-implementing them: `nodes/media_file_input.py`'s `load_media_file()` (whole file → `MediaItem`, run it via `asyncio.to_thread`), `core/media.py`'s `guess_mime()`/`extension_for_mime()`, and `core/media_http.py` for HTTP (`parse_request()` for uploads, `media_response()`/`register_media_route()` to serve an item with its Content-Type and `Range` support, `json_payload()` for JSON/SSE output). The node-facing side is documented under "Media Nodes" in `docs/nodes/index.md`.
- Backpressure: a node class declares media output ports with `MEDIA_OUTPUT_PORTS = {"frames": "drop_oldest"}`. Edges leaving such a port get a bounded queue (`PSF_MEDIA_EDGE_MAXSIZE`, default 8) with that drop policy (`block`, `drop`, `drop_oldest` or `raise`; see `core/stream.py`). Any edge can override it in the workflow YAML with `buffer: {maxsize: 4, drop_policy: drop}`, and `POST /nodes/connect` accepts the same `buffer` field. Use `await self.emit_wait(port, item)` in high-rate producers so a `block` edge actually slows them down. Drops show up in `stats()` and as `pystreamflow_pipe_dropped_total` in Prometheus, next to `pystreamflow_blob_store_bytes`/`_blobs`.

## Authentication
`pystreamflow/core/auth.py`'s `get_or_create_api_key()` is the single source of truth for the main API's key: `PSF_API_KEY` env var if set, otherwise a generated key persisted to `data_dir()/api_key.txt` (or `$PSF_API_KEY_FILE`) and reused across restarts. `check_api_key()` validates an `Authorization: Bearer <key>` or `X-API-Key` header against it - `api/server.py`'s `_require_api_key` middleware calls this on every request except the small exemption list defined right above it (`_AUTH_EXEMPT_PATHS`/`_AUTH_EXEMPT_PREFIXES`) - extend that list, not the auth module itself, if a future route needs to be public. This is deliberately a separate mechanism from `pystreamflow/mcp/server.py`'s own `PSF_MCP_API_KEY`/`require_auth()`, which stays opt-in (unset means no auth) so this project's auth history doesn't retroactively change behavior for an existing MCP deployment - don't merge the two.

## Testing
Tests in `tests/`. Run with Python directly:
```
python tests/test_coverage.py
python tests/test_nodes_coverage.py
```
Coverage target ≥50%.

## Development Workflow
1. Edit code
2. Run `docker compose up -d` for integration
3. UI at http://localhost:8000/ui
4. API docs at http://localhost:8000/docs

## Packaging
`pyproject.toml` defines build. Docker multi-stage build in `Dockerfile`.
