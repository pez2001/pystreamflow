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
