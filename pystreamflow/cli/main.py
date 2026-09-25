import os
import typer
import asyncio
import sys
import json
import httpx
from pathlib import Path
from ..core.persistence import load_workflow
from ..core.engine import Engine

app = typer.Typer(
    help=(
        "PyStreamFlow CLI.\n\n"
        "Most commands talk to a running daemon (`pystreamflow daemon`) over "
        "its main HTTP API. That API requires an API key on every route "
        "except a handful of public ones (see docs/user_guide.md's "
        "Authentication section) - if the daemon's operator never set "
        "PSF_API_KEY, one was auto-generated on startup and logged/saved to "
        "data/api_key.txt. Pass it with --api-key on the affected commands, "
        "or set $PSF_API_KEY once in your shell so every command picks it "
        "up automatically. This is separate from $PSF_MCP_API_KEY, which "
        "only guards /mcp/* and is used by `run`'s auto-pipe mode and `pipe`."
    )
)


def _mcp_headers(api_key: str = None) -> dict:
    """Build the Authorization header for a /mcp/* request.

    Bug fix: the MCP HTTP server (pystreamflow/mcp/server.py) has
    supported (and, once PSF_MCP_API_KEY is set in the server's
    environment, required) Bearer/X-API-Key auth on every /mcp/* route
    since it was first written - but `run`'s auto-pipe path and the
    `pipe` command below have called POST /mcp/call with no headers at
    all since they were first written too, so as soon as anyone actually
    sets PSF_MCP_API_KEY (e.g. following this project's own "decide on
    API authentication" note), the CLI's one and only way of injecting
    data into a running daemon starts failing every single call with a
    flat 401, for every workflow, with no CLI option to fix it short of
    editing this file. `api_key` defaults to the same PSF_MCP_API_KEY env
    var the server itself reads, so a client and server started from the
    same environment (the common case) keep working with zero extra
    flags; --api-key on `pipe`/`run` overrides it for anything else
    (a key supplied out of band, a different value than the local shell's
    own environment, etc). Sends nothing when no key is available either
    way - unchanged behavior against a server that has no key configured.
    """
    key = api_key or os.environ.get("PSF_MCP_API_KEY", "")
    return {"Authorization": f"Bearer {key}"} if key else {}


def _api_headers(api_key: str = None) -> dict:
    """Build the Authorization header for a request against the main
    HTTP API (/nodes, /sessions, ...) - separate from _mcp_headers()
    above, which is only for /mcp/call.

    Bug fix (task: "add authentication"): the shared PSF_API_KEY auth
    added to api/server.py (see pystreamflow/core/auth.py) guards this
    whole surface *on by default* - unlike PSF_MCP_API_KEY, an operator
    doesn't need to opt in for it to be required - so `nodes`/`tail`/
    every `session_*` command below would otherwise start failing every
    call with a flat 401 the moment a real daemon is talking to. Same
    convention as _mcp_headers(): `api_key` defaults to the PSF_API_KEY
    env var the server itself reads (and auto-generates from, if unset),
    so a CLI run from the same environment as `pystreamflow daemon`
    keeps working with zero extra flags; --api-key overrides it for a
    key supplied out of band, e.g. one the server auto-generated and
    logged/saved to data/api_key.txt, since the CLI has no way to read a
    server-side auto-generated key on its own. Sends nothing when no key
    is available either way - unchanged behavior against a server that
    has auth disabled outright (e.g. a test double).
    """
    key = api_key or os.environ.get("PSF_API_KEY", "")
    return {"Authorization": f"Bearer {key}"} if key else {}

@app.command()
def run(workflow: str, api_key: str = typer.Option(None, help="Bearer key for the daemon's /mcp/call endpoint; defaults to $PSF_MCP_API_KEY.")):
    """Run a workflow headlessly in-process (no daemon needed).

    With no stdin piped in, this builds the graph from `workflow` and runs
    it directly via `Engine.run()` until Ctrl-C - nothing is sent over HTTP
    and no API key is needed for this path. If stdin *is* piped, this
    instead auto-pipes that data to the workflow's first Input node on an
    already-running daemon via `POST /mcp/call` - that path uses
    `--api-key`/$PSF_MCP_API_KEY, same as the `pipe` command below, since
    it goes through /mcp/*, not the main API.
    """
    path = Path(workflow)
    if not path.exists():
        typer.echo(f"Workflow file not found: {workflow}")
        raise typer.Exit(code=1)
    # Auto-pipe mode: if stdin is piped, forward to graph via API
    if not sys.stdin.isatty():
        import httpx
        data = sys.stdin.read()
        if data:
            # Find first input node
            graph = load_workflow(str(path))
            input_nodes = [n.id for n in graph.nodes if 'Input' in n.type]
            if input_nodes:
                node_id = input_nodes[0]
                try:
                    payload = json.loads(data)
                except:
                    payload = {"data": data}
                with httpx.Client(timeout=10.0) as client:
                    resp = client.post(
                        "http://localhost:8000/mcp/call",
                        json={"tool": "send_to_node", "arguments": {"node_id": node_id, "payload": payload}},
                        headers=_mcp_headers(api_key),
                    )
                    resp.raise_for_status()
                    typer.echo(f"Piped to node {node_id}: {resp.json()}")
                return
    graph = load_workflow(str(path))
    engine = Engine(graph)
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        typer.echo("Stopped")

@app.command()
def daemon(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Start the API server (the node editor, HTTP API, and /mcp all live here).

    On first run with no PSF_API_KEY set, a key is generated automatically
    and both logged here and saved to data/api_key.txt (or $PSF_API_KEY_FILE)
    - copy it from either place to use with --api-key on the other commands,
    or with the editor's own one-time key prompt in the browser.
    """
    typer.echo(f"Starting PyStreamFlow API daemon on {host}:{port}")
    import uvicorn
    from ..api.server import app as api_app
    uvicorn.run(api_app, host=host, port=port, reload=reload)

@app.command()
def nodes(
    api_url: str = "http://localhost:8000",
    kind: str = None,
    api_key: str = typer.Option(None, help="Bearer key for the daemon's main HTTP API; defaults to $PSF_API_KEY."),
):
    """List nodes by id, type and health. Filter by kind: input, output, modifier."""
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(f"{api_url}/nodes", headers=_api_headers(api_key))
            resp.raise_for_status()
            data = resp.json()
            for nid, info in data.items():
                t = info.get('type','')
                if kind:
                    if kind.lower()=='input' and 'Input' not in t: continue
                    if kind.lower()=='output' and 'Output' not in t: continue
                    if kind.lower()=='modifier' and 'Modifier' not in t and 'Grep' not in t and 'Merge' not in t and 'Fork' not in t: continue
                typer.echo(f"{nid}  {t}  health={info.get('health',{}).get('health')}")
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

@app.command()
def pipe(
    node_id: str,
    api_url: str = "http://localhost:8000",
    api_key: str = typer.Option(None, help="Bearer key for the daemon's /mcp/call endpoint; defaults to $PSF_MCP_API_KEY."),
):
    """Read stdin and pipe data to a node via API. If stdin is piped, auto-connect."""
    data = sys.stdin.read()
    if not data:
        typer.echo("No stdin data")
        raise typer.Exit(code=1)
    try:
        payload = json.loads(data)
    except:
        payload = {"data": data}
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{api_url}/mcp/call",
                json={"tool": "send_to_node", "arguments": {"node_id": node_id, "payload": payload}},
                headers=_mcp_headers(api_key),
            )
            resp.raise_for_status()
            typer.echo(f"Sent to {node_id}: {resp.json()}")
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

@app.command()
def tail(
    node_id: str,
    api_url: str = "http://localhost:8000",
    follow: bool = True,
    api_key: str = typer.Option(None, help="Bearer key for the daemon's main HTTP API; defaults to $PSF_API_KEY."),
):
    """Stream node output to stdout."""
    import time
    last_seen = 0
    while True:
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.get(f"{api_url}/nodes/{node_id}/last?n=100", headers=_api_headers(api_key))
                resp.raise_for_status()
                data = resp.json().get('last', [])
                for item in data[last_seen:]:
                    typer.echo(json.dumps(item))
                last_seen = len(data)
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
        if not follow:
            break
        time.sleep(1)

@app.command()
def validate(workflow: str):
    """Parse and validate a workflow YAML file locally (no daemon, no API key needed)."""
    path = Path(workflow)
    graph = load_workflow(str(path))
    engine = Engine(graph)
    try:
        engine.validate()
        typer.echo("Workflow valid")
    except Exception as e:
        typer.echo(f"Invalid: {e}")
        raise typer.Exit(code=1)

# Session management commands
#
# Bug fix (found while making sessions genuinely discoverable/controllable
# from every surface - UI/API/CLI/MCP - not just the API/MCP they already
# worked from): every command below used to import the in-process
# `core.session_manager` singleton directly and call its methods
# synchronously (via `asyncio.run(...)`) - but each CLI invocation is its
# own fresh Python process with its own empty `SessionManager()`. A
# session created by `session-create` lived only as long as that one
# process (which exits immediately after printing "Session created",
# killing any background task the session started); a *separate*
# `session-list` invocation moments later saw an empty list, every time,
# since it's yet another new process with its own blank session_manager -
# and neither one had any relationship at all to a real running `daemon`
# process's own sessions. These commands never worked as anything more
# than "create a session, immediately throw it away." Fixed by routing
# through HTTP to a real running daemon's /sessions endpoints, the same
# way `nodes`/`pipe`/`tail` above already do - now `session-create`
# genuinely persists on the daemon, and `session-list`/`session-start`/
# etc. from a separate CLI invocation (or the editor's Sessions picker,
# or an MCP client) all see and control the exact same sessions.
_API_KEY_OPTION = typer.Option(None, help="Bearer key for the daemon's main HTTP API; defaults to $PSF_API_KEY.")

@app.command()
def session_create(
    workflow: str,
    api_url: str = "http://localhost:8000",
    api_key: str = _API_KEY_OPTION,
):
    """Create a session on a running daemon from a workflow YAML path.

    `workflow` is resolved by the *daemon* process, not this CLI's own
    working directory - pass a path the daemon can actually read (typically
    an absolute path, or one relative to wherever `pystreamflow daemon` was
    started), same as POST /sessions always required.
    """
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(f"{api_url}/sessions", json={"workflow_path": workflow}, headers=_api_headers(api_key))
            resp.raise_for_status()
            data = resp.json()
            if 'error' in data:
                typer.echo(f"Error: {data['error']}")
                raise typer.Exit(code=1)
            typer.echo(f"Session created: {data['id']}")
    except httpx.HTTPError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

@app.command()
def session_list(api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """List every session on a running daemon, with its status and workflow path.

    The same sessions are visible from the editor UI's own Sessions picker
    and from the MCP `list_sessions` tool - all four surfaces (UI/API/CLI/
    MCP) read and control the exact same daemon-side sessions.
    """
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(f"{api_url}/sessions", headers=_api_headers(api_key))
            resp.raise_for_status()
            sessions = resp.json().get('sessions', [])
            if not sessions:
                typer.echo("No sessions.")
            for s in sessions:
                typer.echo(f"{s['id']}  {s['status']}  {s['workflow_path']}  nodes={s['nodes']} edges={s['edges']}")
    except httpx.HTTPError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

def _session_action(session_id: str, action: str, api_url: str, past_tense: str, api_key: str = None):
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(f"{api_url}/sessions/{session_id}/{action}", headers=_api_headers(api_key))
            resp.raise_for_status()
            data = resp.json()
            if 'error' in data:
                typer.echo(f"Session not found: {session_id}")
                raise typer.Exit(code=1)
            typer.echo(f"Session {session_id} {past_tense} (status={data.get('status')})")
    except httpx.HTTPError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

@app.command()
def session_start(session_id: str, api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """Start (or resume, if paused) a session created with `session-create`."""
    _session_action(session_id, 'start', api_url, 'started', api_key)

@app.command()
def session_stop(session_id: str, api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """Stop a running or paused session."""
    _session_action(session_id, 'stop', api_url, 'stopped', api_key)

@app.command()
def session_pause(session_id: str, api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """Pause a running session in place, without stopping it."""
    _session_action(session_id, 'pause', api_url, 'paused', api_key)

@app.command()
def session_resume(session_id: str, api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """Resume a paused session."""
    _session_action(session_id, 'resume', api_url, 'resumed', api_key)

@app.command()
def session_delete(session_id: str, api_url: str = "http://localhost:8000", api_key: str = _API_KEY_OPTION):
    """Stop (if needed) and permanently remove a session."""
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.delete(f"{api_url}/sessions/{session_id}", headers=_api_headers(api_key))
            resp.raise_for_status()
            data = resp.json()
            if not data.get('deleted'):
                typer.echo(f"Session not found: {session_id}")
                raise typer.Exit(code=1)
            typer.echo(f"Session {session_id} deleted")
    except httpx.HTTPError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
