"""Shared pytest setup.

Sets a fixed PSF_API_KEY before anything imports pystreamflow.api.server,
so the new shared-API-key auth middleware (pystreamflow/core/auth.py)
has a stable, known key for the whole test session instead of
generating (and persisting to data_dir()/api_key.txt) a random one on
first import - which would make every existing TestClient(app) call
against the protected surface start failing with 401, and would litter
the repo/test run with a generated key file. conftest.py is imported by
pytest before it collects any test module in this directory, so this
env var is guaranteed to be set before the first `from
pystreamflow.api.server import app` anywhere in the suite.

Test files that hit the authenticated surface (test_api_server_coverage.py,
test_api_coverage.py) import TEST_API_KEY from here and pass it as a
Bearer token on their TestClient instances. Nothing else needs to change
this value - core.web_server.app and mcp.server.app are separate FastAPI
apps untouched by this middleware, and /mcp/* is explicitly exempt from
it even when reached through the full pystreamflow.api.server app.

Also provides the `live_mcp_server`/`live_mcp_server_with_key` fixtures
(a real pystreamflow.mcp.server run as its own OS subprocess) shared by
every test module that needs to drive the real MCP transports end-to-end
- see _spawn_live_mcp_server()'s own docstring for exactly why this has
to be a real subprocess rather than an in-process server or ASGI
transport. Defined here (not in any one test file) so that
test_mcp_server_coverage.py and test_new_feature_nodes.py (MCPClientNode's
"connects to this project's own real server" test) can each request their
own independent instance without fighting over the same in-process
mcp/server.py module singleton - two test files importing that module in
the same pytest process share the exact same StreamableHTTPSessionManager
object, whose .run() the SDK documents as callable "only ... once per
instance", so any second in-process attempt to enter its lifespan (from
either file) would permanently fail the whole run. A subprocess per
fixture instantiation sidesteps that entirely.
"""
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

TEST_API_KEY = "test-api-key-for-pytest"
os.environ.setdefault("PSF_API_KEY", TEST_API_KEY)


def _spawn_live_mcp_server(extra_env=None):
    """Starts a real `python -m uvicorn pystreamflow.mcp.server:app` in its
    own OS subprocess on a free 127.0.0.1 port, waits for /health to
    answer, and returns (process, base_url).

    A real subprocess - not an in-process thread - for two independent
    reasons found while building this fixture:

    1. The SDK's StreamableHTTPSessionManager (created once, at import
       time, as part of mcp/server.py's module-level `app`) explicitly
       documents that its .run() "can only be called once per instance -
       Create a new instance if you need to run again", and never resets
       that guard even after a clean shutdown. test_mcp_server_coverage.py's
       own test_session_lifecycle_via_tools uses `with TestClient(app) as
       c:` (needed to keep a session's background engine task alive
       across calls) - which itself enters that exact app's lifespan
       once, permanently exhausting the guard for the rest of the
       process. An in-process live server sharing that same `app`
       singleton (whether via a background thread or otherwise, and
       whether started from this file or another one importing the same
       module in the same pytest run) would therefore always collide
       with that test - whichever of the two runs first "wins" and the
       other fails with the SDK's RuntimeError, no matter the ordering.
       A subprocess gets its own fresh Python process and therefore its
       own fresh, never-yet-started StreamableHTTPSessionManager,
       sidestepping the shared-singleton conflict entirely.
    2. uvicorn.Server installs SIGINT/SIGTERM handlers by default, which
       only works from the main thread of a process - a background-thread
       approach needs that suppressed; a subprocess needs no such
       workaround since it has its own main thread.

    This also happens to be a more faithful test: it's exactly how LM
    Studio (a separate process) actually talks to this server, not an
    in-process shortcut.

    Deliberately does NOT use an in-process ASGI transport (e.g.
    httpx2.ASGITransport with a fake base_url like "http://testserver")
    either: that combination was tried while building this fix and
    tripped an "Invalid Host header" / 421 Misdirected Request rejection -
    traced to the SDK's low-level Server.streamable_http_app()
    auto-enabling DNS-rebinding protection whenever host is a loopback
    address (its default), which only allow-lists real 127.0.0.1/localhost
    Host headers, not synthetic ones like "testserver". A real loopback
    socket naturally satisfies that check, matching every live daemon
    check done while building this fix (including the exact bug report's
    own mcp.lan:8000/mcp URL shape, which resolves to a real host:port).
    (A fake in-process MCP server built purely for a test, with
    transport_security explicitly disabled, is a different matter and is
    used elsewhere for tests that don't need to hit this project's own
    real server - see test_new_feature_nodes.py's `fake_mcp_app` fixture.)
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "pystreamflow.mcp.server:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 10.0
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            output = proc.stdout.read().decode(errors="replace")
            raise RuntimeError(f"live MCP test server exited early (code {proc.returncode}):\n{output}")
        try:
            r = httpx.get(f"{base_url}/health", timeout=0.5)
            if r.status_code == 200:
                return proc, base_url
        except httpx.HTTPError as exc:
            last_error = exc
        time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f"live MCP test server failed to become healthy in time: {last_error}")


@pytest.fixture(scope='module')
def live_mcp_server():
    """See _spawn_live_mcp_server() for why this is a real subprocess.
    Module-scoped: starting uvicorn is not free, and every test using this
    fixture only needs an unauthenticated server (PSF_MCP_API_KEY unset),
    so one shared instance per test module is both faster and sufficient
    - unlike live_mcp_server_with_key below, which needs its own
    subprocess per test with a specific PSF_MCP_API_KEY set at startup.
    """
    proc, base_url = _spawn_live_mcp_server()
    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)


@pytest.fixture
def live_mcp_server_with_key():
    """A separate, function-scoped live server with PSF_MCP_API_KEY set at
    startup (MCP_API_KEY is read from that env var once, at module import
    time, inside the subprocess - it can't be monkeypatched after the fact
    the way the in-process require_auth()/_MCPAuthMiddleware unit tests
    do, since this server lives in a different process entirely).
    """
    proc, base_url = _spawn_live_mcp_server(extra_env={'PSF_MCP_API_KEY': 'sse-fixture-secret'})
    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)
