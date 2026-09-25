"""Coverage for the new shared API-key auth (task: "add authentication"):

- pystreamflow/core/auth.py's get_or_create_api_key()/check_api_key()
  (env var precedence, generate+persist+reuse when unset).
- pystreamflow/api/server.py's _require_api_key middleware wired from
  it: unauthenticated/wrong-key requests rejected on the protected
  surface, the editor shell/static assets/health/version/mcp/api stay
  reachable with no key at all.

Every test here that touches core.auth's process-wide `_cached_key` (the
same cache api/server.py's middleware reads on every request, for the
one real `app` object the whole test session shares) restores it via the
`_isolated_auth_cache` fixture, so it never leaks a different key into
every other test module's already-created TestClient instances.
"""
import os

import pytest
from conftest import TEST_API_KEY
from fastapi.testclient import TestClient

from pystreamflow.api import server as api_server_module
from pystreamflow.core import auth as auth_module

_AUTH_HEADERS = {"Authorization": f"Bearer {TEST_API_KEY}"}


@pytest.fixture
def _isolated_auth_cache(tmp_path, monkeypatch):
    """Give a test a clean slate to exercise get_or_create_api_key()'s
    generate/persist/reuse logic, then put everything back exactly as
    the rest of the suite (and its already-built `app`/TestClient
    objects) expects it: PSF_API_KEY back to TEST_API_KEY and the
    process-wide cache re-populated with that same value.
    """
    monkeypatch.delenv("PSF_API_KEY", raising=False)
    monkeypatch.setenv("PSF_API_KEY_FILE", str(tmp_path / "api_key.txt"))
    auth_module.reset_cached_key()
    try:
        yield tmp_path
    finally:
        monkeypatch.setenv("PSF_API_KEY", TEST_API_KEY)
        auth_module.reset_cached_key()
        assert auth_module.get_or_create_api_key() == TEST_API_KEY


def test_env_var_takes_precedence_and_is_not_persisted(_isolated_auth_cache, monkeypatch):
    monkeypatch.setenv("PSF_API_KEY", "from-env-var")
    key = auth_module.get_or_create_api_key()
    assert key == "from-env-var"
    assert not os.path.exists(auth_module._key_file_path())


def test_generates_and_persists_a_key_when_unset(_isolated_auth_cache):
    key = auth_module.get_or_create_api_key()
    assert len(key) > 20
    path = auth_module._key_file_path()
    assert os.path.isfile(path)
    with open(path, encoding="utf-8") as f:
        assert f.read().strip() == key


def test_reuses_persisted_key_across_a_restart_instead_of_regenerating(_isolated_auth_cache):
    first = auth_module.get_or_create_api_key()
    # Simulate a fresh process: clear only the in-memory cache, leaving
    # the on-disk file (and still no PSF_API_KEY env var) exactly as a
    # real restart would find them.
    auth_module.reset_cached_key()
    second = auth_module.get_or_create_api_key()
    assert first == second


def test_check_api_key_accepts_bearer_and_x_api_key_headers(_isolated_auth_cache, monkeypatch):
    monkeypatch.setenv("PSF_API_KEY", "the-real-key")
    auth_module.reset_cached_key()
    assert auth_module.check_api_key("Bearer the-real-key", None) is True
    assert auth_module.check_api_key(None, "the-real-key") is True
    assert auth_module.check_api_key("Bearer wrong", None) is False
    assert auth_module.check_api_key(None, "wrong") is False
    assert auth_module.check_api_key(None, None) is False
    # A bare token with no "Bearer " prefix on the Authorization header
    # is not accepted - only a well-formed Bearer value or X-API-Key.
    assert auth_module.check_api_key("the-real-key", None) is False


def test_protected_route_rejects_missing_or_wrong_key():
    client = TestClient(api_server_module.app)
    r = client.get("/nodes")
    assert r.status_code == 401
    assert "error" in r.json()

    r = client.get("/nodes", headers={"X-API-Key": "definitely-wrong"})
    assert r.status_code == 401


def test_protected_route_accepts_the_configured_key():
    client = TestClient(api_server_module.app, headers=_AUTH_HEADERS)
    r = client.get("/nodes")
    assert r.status_code == 200

    # X-API-Key works exactly like Bearer.
    client2 = TestClient(api_server_module.app, headers={"X-API-Key": TEST_API_KEY})
    r2 = client2.get("/nodes")
    assert r2.status_code == 200


def test_editor_shell_and_ops_routes_stay_public_with_no_key():
    client = TestClient(api_server_module.app)
    for path in ("/", "/ui", "/health", "/version"):
        r = client.get(path)
        assert r.status_code == 200, f"{path} should not require auth"


def test_static_assets_stay_public_with_no_key():
    client = TestClient(api_server_module.app)
    r = client.get("/static/editor.js")
    assert r.status_code == 200


def test_dynamic_api_node_routes_are_exempt_from_this_apps_key():
    # /api/* belongs to ApiInputNode/ApiOutputNode (core.web_server's
    # mounted-at-"/" catch-all), not this app's own admin surface - see
    # the long comment above _AUTH_EXEMPT_PREFIXES in api/server.py.
    # Nothing is registered under /api/* here, so this just confirms the
    # request reaches routing (a plain 404 from the fallback app) rather
    # than being turned away with a 401 before it gets there.
    client = TestClient(api_server_module.app)
    r = client.get("/api/nonexistent-demo-route")
    assert r.status_code != 401
