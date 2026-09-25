"""Shared API-key authentication for the main HTTP API + editor UI
(``pystreamflow/api/server.py``), added alongside the pre-existing,
separate ``PSF_MCP_API_KEY`` mechanism in ``pystreamflow/mcp/server.py``.

Why a second, separate mechanism rather than reusing the MCP one: the two
have deliberately different default postures. ``PSF_MCP_API_KEY`` is
opt-in - unset means "no auth at all" - because it predates this feature
and changing that default retroactively would silently lock out every
already-configured MCP deployment the next time this project is
upgraded. The main API/editor surface has no such installed base yet, so
this one is on by default: if ``PSF_API_KEY`` isn't set, a random key is
generated on first startup and persisted so the operator can retrieve it
and actually use the app, but the app is never reachable un-authenticated
just because nobody set an env var. ``/mcp/*`` keeps using its own
``require_auth()`` unchanged - this module never touches it.
"""
import logging
import os
import secrets

from .paths import data_dir, ensure_dir

logger = logging.getLogger("pystreamflow.auth")

# Process-wide cache so repeated calls (every request, via the FastAPI
# dependency/middleware below) don't re-read-or-generate the key file each
# time, and so a key generated on first use stays stable for the rest of
# this process's life even if the underlying file were somehow touched.
_cached_key: str | None = None


def _key_file_path() -> str:
    """Where a generated key is persisted, so an operator who lost the
    startup log line can still recover it without regenerating (which
    would invalidate every client/editor session already using the old
    one). Overridable independently of PSF_DATA_DIR via PSF_API_KEY_FILE
    for deployments that want it somewhere other than the data dir
    (e.g. a secrets mount).
    """
    override = os.environ.get('PSF_API_KEY_FILE')
    if override:
        return override
    return os.path.join(data_dir(), 'api_key.txt')


def get_or_create_api_key() -> str:
    """Return the API key that guards the main HTTP API + editor UI.

    Precedence: ``PSF_API_KEY`` env var always wins (and is never
    persisted to disk - an operator managing it via their own env/secrets
    system shouldn't have this module also scatter a copy into the data
    dir). Otherwise, reuse a previously-generated key from the key file
    if one exists (so restarting the daemon doesn't invalidate every
    already-configured client/editor by handing out a new random key
    every time), and only generate+persist a brand new one if neither is
    available.
    """
    global _cached_key
    if _cached_key is not None:
        return _cached_key

    env_key = os.environ.get('PSF_API_KEY')
    if env_key:
        _cached_key = env_key
        return _cached_key

    path = _key_file_path()
    try:
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8') as f:
                existing = f.read().strip()
            if existing:
                _cached_key = existing
                return _cached_key
    except OSError:
        pass

    new_key = secrets.token_urlsafe(32)
    try:
        ensure_dir(os.path.dirname(path) or '.')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_key)
    except OSError as e:
        # Persisting failed (read-only filesystem, etc). Still usable for
        # this process's lifetime - just won't survive a restart, and the
        # operator needs to grab it from the log line below every time.
        logger.warning("could not persist generated API key to %s: %s", path, e)

    logger.warning(
        "No PSF_API_KEY set - generated a new API key and saved it to %s. "
        "Use it as a Bearer token or X-API-Key header, or set PSF_API_KEY "
        "yourself to control it directly. Generated key: %s",
        path, new_key,
    )
    _cached_key = new_key
    return _cached_key


def check_api_key(authorization: str | None, x_api_key: str | None) -> bool:
    """Pure check against the current key, shared by the middleware
    (api/server.py) and anything else (tests, future routes) that wants
    to validate a request's credentials without depending on FastAPI's
    Header()-injection machinery. Mirrors mcp/server.py's require_auth()
    header-parsing shape (Bearer <key> or X-API-Key).
    """
    expected = get_or_create_api_key()
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
    elif x_api_key:
        token = x_api_key
    return token is not None and token == expected


def reset_cached_key() -> None:
    """Test-only escape hatch: clears the in-process cache so a test can
    change PSF_API_KEY/PSF_API_KEY_FILE and observe the new value, rather
    than being stuck with whatever the first call in the test process
    happened to cache.
    """
    global _cached_key
    _cached_key = None
