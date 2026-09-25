"""
Coverage for pystreamflow/core/config.py - previously 45% covered.
`pydantic_settings` is installed in this environment, so the real
`Settings` class (env-driven via the PSF_ prefix) is what actually runs
in production here; the `except ImportError:` fallback shim class only
ever executes when pydantic_settings is missing, so it's exercised by
temporarily forcing that import to fail (via sys.modules) and reloading
the module - nothing else in the codebase imports pystreamflow.core.config
(verified: it's a leaf module), so reloading it here can't leak stale
state into anything else, and the module is reloaded back to its normal
state afterward regardless of test outcome.
"""
import importlib
import sys

from pystreamflow.core.config import Config, Settings, settings


def test_settings_defaults():
    assert settings.debug is False
    assert settings.host == '127.0.0.1'
    assert settings.port == 8000


def test_settings_reads_env_with_prefix(monkeypatch):
    monkeypatch.setenv('PSF_HOST', '0.0.0.0')
    monkeypatch.setenv('PSF_PORT', '9090')
    monkeypatch.setenv('PSF_DEBUG', 'true')
    s = Settings()
    assert s.host == '0.0.0.0'
    assert s.port == 9090
    assert s.debug is True


def test_config_compatibility_alias():
    cfg = Config()
    assert cfg.get('missing', 'default') == 'default'
    assert cfg.load() is True


def test_fallback_shim_used_when_pydantic_settings_is_unavailable(monkeypatch):
    monkeypatch.setitem(sys.modules, 'pydantic_settings', None)
    import pystreamflow.core.config as config_module
    try:
        reloaded = importlib.reload(config_module)
        # The fallback shim's __init__ reads declared annotations from
        # PSF_-prefixed env vars, casting to each field's declared type,
        # and falls back to the class-level default otherwise.
        assert reloaded.settings.host == '127.0.0.1'
        assert reloaded.settings.port == 8000
        assert reloaded.settings.debug is False

        monkeypatch.setenv('PSF_PORT', '1234')
        monkeypatch.setenv('PSF_DEBUG', 'yes')
        overridden = reloaded.Settings()
        assert overridden.port == 1234
        assert overridden.debug is True

        # A non-castable env value falls back to the raw string rather
        # than crashing (the shim's `except (TypeError, ValueError):`).
        monkeypatch.setenv('PSF_PORT', 'not-an-int')
        fallback_value = reloaded.Settings()
        assert fallback_value.port == 'not-an-int'
    finally:
        # Restore the real pydantic_settings-backed module for every test
        # that runs after this one in the same session.
        monkeypatch.undo()
        importlib.reload(config_module)
