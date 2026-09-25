try:
    from pydantic_settings import BaseSettings
except ImportError:
    # pydantic>=2 moved BaseSettings out to the pydantic-settings package.
    # Accessing pydantic.BaseSettings there doesn't raise ImportError (it
    # raises pydantic.errors.PydanticImportError via __getattr__), so the
    # fallback above never actually worked once pydantic-settings was
    # missing. Fall back to a tiny compatible shim instead.
    import os

    class BaseSettings:  # type: ignore[no-redef]
        """Minimal stand-in for pydantic_settings.BaseSettings.

        Reads declared class-level annotations from environment variables
        using each subclass's ``Config.env_prefix`` (default: no prefix),
        falling back to the annotated default value.
        """

        def __init__(self, **overrides):
            env_prefix = getattr(getattr(self, "Config", object), "env_prefix", "")
            for name, default in getattr(type(self), "__annotations__", {}).items():
                value = overrides.get(name, getattr(type(self), name, None))
                env_value = os.environ.get(f"{env_prefix}{name.upper()}")
                if env_value is not None:
                    caster = type(getattr(type(self), name, "")) or str
                    try:
                        if caster is bool:
                            value = env_value.strip().lower() in ("1", "true", "yes", "on")
                        else:
                            value = caster(env_value)
                    except (TypeError, ValueError):
                        value = env_value
                setattr(self, name, value)


class Settings(BaseSettings):
    debug: bool = False
    host: str = "127.0.0.1"
    port: int = 8000

    class Config:
        env_prefix = "PSF_"

settings = Settings()

# Compatibility alias for tests expecting Config
class Config:
    def __init__(self):
        self._data = {}
    def get(self, key, default=None):
        return self._data.get(key, default)
    def load(self):
        return True
