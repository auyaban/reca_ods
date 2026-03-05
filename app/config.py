from functools import lru_cache
import os

from dotenv import load_dotenv
from pathlib import Path

from app.paths import app_data_dir
from app.utils.cache import ttl_bucket

_ENV_PATH = app_data_dir() / ".env"
_SETTINGS_CACHE_TTL_SECONDS = 300
_DEFAULT_SUPABASE_AUTH_EMAIL = "test@reca.local"
_DEFAULT_SUPABASE_AUTH_PASSWORD = "Reca.Test.2026!v3"
_LEGACY_SUPABASE_AUTH_PASSWORDS = {
    "Reca.Test.2026!",
}


def _load_env() -> None:
    load_dotenv(dotenv_path=_ENV_PATH, override=True)
    if not _ENV_PATH.exists():
        fallback = Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv_path=fallback, override=False)


_load_env()


def _clean_env(value: str) -> str:
    clean = value.strip()
    if (clean.startswith('"') and clean.endswith('"')) or (
        clean.startswith("'") and clean.endswith("'")
    ):
        clean = clean[1:-1].strip()
    return clean


def _env_or_default(key: str, default: str) -> str:
    raw = os.getenv(key)
    if raw is None:
        return default
    clean = _clean_env(raw)
    if clean == "":
        return default
    if key == "SUPABASE_AUTH_PASSWORD" and clean in _LEGACY_SUPABASE_AUTH_PASSWORDS:
        return _DEFAULT_SUPABASE_AUTH_PASSWORD
    return clean


class Settings:
    def __init__(self) -> None:
        self.supabase_url = _clean_env(os.getenv("SUPABASE_URL", ""))
        self.supabase_anon_key = _clean_env(os.getenv("SUPABASE_ANON_KEY", ""))
        self.supabase_auth_email = _env_or_default(
            "SUPABASE_AUTH_EMAIL", _DEFAULT_SUPABASE_AUTH_EMAIL
        )
        self.supabase_auth_password = _env_or_default(
            "SUPABASE_AUTH_PASSWORD", _DEFAULT_SUPABASE_AUTH_PASSWORD
        )
        self.supabase_rpc_terminar_servicio = _clean_env(
            os.getenv("SUPABASE_RPC_TERMINAR_SERVICIO", "")
        )


@lru_cache
def _get_settings_cached(_ttl_bucket: int) -> Settings:
    _load_env()
    return Settings()


def get_settings() -> Settings:
    return _get_settings_cached(ttl_bucket(_SETTINGS_CACHE_TTL_SECONDS))


def clear_settings_cache(reload_env: bool = True) -> None:
    _get_settings_cached.cache_clear()
    if reload_env:
        _load_env()
