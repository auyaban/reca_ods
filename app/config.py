from functools import lru_cache
import os

from dotenv import load_dotenv
from pathlib import Path

from app.paths import app_data_dir
from app.utils.cache import ttl_bucket

_ENV_PATH = app_data_dir() / ".env"
_SETTINGS_CACHE_TTL_SECONDS = 300


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


class Settings:
    def __init__(self) -> None:
        self.supabase_url = _clean_env(os.getenv("SUPABASE_URL", ""))
        self.supabase_anon_key = _clean_env(os.getenv("SUPABASE_ANON_KEY", ""))
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
