from functools import lru_cache
import os

from dotenv import load_dotenv
from pathlib import Path

from app.paths import app_data_dir

_ENV_PATH = app_data_dir() / ".env"
load_dotenv(dotenv_path=_ENV_PATH, override=True)
if not _ENV_PATH.exists():
    fallback = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=fallback, override=False)


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
def get_settings() -> Settings:
    return Settings()
