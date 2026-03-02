from functools import lru_cache

from supabase import create_client, Client

from app.config import get_settings
from app.utils.cache import ttl_bucket

_SUPABASE_CLIENT_CACHE_TTL_SECONDS = 300

@lru_cache
def _get_supabase_client_cached(
    supabase_url: str, supabase_anon_key: str, _ttl_bucket: int
) -> Client:
    if not supabase_url or not supabase_anon_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY")
    return create_client(supabase_url, supabase_anon_key)


def get_supabase_client() -> Client:
    settings = get_settings()
    return _get_supabase_client_cached(
        settings.supabase_url,
        settings.supabase_anon_key,
        ttl_bucket(_SUPABASE_CLIENT_CACHE_TTL_SECONDS),
    )


def clear_supabase_client_cache() -> None:
    _get_supabase_client_cached.cache_clear()
