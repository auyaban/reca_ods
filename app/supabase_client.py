from functools import lru_cache

from supabase import create_client, Client

from app.config import get_settings
from app.utils.cache import ttl_bucket

_SUPABASE_CLIENT_CACHE_TTL_SECONDS = 300


def _ensure_authenticated(client: Client, email: str, password: str) -> None:
    session = client.auth.get_session()
    if session and session.access_token:
        return

    response = client.auth.sign_in_with_password(
        {
            "email": email,
            "password": password,
        }
    )
    session = response.session or client.auth.get_session()
    if not session or not session.access_token:
        raise RuntimeError("Supabase authentication failed: missing access token")


@lru_cache
def _get_supabase_client_cached(
    supabase_url: str,
    supabase_anon_key: str,
    supabase_auth_email: str,
    supabase_auth_password: str,
    _ttl_bucket: int,
) -> Client:
    if not supabase_url or not supabase_anon_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY")
    if not supabase_auth_email or not supabase_auth_password:
        raise RuntimeError("Missing SUPABASE_AUTH_EMAIL or SUPABASE_AUTH_PASSWORD")

    client = create_client(supabase_url, supabase_anon_key)
    _ensure_authenticated(client, supabase_auth_email, supabase_auth_password)
    return client


def get_supabase_client() -> Client:
    settings = get_settings()
    return _get_supabase_client_cached(
        settings.supabase_url,
        settings.supabase_anon_key,
        settings.supabase_auth_email,
        settings.supabase_auth_password,
        ttl_bucket(_SUPABASE_CLIENT_CACHE_TTL_SECONDS),
    )


def clear_supabase_client_cache() -> None:
    _get_supabase_client_cached.cache_clear()
