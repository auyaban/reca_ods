from functools import lru_cache

from supabase import create_client, Client
from supabase_auth.errors import AuthApiError, AuthInvalidCredentialsError

from app.config import get_settings, persist_supabase_auth_credentials
from app.logging_utils import LOGGER_BACKEND, get_logger
from app.utils.cache import ttl_bucket

_SUPABASE_CLIENT_CACHE_TTL_SECONDS = 300
_LOGGER = get_logger(LOGGER_BACKEND)


def _ensure_authenticated(client: Client, email: str, passwords: tuple[str, ...]) -> None:
    session = client.auth.get_session()
    if session and session.access_token:
        return

    last_error: Exception | None = None
    for password in passwords:
        try:
            response = client.auth.sign_in_with_password(
                {
                    "email": email,
                    "password": password,
                }
            )
            session = response.session or client.auth.get_session()
            if not session or not session.access_token:
                raise RuntimeError("Supabase authentication failed: missing access token")
            try:
                persist_supabase_auth_credentials(email, password)
            except OSError as exc:
                _LOGGER.warning("No se pudo persistir credencial Supabase autocorregida: %s", exc)
            return
        except (AuthApiError, AuthInvalidCredentialsError, RuntimeError, ValueError, TypeError) as exc:
            last_error = exc
            _LOGGER.warning("Supabase auth intento fallido para %s: %s", email, exc)
            continue

    raise RuntimeError(f"Supabase authentication failed for {email}: {last_error}")


@lru_cache
def _get_supabase_client_cached(
    supabase_url: str,
    supabase_anon_key: str,
    supabase_auth_email: str,
    supabase_auth_passwords: tuple[str, ...],
    _ttl_bucket: int,
) -> Client:
    if not supabase_url or not supabase_anon_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY")
    if not supabase_auth_email or not supabase_auth_passwords:
        raise RuntimeError("Missing SUPABASE_AUTH_EMAIL or SUPABASE_AUTH_PASSWORD")

    client = create_client(supabase_url, supabase_anon_key)
    _ensure_authenticated(client, supabase_auth_email, supabase_auth_passwords)
    return client


def get_supabase_client() -> Client:
    settings = get_settings()
    return _get_supabase_client_cached(
        settings.supabase_url,
        settings.supabase_anon_key,
        settings.supabase_auth_email,
        settings.supabase_auth_password_candidates,
        ttl_bucket(_SUPABASE_CLIENT_CACHE_TTL_SECONDS),
    )


def clear_supabase_client_cache() -> None:
    _get_supabase_client_cached.cache_clear()
