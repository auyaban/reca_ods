from functools import lru_cache

from supabase import create_client, Client
from supabase_auth.errors import AuthApiError, AuthInvalidCredentialsError

from app.config import get_settings, persist_supabase_auth_credentials
from app.logging_utils import LOGGER_BACKEND, get_logger
from app.services.errors import SUPABASE_ERRORS
from app.utils.cache import ttl_bucket

_SUPABASE_CLIENT_CACHE_TTL_SECONDS = 300
_LOGGER = get_logger(LOGGER_BACKEND)
_AUTH_ERROR_MARKERS = (
    "authentication failed",
    "invalid login credentials",
    "invalid jwt",
    "jwt",
    "refresh token",
    "session not found",
    "not authenticated",
    "auth session missing",
    "supabase auth",
    "missing access token",
    "401",
)
_PERMISSION_ERROR_MARKERS = (
    "permission denied",
    "42501",
    "row-level security",
    "violates row-level security policy",
    "forbidden",
    "insufficient_privilege",
)
_CONNECTIVITY_ERROR_MARKERS = (
    "timed out",
    "timeout",
    "connection refused",
    "connection reset",
    "network is unreachable",
    "temporary failure",
    "name or service not known",
    "nodename nor servname provided",
    "remoteprotocolerror",
    "connecterror",
)


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


def _error_text(exc: Exception) -> str:
    return str(exc or "").strip().lower()


def is_auth_error(exc: Exception) -> bool:
    text = _error_text(exc)
    return isinstance(exc, (AuthApiError, AuthInvalidCredentialsError)) or any(marker in text for marker in _AUTH_ERROR_MARKERS)


def is_permission_error(exc: Exception) -> bool:
    text = _error_text(exc)
    return any(marker in text for marker in _PERMISSION_ERROR_MARKERS)


def is_connectivity_error(exc: Exception) -> bool:
    text = _error_text(exc)
    return any(marker in text for marker in _CONNECTIVITY_ERROR_MARKERS)


def classify_supabase_error(exc: Exception) -> str:
    if is_auth_error(exc):
        return "auth"
    if is_permission_error(exc):
        return "permission"
    if is_connectivity_error(exc):
        return "connectivity"
    return "generic"


def execute_with_reauth(operation, *, context: str = "supabase", retry_on_auth: bool = True):
    last_error: Exception | None = None
    attempts = 2 if retry_on_auth else 1
    for attempt in range(1, attempts + 1):
        client = get_supabase_client()
        try:
            return operation(client)
        except SUPABASE_ERRORS as exc:
            last_error = exc
            if retry_on_auth and attempt < attempts and is_auth_error(exc):
                _LOGGER.warning(
                    "Supabase auth invalida o vencida en %s. Reautenticando y reintentando una vez: %s",
                    context,
                    exc,
                )
                clear_supabase_client_cache()
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Supabase operation failed without explicit error: {context}")


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
