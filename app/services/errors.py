from typing import TypeAlias

try:
    from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:  # pragma: no cover - dependencia opcional en runtime
    PostgrestAPIError = RuntimeError  # type: ignore[assignment]

SUPABASE_ERROR_TYPES: TypeAlias = tuple[type[BaseException], ...]
RUNTIME_ERROR_TYPES: TypeAlias = tuple[type[BaseException], ...]

SUPABASE_ERRORS: SUPABASE_ERROR_TYPES = (
    PostgrestAPIError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)

RUNTIME_ERRORS: RUNTIME_ERROR_TYPES = (
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


class ServiceError(Exception):
    def __init__(self, detail: str, status_code: int = 400) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code
