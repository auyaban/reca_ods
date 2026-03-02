from typing import Any, Callable

from app.logging_utils import LOGGER_BACKEND, get_logger
from app.services.errors import SUPABASE_ERRORS, ServiceError

_logger = get_logger(LOGGER_BACKEND)
_BACKGROUND_TASK_ERRORS = tuple(exc for exc in SUPABASE_ERRORS if exc is not TypeError) + (
    ServiceError,
    OSError,
    RuntimeError,
    ValueError,
)


class InlineBackgroundTasks:
    def __init__(self) -> None:
        self._tasks: list[tuple[Callable[..., Any], tuple, dict]] = []

    def add_task(self, func: Callable[..., Any], *args, **kwargs) -> None:
        self._tasks.append((func, args, kwargs))

    def run(self) -> None:
        for index, (func, args, kwargs) in enumerate(self._tasks, start=1):
            try:
                func(*args, **kwargs)
            except _BACKGROUND_TASK_ERRORS as exc:
                _logger.exception(
                    "Background task fallo y se continua con las siguientes. "
                    "Task=%s Index=%s Error=%s",
                    getattr(func, "__name__", str(func)),
                    index,
                    exc,
                )
