from app.logging_utils import LOGGER_BACKEND, get_logger
from app.paths import resource_path

_logger = get_logger(LOGGER_BACKEND)


def get_version() -> str:
    try:
        path = resource_path("VERSION")
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        _logger.warning("No se pudo leer VERSION: %s", exc)
        return "0.0.0"
