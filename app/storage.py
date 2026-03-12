from app.logging_utils import LOGGER_BACKEND, get_logger
from app.paths import app_data_dir

_logger = get_logger(LOGGER_BACKEND)


def ensure_appdata_files() -> None:
    data_root = app_data_dir()
    required_dirs = [
        data_root,
        data_root / "logs",
        data_root / "queues",
        data_root / "secrets",
    ]
    for directory in required_dirs:
        directory.mkdir(parents=True, exist_ok=True)
    _logger.info("Estructura local verificada en %s", data_root)
