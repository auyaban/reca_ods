from __future__ import annotations

import logging
import threading
from pathlib import Path

_LOGGER_LOCK = threading.Lock()

LOGGER_GUI = "reca.gui"
LOGGER_GOOGLE_DRIVE = "reca.google_drive"
LOGGER_BACKEND = "reca.backend"
LOGGER_BACKEND_INSERT = "reca.backend.insert"
LOGGER_UPDATER = "reca.updater"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def get_file_logger(
    name: str,
    log_file: Path,
    *,
    level: int = logging.INFO,
    announce: bool = False,
    propagate: bool = False,
) -> logging.Logger:
    logger = logging.getLogger(name)
    target = log_file.resolve()

    with _LOGGER_LOCK:
        logger.setLevel(level)
        logger.propagate = propagate

        exists = False
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                try:
                    if Path(handler.baseFilename).resolve() == target:
                        exists = True
                        break
                except OSError:
                    continue

        if not exists:
            target.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(target, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            logger.addHandler(handler)

    if announce:
        logger.info("Logger iniciado. Archivo=%s", target)
    return logger
