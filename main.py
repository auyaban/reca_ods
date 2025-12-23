from pathlib import Path
import logging
import time

from fastapi import FastAPI, Request

from app.routes import router
from app.storage import ensure_appdata_files

app = FastAPI(title="RECA ODS API")
app.include_router(router)


@app.on_event("startup")
def _startup() -> None:
    ensure_appdata_files()

_LOG_DIR = Path(__file__).resolve().parent / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "api.log"

_logger = logging.getLogger("reca_ods_api")
if not _logger.handlers:
    handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.info("API logger iniciado. Archivo=%s", _LOG_FILE)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = (time.perf_counter() - start) * 1000
        _logger.exception(
            "ERROR %s %s -> 500 in %.2fms",
            request.method,
            request.url.path,
            duration_ms,
        )
        raise exc

    duration_ms = (time.perf_counter() - start) * 1000
    _logger.info(
        "%s %s -> %s in %.2fms",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response
