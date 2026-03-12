from __future__ import annotations

from app.google_drive_sync import flush_google_drive_queue, get_google_drive_queue_status
from app.services.errors import ServiceError


def google_drive_flush() -> dict:
    try:
        result = flush_google_drive_queue()
    except Exception as exc:
        raise ServiceError(f"No se pudo sincronizar la cola de Drive: {exc}", status_code=500) from exc
    return {"data": result}


def google_drive_status() -> dict:
    return {"data": get_google_drive_queue_status()}
