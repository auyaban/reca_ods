import logging
from functools import lru_cache
from typing import Any

import requests

from app.models.payloads import TerminarServicioRequest, dump_ods_for_rpc
from app.config import get_settings
from app.excel_sync import append_row, queue_action
from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client

_logger = logging.getLogger("reca_ods_insert")


@lru_cache
def _fetch_ods_schema() -> dict[str, dict[str, Any]]:
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_anon_key:
        return {}

    url = settings.supabase_url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": settings.supabase_anon_key,
        "Authorization": f"Bearer {settings.supabase_anon_key}",
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        _logger.warning("No se pudo leer schema ODS: %s", exc)
        return {}

    data = response.json()
    schema = None
    if "definitions" in data:
        schema = data["definitions"].get("ods")
    if not schema and "components" in data:
        schema = data.get("components", {}).get("schemas", {}).get("ods")
    if not schema:
        _logger.warning("Schema ODS no encontrado en OpenAPI.")
        return {}
    return schema.get("properties", {}) or {}


def _first_date_value(raw: str) -> str | None:
    parts = [item.strip() for item in raw.replace(";", ",").split(",")]
    for part in parts:
        if part:
            return part
    return None


def _coerce_value(value: Any, schema: dict[str, Any]) -> Any:
    expected = schema.get("type")
    fmt = schema.get("format", "")

    if value is None:
        return None

    if expected == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in {0, 1, 0.0, 1.0}:
            return bool(value)
        if isinstance(value, str):
            clean = value.strip().lower()
            if clean in {"si", "true", "1"}:
                return True
            if clean in {"no", "false", "0"}:
                return False
        return value

    if expected == "integer":
        if isinstance(value, int):
            return value
        if isinstance(value, (float, str)):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return value

    if expected == "number":
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return value

    if expected == "string" and fmt == "date" and isinstance(value, str):
        clean = _first_date_value(value)
        if clean is None:
            return None
        return clean

    if isinstance(value, str):
        return value.strip()

    return value


def _apply_schema(ods_data: dict[str, Any]) -> dict[str, Any]:
    schema = _fetch_ods_schema()
    if not schema:
        return ods_data

    filtered: dict[str, Any] = {}
    dropped = []
    for key, value in ods_data.items():
        if key not in schema:
            dropped.append(key)
            continue
        filtered[key] = _coerce_value(value, schema[key])

    if dropped:
        _logger.info("ODS columnas ignoradas (no en schema): %s", dropped)

    return filtered


def _persist_excel_background(ods_data: dict) -> None:
    try:
        append_row(ods_data)
    except PermissionError:
        queue_action("append", ods_data, None, "archivo_abierto")
    except Exception:
        queue_action("append", ods_data, None, "error_guardado")


def terminar_servicio(payload: TerminarServicioRequest, background_tasks) -> dict:
    client = get_supabase_client()
    try:
        if payload.usuarios_nuevos:
            nuevos = [item.model_dump() for item in payload.usuarios_nuevos]
            cedulas = [item["cedula_usuario"] for item in nuevos if item.get("cedula_usuario")]
            existentes = set()
            if cedulas:
                existente_resp = (
                    client.table("usuarios_reca")
                    .select("cedula_usuario")
                    .in_("cedula_usuario", cedulas)
                    .execute()
                )
                existentes = {item["cedula_usuario"] for item in (existente_resp.data or [])}
            to_insert = [item for item in nuevos if item.get("cedula_usuario") not in existentes]
            if to_insert:
                client.table("usuarios_reca").insert(to_insert).execute()

        ods_data = dump_ods_for_rpc(payload.ods)
        fecha_ingreso = ods_data.get("fecha_ingreso")
        if isinstance(fecha_ingreso, str):
            cleaned = _first_date_value(fecha_ingreso)
            ods_data["fecha_ingreso"] = cleaned

        ods_data = _apply_schema(ods_data)

        _logger.info("ODS payload keys=%s", list(ods_data.keys()))
        response = client.table("ods").insert(ods_data).execute()
        if response.data:
            inserted = response.data[0]
            if isinstance(inserted, dict) and inserted.get("id"):
                ods_data["id"] = inserted["id"]
    except Exception as exc:
        message = str(exc)
        if "boolean" in message:
            coerced = dict(ods_data)
            for key, value in ods_data.items():
                if isinstance(value, str):
                    val = value.strip().lower()
                    if val in {"si", "true", "1"}:
                        coerced[key] = True
                    elif val in {"no", "false", "0"}:
                        coerced[key] = False
                elif isinstance(value, (int, float)) and value in {0, 1, 0.0, 1.0}:
                    coerced[key] = bool(value)

            try:
                response = client.table("ods").insert(coerced).execute()
                return {"data": response.data}
            except Exception:
                schema = _fetch_ods_schema()
                bool_fields = [key for key, info in schema.items() if info.get("type") == "boolean"]
                numeric_fields = [
                    key
                    for key, value in ods_data.items()
                    if isinstance(value, (float, int))
                ]
                message = (
                    "Supabase error: tipo booleano invalido. "
                    "Revisa columnas boolean en ODS. "
                    f"Columnas boolean segun schema: {bool_fields}. "
                    f"Campos numericos enviados: {numeric_fields}"
                )
        raise ServiceError(message, status_code=502) from exc

    background_tasks.add_task(_persist_excel_background, ods_data)

    return {"data": response.data, "excel_status": "background", "excel_error": None}
