from functools import lru_cache
from typing import Any

import requests

from app.models.payloads import TerminarServicioRequest, dump_ods_for_rpc
from app.config import get_settings
from app.google_drive_sync import sync_new_ods_record
from app.logging_utils import LOGGER_BACKEND_INSERT, get_logger
from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket

_logger = get_logger(LOGGER_BACKEND_INSERT)


_ODS_SCHEMA_CACHE_TTL_SECONDS = 180
_YEAR_FIELD_ALIASES = (
    "ano_servicio",
    "año_servicio",
    "a\u00c3\u00b1o_servicio",
    "a?o_servicio",
    "a\u00ef\u00bf\u00bdo_servicio",
    "a\u00c3\u0192\u00c2\u00b1o_servicio",
)
_ODS_PASSTHROUGH_FIELDS = {"session_id", "started_at", "submitted_at"}


@lru_cache
def _fetch_ods_schema_cached(
    supabase_url: str, supabase_anon_key: str, _ttl_bucket: int
) -> dict[str, dict[str, Any]]:
    if not supabase_url or not supabase_anon_key:
        return {}

    url = supabase_url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": supabase_anon_key,
        "Authorization": f"Bearer {supabase_anon_key}",
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


def _fetch_ods_schema() -> dict[str, dict[str, Any]]:
    settings = get_settings()
    return _fetch_ods_schema_cached(
        settings.supabase_url,
        settings.supabase_anon_key,
        ttl_bucket(_ODS_SCHEMA_CACHE_TTL_SECONDS),
    )


def clear_schema_cache() -> None:
    _fetch_ods_schema_cached.cache_clear()


def _first_date_value(raw: str) -> str | None:
    parts = [item.strip() for item in raw.replace(";", ",").split(",")]
    for part in parts:
        if part:
            return part
    return None


def _coerce_value(field: str, value: Any, schema: dict[str, Any]) -> Any:
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
            if clean == "":
                return None
            if clean in {"si", "true", "1"}:
                return True
            if clean in {"no", "false", "0"}:
                return False
        raise ValueError(f"{field}: valor booleano invalido ({value!r})")

    if expected == "integer":
        if isinstance(value, bool):
            raise ValueError(f"{field}: no se permite bool para entero")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ValueError(f"{field}: el valor {value!r} no es entero")
        if isinstance(value, str):
            clean = value.strip()
            if clean == "":
                return None
            try:
                parsed = float(clean)
            except (TypeError, ValueError):
                raise ValueError(f"{field}: valor entero invalido ({value!r})")
            if not parsed.is_integer():
                raise ValueError(f"{field}: el valor {value!r} no es entero")
            return int(parsed)
        raise ValueError(f"{field}: tipo invalido para entero ({type(value).__name__})")

    if expected == "number":
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            clean = value.strip()
            if clean == "":
                return None
            try:
                return float(clean)
            except ValueError:
                raise ValueError(f"{field}: valor numerico invalido ({value!r})")
        raise ValueError(f"{field}: tipo invalido para numero ({type(value).__name__})")

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

    year_schema_key = next((name for name in _YEAR_FIELD_ALIASES if name in schema), None)
    if year_schema_key:
        year_value = None
        for key in _YEAR_FIELD_ALIASES:
            if key in ods_data:
                year_value = ods_data[key]
                break
        if year_value is not None:
            for key in _YEAR_FIELD_ALIASES:
                ods_data.pop(key, None)
            ods_data[year_schema_key] = year_value

    filtered: dict[str, Any] = {}
    dropped = []
    for key, value in ods_data.items():
        if key not in schema:
            if key in _ODS_PASSTHROUGH_FIELDS:
                filtered[key] = value
                continue
            dropped.append(key)
            continue
        try:
            filtered[key] = _coerce_value(key, value, schema[key])
        except ValueError as exc:
            raise ServiceError(str(exc), status_code=422) from exc

    if dropped:
        _logger.info("ODS columnas ignoradas (no en schema): %s", dropped)

    return filtered


def terminar_servicio(payload: TerminarServicioRequest, background_tasks) -> dict:
    try:
        if payload.usuarios_nuevos:
            nuevos = [item.model_dump() for item in payload.usuarios_nuevos]
            cedulas = [item["cedula_usuario"] for item in nuevos if item.get("cedula_usuario")]
            existentes = set()
            if cedulas:
                existente_resp = execute_with_reauth(
                    lambda retry_client: (
                        retry_client.table("usuarios_reca")
                        .select("cedula_usuario")
                        .in_("cedula_usuario", cedulas)
                        .execute()
                    ),
                    context="terminar_servicio.fetch_existing_users",
                )
                existentes = {item["cedula_usuario"] for item in (existente_resp.data or [])}
            to_insert = [item for item in nuevos if item.get("cedula_usuario") not in existentes]
            if to_insert:
                execute_with_reauth(
                    lambda retry_client: retry_client.table("usuarios_reca").insert(to_insert).execute(),
                    context="terminar_servicio.insert_new_users",
                )

        ods_data = dump_ods_for_rpc(payload.ods)
        fecha_ingreso = ods_data.get("fecha_ingreso")
        if isinstance(fecha_ingreso, str):
            cleaned = _first_date_value(fecha_ingreso)
            ods_data["fecha_ingreso"] = cleaned

        ods_data = _apply_schema(ods_data)

        _logger.info("ODS payload keys=%s", list(ods_data.keys()))
        response = execute_with_reauth(
            lambda retry_client: retry_client.table("ods").insert(ods_data).execute(),
            context="terminar_servicio.insert_ods",
        )
        sync_result = {
            "sync_status": "warning",
            "sync_error": "No se recibio la fila insertada desde Supabase.",
            "sync_target": None,
        }
        if response.data:
            inserted = response.data[0]
            if isinstance(inserted, dict):
                if inserted.get("id"):
                    ods_data["id"] = inserted["id"]
                sync_result = sync_new_ods_record(inserted)
        else:
            sync_result = sync_new_ods_record(ods_data)
    except ServiceError:
        raise
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {
        "data": response.data,
        "sync_status": sync_result.get("sync_status"),
        "sync_error": sync_result.get("sync_error"),
        "sync_target": sync_result.get("sync_target"),
    }
