from functools import lru_cache
from typing import Any

import requests
from pydantic import BaseModel, Field

from app.config import get_settings
from app.excel_sync import (
    clear_queue,
    delete_row_and_update_factura,
    flush_queue,
    get_queue_status,
    queue_action,
    rebuild_excel_from_supabase,
    rebuild_excel_from_supabase_query,
    update_row_and_update_factura,
)
from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client


class OdsFiltro(BaseModel):
    id: str | None = None
    fecha_servicio: str | None = None
    codigo_servicio: str | None = None
    nit_empresa: str | None = None
    nombre_profesional: str | None = None


class OdsActualizarRequest(BaseModel):
    filtro: OdsFiltro
    datos: dict = Field(default_factory=dict)
    original: dict | None = None
    force_excel_sync: bool = False


class OdsEliminarRequest(BaseModel):
    filtro: OdsFiltro
    original: dict | None = None


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
    except requests.RequestException:
        return {}

    data = response.json()
    schema = None
    if "definitions" in data:
        schema = data["definitions"].get("ods")
    if not schema and "components" in data:
        schema = data.get("components", {}).get("schemas", {}).get("ods")
    if not schema:
        return {}
    return schema.get("properties", {}) or {}


def _require_filtro(filtro: OdsFiltro) -> None:
    if not any(
        [
            filtro.id,
            filtro.fecha_servicio,
            filtro.codigo_servicio,
            filtro.nit_empresa,
            filtro.nombre_profesional,
        ]
    ):
        raise ServiceError("Debe enviar al menos un filtro", status_code=422)


def _apply_filters(query, filtro: OdsFiltro, partial: bool = False):
    if filtro.id:
        query = query.eq("id", filtro.id)
    if filtro.fecha_servicio:
        query = query.eq("fecha_servicio", filtro.fecha_servicio.strip())
    if filtro.codigo_servicio:
        query = query.eq("codigo_servicio", filtro.codigo_servicio.strip())
    if filtro.nit_empresa:
        query = query.eq("nit_empresa", filtro.nit_empresa.strip())
    if filtro.nombre_profesional:
        value = filtro.nombre_profesional.strip()
        if partial:
            query = query.ilike("nombre_profesional", f"%{value}%")
        else:
            query = query.eq("nombre_profesional", value)
    return query


def _normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, float):
        return round(value, 4)
    return value


def _coerce_update_value(value: Any, schema: dict[str, Any]) -> Any:
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
        parts = [item.strip() for item in value.replace(";", ",").split(",")]
        for part in parts:
            if part:
                return part
        return None

    if isinstance(value, str):
        return value.strip()

    return value


def _filter_update_fields(datos: dict) -> dict:
    schema = _fetch_ods_schema()
    if not schema:
        return datos
    filtered = {}
    for key, value in datos.items():
        if key not in schema:
            continue
        filtered[key] = _coerce_update_value(value, schema[key])
    return filtered


def buscar_entradas(
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    filtro = OdsFiltro(
        nombre_profesional=nombre_profesional,
        nit_empresa=nit_empresa,
        fecha_servicio=fecha_servicio,
        codigo_servicio=codigo_servicio,
    )
    _require_filtro(filtro)

    client = get_supabase_client()
    try:
        query = client.table("ods").select(
            "id,fecha_servicio,nombre_profesional,nombre_empresa,codigo_servicio,nit_empresa"
        )
        query = _apply_filters(query, filtro, partial=True)
        response = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


def obtener_entrada(
    id: str | None = None,
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    filtro = OdsFiltro(
        id=id,
        nombre_profesional=nombre_profesional,
        nit_empresa=nit_empresa,
        fecha_servicio=fecha_servicio,
        codigo_servicio=codigo_servicio,
    )
    _require_filtro(filtro)

    client = get_supabase_client()
    try:
        query = client.table("ods").select("*")
        query = _apply_filters(query, filtro, partial=False)
        response = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    if not response.data:
        raise ServiceError("No se encontro la entrada", status_code=404)
    if len(response.data) > 1:
        raise ServiceError(
            "El filtro devuelve multiples entradas. Ajusta la busqueda.",
            status_code=409,
        )
    return {"data": response.data[0]}


def _queue_factura_update(row: dict, reason: str) -> None:
    mes = int(row.get("mes_servicio", 0) or 0)
    ano_value = row.get("aヵo_servicio", 0) or row.get("ano_servicio", 0)
    ano = int(ano_value or 0)
    if not mes or not ano:
        return
    orden = str(row.get("orden_clausulada", "")).strip().lower()
    tipo = "clausulada" if orden.startswith("s") or orden == "true" else "no clausulada"
    queue_action(
        "factura_update",
        {},
        None,
        reason,
        meta={"mes": mes, "ano": ano, "tipo": tipo},
    )


def _update_excel_background(original: dict, merged: dict) -> None:
    try:
        update_row_and_update_factura(original, merged)
    except PermissionError:
        queue_action("update", merged, original, "archivo_abierto")
        _queue_factura_update(merged, "archivo_abierto")
    except Exception:
        queue_action("update", merged, original, "error_guardado")
        _queue_factura_update(merged, "error_guardado")


def _rebuild_excel_now_or_queue() -> tuple[str, str | None]:
    try:
        rebuild_excel_from_supabase_query(create_backup=False)
        return "ok", None
    except PermissionError:
        queue_action("rebuild", {}, None, "archivo_abierto")
        return "pendiente", "archivo_abierto"
    except Exception as exc:
        queue_action("rebuild", {}, None, "error_guardado")
        return "error", str(exc)


def _rebuild_excel_background() -> None:
    try:
        rebuild_excel_from_supabase_query(create_backup=False)
    except PermissionError:
        queue_action("rebuild", {}, None, "archivo_abierto")
    except Exception:
        queue_action("rebuild", {}, None, "error_guardado")


def _delete_excel_background(original: dict) -> None:
    try:
        delete_row_and_update_factura(original)
    except PermissionError:
        queue_action("delete", original, original, "archivo_abierto")
        _queue_factura_update(original, "archivo_abierto")
    except Exception:
        queue_action("delete", original, original, "error_guardado")
        _queue_factura_update(original, "error_guardado")


def actualizar_entrada(payload: OdsActualizarRequest, background_tasks) -> dict:
    _require_filtro(payload.filtro)
    if not payload.datos and not payload.force_excel_sync:
        raise ServiceError("No hay datos para actualizar", status_code=422)

    client = get_supabase_client()
    try:
        query = client.table("ods").select("*")
        query = _apply_filters(query, payload.filtro, partial=False)
        current = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    if not current.data:
        raise ServiceError("No se encontro la entrada", status_code=404)
    if len(current.data) > 1:
        raise ServiceError(
            "El filtro devuelve multiples entradas. Ajusta la busqueda.",
            status_code=409,
        )

    current_row = current.data[0]
    update_fields = _filter_update_fields(payload.datos)

    cambios = {}
    for key, value in update_fields.items():
        if _normalize_value(current_row.get(key)) != _normalize_value(value):
            cambios[key] = value

    if not cambios and payload.force_excel_sync:
        excel_status, excel_error = _rebuild_excel_now_or_queue()
        return {"data": current_row, "cambios": [], "excel_status": excel_status, "excel_error": excel_error}
    if not cambios:
        return {"data": current_row, "cambios": [], "excel_status": "ok", "excel_error": None}

    try:
        query = client.table("ods").update(cambios)
        query = _apply_filters(query, payload.filtro, partial=False)
        updated = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    excel_status, excel_error = _rebuild_excel_now_or_queue()

    return {
        "data": updated.data,
        "cambios": list(cambios.keys()),
        "excel_status": excel_status,
        "excel_error": excel_error,
    }


def eliminar_entrada(payload: OdsEliminarRequest, background_tasks) -> dict:
    _require_filtro(payload.filtro)

    client = get_supabase_client()
    try:
        query = client.table("ods").select("id")
        query = _apply_filters(query, payload.filtro, partial=False)
        current = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    if not current.data:
        raise ServiceError("No se encontro la entrada", status_code=404)
    if len(current.data) > 1:
        raise ServiceError(
            "El filtro devuelve multiples entradas. Ajusta la busqueda.",
            status_code=409,
        )

    try:
        query = client.table("ods").delete()
        query = _apply_filters(query, payload.filtro, partial=False)
        response = query.execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    original = payload.original or current.data[0]
    background_tasks.add_task(_delete_excel_background, original)

    return {"data": response.data, "excel_status": "background", "excel_error": None}


def flush_excel_queue() -> dict:
    try:
        result = flush_queue()
    except PermissionError as exc:
        raise ServiceError(f"El archivo Excel esta abierto. {exc}", status_code=423) from exc
    return {"data": result}


def excel_status() -> dict:
    return {"data": get_queue_status()}


def rebuild_excel() -> dict:
    status = get_queue_status()
    if status.get("locked"):
        raise ServiceError(
            "El archivo Excel esta abierto. Cierralo antes de reconstruir.",
            status_code=423,
        )

    client = get_supabase_client()
    try:
        response = client.table("ods").select("*").execute()
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    rows = response.data or []
    try:
        result = rebuild_excel_from_supabase(rows)
        clear_queue()
    except Exception as exc:
        raise ServiceError(f"No se pudo reconstruir el Excel: {exc}", status_code=500) from exc

    return {"data": result}
