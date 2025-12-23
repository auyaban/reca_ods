from functools import lru_cache
from typing import Any

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import get_settings
from app.excel_sync import delete_row, flush_queue, queue_action, update_row
from app.supabase_client import get_supabase_client


router = APIRouter(prefix="/wizard/editar", tags=["wizard"])


class OdsFiltro(BaseModel):
    id: str | None = None
    id_servicio: int | None = None
    fecha_servicio: str | None = None
    codigo_servicio: str | None = None
    nit_empresa: str | None = None
    nombre_profesional: str | None = None


class OdsActualizarRequest(BaseModel):
    filtro: OdsFiltro
    datos: dict = Field(default_factory=dict)
    original: dict | None = None


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
            filtro.id_servicio,
            filtro.fecha_servicio,
            filtro.codigo_servicio,
            filtro.nit_empresa,
            filtro.nombre_profesional,
        ]
    ):
        raise HTTPException(status_code=422, detail="Debe enviar al menos un filtro")


def _apply_filters(query, filtro: OdsFiltro, partial: bool = False):
    if filtro.id:
        query = query.eq("id", filtro.id)
    if filtro.id_servicio is not None:
        query = query.eq("id_servicio", filtro.id_servicio)
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
        return value.strip()

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


@router.get("/buscar")
def buscar_entradas(
    id_servicio: int | None = None,
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    filtro = OdsFiltro(
        id_servicio=id_servicio,
        nombre_profesional=nombre_profesional,
        nit_empresa=nit_empresa,
        fecha_servicio=fecha_servicio,
        codigo_servicio=codigo_servicio,
    )
    _require_filtro(filtro)

    client = get_supabase_client()
    try:
        query = client.table("ods").select(
            "id,id_servicio,fecha_servicio,nombre_profesional,nombre_empresa,codigo_servicio,nit_empresa"
        )
        query = _apply_filters(query, filtro, partial=True)
        response = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    return {"data": response.data}


@router.get("/entrada")
def obtener_entrada(
    id: str | None = None,
    id_servicio: int | None = None,
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    filtro = OdsFiltro(
        id=id,
        id_servicio=id_servicio,
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
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    if not response.data:
        raise HTTPException(status_code=404, detail="No se encontro la entrada")
    if len(response.data) > 1:
        raise HTTPException(
            status_code=409,
            detail="El filtro devuelve multiples entradas. Ajusta la busqueda.",
        )
    return {"data": response.data[0]}


@router.post("/actualizar")
def actualizar_entrada(payload: OdsActualizarRequest):
    _require_filtro(payload.filtro)
    if not payload.datos:
        raise HTTPException(status_code=422, detail="No hay datos para actualizar")

    client = get_supabase_client()
    try:
        query = client.table("ods").select("*")
        query = _apply_filters(query, payload.filtro, partial=False)
        current = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    if not current.data:
        raise HTTPException(status_code=404, detail="No se encontro la entrada")
    if len(current.data) > 1:
        raise HTTPException(
            status_code=409,
            detail="El filtro devuelve multiples entradas. Ajusta la busqueda.",
        )

    current_row = current.data[0]
    update_fields = _filter_update_fields(payload.datos)

    cambios = {}
    for key, value in update_fields.items():
        if _normalize_value(current_row.get(key)) != _normalize_value(value):
            cambios[key] = value

    if not cambios:
        return {"data": current_row, "cambios": [], "excel_status": "ok", "excel_error": None}

    try:
        query = client.table("ods").update(cambios)
        query = _apply_filters(query, payload.filtro, partial=False)
        updated = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    excel_status = "ok"
    excel_error = None
    original = payload.original or current_row
    try:
        update_row(original, {**current_row, **cambios})
    except PermissionError:
        excel_status = "pendiente"
        excel_error = "El archivo Excel esta abierto. Se dejo en cola."
        queue_action("update", {**current_row, **cambios}, original, "archivo_abierto")
    except Exception as exc:
        excel_status = "error"
        excel_error = str(exc)
        queue_action("update", {**current_row, **cambios}, original, "error_guardado")

    return {
        "data": updated.data,
        "cambios": list(cambios.keys()),
        "excel_status": excel_status,
        "excel_error": excel_error,
    }


@router.post("/eliminar")
def eliminar_entrada(payload: OdsEliminarRequest):
    _require_filtro(payload.filtro)

    client = get_supabase_client()
    try:
        query = client.table("ods").select("id")
        query = _apply_filters(query, payload.filtro, partial=False)
        current = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    if not current.data:
        raise HTTPException(status_code=404, detail="No se encontro la entrada")
    if len(current.data) > 1:
        raise HTTPException(
            status_code=409,
            detail="El filtro devuelve multiples entradas. Ajusta la busqueda.",
        )

    try:
        query = client.table("ods").delete()
        query = _apply_filters(query, payload.filtro, partial=False)
        response = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    excel_status = "ok"
    excel_error = None
    original = payload.original or current.data[0]
    try:
        delete_row(original)
    except PermissionError:
        excel_status = "pendiente"
        excel_error = "El archivo Excel esta abierto. Se dejo en cola."
        queue_action("delete", original, original, "archivo_abierto")
    except Exception as exc:
        excel_status = "error"
        excel_error = str(exc)
        queue_action("delete", original, original, "error_guardado")

    return {"data": response.data, "excel_status": excel_status, "excel_error": excel_error}


@router.post("/excel/flush")
def flush_excel_queue() -> dict:
    try:
        result = flush_queue()
    except PermissionError as exc:
        raise HTTPException(
            status_code=423,
            detail=f"El archivo Excel esta abierto. {exc}",
        ) from exc
    return {"data": result}
