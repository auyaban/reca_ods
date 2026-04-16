import re
from typing import Any

from pydantic import BaseModel

from app.catalog_index import get_indexed_profesionales, sync_local_catalog_indexes
from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import execute_with_reauth
from app.utils.text import normalize_text

_PROGRAMA_INCLUSION = "InclusiÃ³n Laboral"
_PROGRAMA_INTERPRETE = "Interprete"


def _resolve_programa(value: str) -> str | None:
    key = normalize_text(value or "", lowercase=True)
    key = re.sub(r"[^a-z\s]", " ", key)
    key = re.sub(r"\s+", " ", key).strip()
    if not key:
        return None
    if ("inclus" in key or "inclu" in key) and "labor" in key:
        return _PROGRAMA_INCLUSION
    if "interp" in key or ("int" in key and "rpret" in key):
        return _PROGRAMA_INTERPRETE
    return None


def _insert_profesional(*, nombre: str, programa: str) -> None:
    for _ in range(3):
        try:
            last = execute_with_reauth(
                lambda retry_client: (
                    retry_client.table("profesionales")
                    .select("id")
                    .order("id", desc=True)
                    .limit(1)
                    .execute()
                ),
                context="seccion1.insert_profesional.fetch_last_id",
            )
            next_id = int(last.data[0]["id"]) + 1 if last.data else 1
            payload_db = {"id": next_id, "nombre_profesional": nombre, "programa": programa}
            execute_with_reauth(
                lambda retry_client: retry_client.table("profesionales").insert(payload_db).execute(),
                context="seccion1.insert_profesional.insert",
            )
            return
        except SUPABASE_ERRORS as exc:
            if "duplicate key value" in str(exc).lower():
                continue
            raise
    raise ServiceError("No se pudo crear profesional por conflicto de id", status_code=409)


def get_orden_clausulada_opciones() -> dict:
    opciones = [
        {"id": "si", "label": "Si"},
        {"id": "no", "label": "No"},
    ]
    return {"data": opciones}


def get_profesionales(programa: str | None = None) -> dict:
    programa_resuelto = _resolve_programa(programa or "")
    try:
        rows = list(get_indexed_profesionales())
    except (RuntimeError, ValueError, TypeError, OSError) as exc:
        raise ServiceError(f"No se pudo leer indice local de profesionales: {exc}", status_code=500) from exc

    nombres_meta: dict[str, dict[str, Any]] = {}
    for item in rows:
        nombre = (item.get("nombre_profesional") or "").strip()
        programa_item = _resolve_programa(str(item.get("programa") or ""))
        es_interprete = bool(item.get("es_interprete")) or programa_item == _PROGRAMA_INTERPRETE
        if not nombre:
            continue
        if programa_resuelto == _PROGRAMA_INTERPRETE and not es_interprete:
            continue
        if programa_resuelto == _PROGRAMA_INCLUSION and es_interprete:
            continue
        nombres_meta[nombre] = {
            "nombre_profesional": nombre,
            "programa": _PROGRAMA_INTERPRETE if es_interprete else (programa_item or _PROGRAMA_INCLUSION),
            "es_interprete": es_interprete,
        }

    nombres = sorted(nombres_meta.keys(), key=lambda value: value.lower())
    return {"data": [nombres_meta[nombre] for nombre in nombres]}


class Seccion1ConfirmarRequest(BaseModel):
    orden_clausulada: str
    nombre_profesional: str


class CrearProfesionalRequest(BaseModel):
    nombre_profesional: str
    programa: str


def confirmar_seccion_1(payload: Seccion1ConfirmarRequest) -> dict:
    orden = payload.orden_clausulada.strip().lower()
    if orden not in {"si", "no"}:
        raise ServiceError("orden_clausulada debe ser 'si' o 'no'", status_code=422)

    return {
        "data": {
            "orden_clausulada": orden,
            "nombre_profesional": payload.nombre_profesional.strip(),
        }
    }


def crear_profesional(payload: CrearProfesionalRequest) -> dict:
    nombre = " ".join(payload.nombre_profesional.strip().split())
    if not nombre:
        raise ServiceError("nombre_profesional es obligatorio", status_code=422)
    nombre = " ".join([part.capitalize() for part in nombre.split(" ")])

    programa = _resolve_programa(payload.programa)
    if not programa:
        raise ServiceError("programa invalido", status_code=422)

    try:
        if programa == _PROGRAMA_INTERPRETE:
            execute_with_reauth(
                lambda retry_client: retry_client.table("interpretes").insert({"nombre": nombre}).execute(),
                context="seccion1.crear_profesional.interprete",
            )
            try:
                sync_local_catalog_indexes(catalogs=("profesionales",))
            except Exception:
                pass
            return {"data": {"nombre_profesional": nombre}}

        _insert_profesional(nombre=nombre, programa=programa)
        try:
            sync_local_catalog_indexes(catalogs=("profesionales",))
        except Exception:
            pass
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": {"nombre_profesional": nombre}}
