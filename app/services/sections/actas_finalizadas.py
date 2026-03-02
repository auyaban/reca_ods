from pydantic import BaseModel, Field

from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import get_supabase_client

_TABLE = "formatos_finalizados_il"
_PAGE_SIZE = 1000


def _is_revisado(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "si", "s", "yes", "y", "x"}
    return bool(value)


def _iter_rows_keyset() -> list[dict]:
    client = get_supabase_client()
    rows: list[dict] = []
    last_registro_id: str | None = None

    while True:
        query = (
            client.table(_TABLE)
            .select("registro_id,revisado")
            .order("registro_id", desc=False)
            .limit(_PAGE_SIZE)
        )
        if last_registro_id:
            query = query.gt("registro_id", last_registro_id)

        response = query.execute()
        batch = list(response.data or [])
        if not batch:
            break

        rows.extend(batch)

        last_value = batch[-1].get("registro_id")
        if not last_value:
            break
        last_registro_id = str(last_value)

        if len(batch) < _PAGE_SIZE:
            break

    return rows


def _count_pendientes() -> int:
    client = get_supabase_client()
    try:
        # Conteo server-side para evitar inconsistencias por paginacion/rangos
        # cuando hay inserciones/eliminaciones concurrentes.
        response = (
            client.table(_TABLE)
            .select("registro_id", count="exact", head=True)
            .or_("revisado.is.null,revisado.eq.false")
            .execute()
        )
        if response.count is not None:
            return int(response.count)

        # Fallback con cursor/keyset (sin offset) para evitar saltos de filas
        # bajo concurrencia de inserciones/eliminaciones.
        rows = _iter_rows_keyset()
        pendientes = sum(0 if _is_revisado(row.get("revisado")) else 1 for row in rows)
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc
    return int(pendientes)


def listar_actas_finalizadas(limit: int = 500) -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table(_TABLE)
            .select(
                "registro_id,session_id,created_at,finalizado_at_colombia,finalizado_at_iso,"
                "nombre_usuario,nombre_empresa,nombre_formato,path_formato,revisado"
            )
            .order("created_at", desc=True)
            .limit(max(1, min(int(limit), 2000)))
            .execute()
        )
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    pendientes = _count_pendientes()
    return {"data": response.data or [], "pendientes": pendientes}


def estado_actas_finalizadas() -> dict:
    return {"data": {"pendientes": _count_pendientes()}}


class ActaRevisadoRequest(BaseModel):
    registro_id: str | None = None
    session_id: str | None = None
    revisado: bool = Field(default=False)


def actualizar_revisado(payload: ActaRevisadoRequest) -> dict:
    registro_id = (payload.registro_id or "").strip()
    session_id = (payload.session_id or "").strip()
    if not registro_id and not session_id:
        raise ServiceError("Debe enviar registro_id o session_id", status_code=422)

    client = get_supabase_client()
    try:
        query = client.table(_TABLE).update({"revisado": bool(payload.revisado)})
        if registro_id:
            query = query.eq("registro_id", registro_id)
        if session_id:
            query = query.eq("session_id", session_id)
        updated = query.execute()
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    pendientes = _count_pendientes()
    return {"data": updated.data or [], "pendientes": pendientes}
