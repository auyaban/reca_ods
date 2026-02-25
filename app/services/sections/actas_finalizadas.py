from pydantic import BaseModel, Field

from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client

_TABLE = "formatos_finalizados_il"


def _count_pendientes() -> int:
    client = get_supabase_client()
    try:
        response = (
            client.table(_TABLE)
            .select("registro_id", count="exact")
            .eq("revisado", False)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc
    return int(getattr(response, "count", 0) or 0)


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
    except Exception as exc:
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
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    pendientes = _count_pendientes()
    return {"data": updated.data or [], "pendientes": pendientes}
