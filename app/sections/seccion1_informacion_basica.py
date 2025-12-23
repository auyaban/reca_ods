from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.supabase_client import get_supabase_client

router = APIRouter(prefix="/wizard/seccion-1", tags=["wizard"])


@router.get("/orden-clausulada/opciones")
def get_orden_clausulada_opciones() -> dict:
    opciones = [
        {"id": "si", "label": "Sí"},
        {"id": "no", "label": "No"},
    ]
    return {"data": opciones}


@router.get("/id-servicio")
def get_siguiente_id_servicio() -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("ods")
            .select("id_servicio")
            .order("id_servicio", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    if response.data:
        siguiente = int(response.data[0]["id_servicio"]) + 1
    else:
        siguiente = 1

    return {"data": {"id_servicio": siguiente}}


@router.get("/profesionales")
def get_profesionales(programa: str | None = None) -> dict:
    client = get_supabase_client()
    query = client.table("profesionales").select("nombre_profesional")
    if programa:
        query = query.eq("programa", programa)

    try:
        response = query.execute()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    return {"data": response.data}


class Seccion1ConfirmarRequest(BaseModel):
    id_servicio: int
    orden_clausulada: str
    nombre_profesional: str


@router.post("/confirmar")
def confirmar_seccion_1(payload: Seccion1ConfirmarRequest) -> dict:
    orden = payload.orden_clausulada.strip().lower()
    if orden not in {"si", "no"}:
        raise HTTPException(
            status_code=422,
            detail="orden_clausulada debe ser 'si' o 'no'",
        )

    return {
        "data": {
            "id_servicio": payload.id_servicio,
            "orden_clausulada": orden,
            "nombre_profesional": payload.nombre_profesional.strip(),
        }
    }
