from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.supabase_client import get_supabase_client

router = APIRouter(prefix="/wizard/seccion-1", tags=["wizard"])
_PROGRAMAS = {
    "inclusion laboral": "Inclusión Laboral",
    "inclusión laboral": "Inclusión Laboral",
    "interprete": "Interprete",
    "intérprete": "Interprete",
}


@router.get("/orden-clausulada/opciones")
def get_orden_clausulada_opciones() -> dict:
    opciones = [
        {"id": "si", "label": "Sí"},
        {"id": "no", "label": "No"},
    ]
    return {"data": opciones}


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
    orden_clausulada: str
    nombre_profesional: str


class CrearProfesionalRequest(BaseModel):
    nombre_profesional: str
    programa: str


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
            "orden_clausulada": orden,
            "nombre_profesional": payload.nombre_profesional.strip(),
        }
    }


@router.post("/profesionales")
def crear_profesional(payload: CrearProfesionalRequest) -> dict:
    nombre = " ".join(payload.nombre_profesional.strip().split())
    if not nombre:
        raise HTTPException(status_code=422, detail="nombre_profesional es obligatorio")
    nombre = " ".join([part.capitalize() for part in nombre.split(" ")])

    programa_key = " ".join(payload.programa.strip().lower().split())
    programa = _PROGRAMAS.get(programa_key)
    if not programa:
        raise HTTPException(status_code=422, detail="programa invalido")

    client = get_supabase_client()
    try:
        last = (
            client.table("profesionales")
            .select("id")
            .order("id", desc=True)
            .limit(1)
            .execute()
        )
        next_id = int(last.data[0]["id"]) + 1 if last.data else 1
        payload_db = {"id": next_id, "nombre_profesional": nombre, "programa": programa}
        response = client.table("profesionales").insert(payload_db).execute()
    except Exception as exc:
        message = str(exc)
        # Retry once if the id collided
        if "duplicate key value" in message:
            try:
                last = (
                    client.table("profesionales")
                    .select("id")
                    .order("id", desc=True)
                    .limit(1)
                    .execute()
                )
                next_id = int(last.data[0]["id"]) + 1 if last.data else 1
                payload_db = {"id": next_id, "nombre_profesional": nombre, "programa": programa}
                response = client.table("profesionales").insert(payload_db).execute()
            except Exception as exc2:
                raise HTTPException(status_code=502, detail=f"Supabase error: {exc2}") from exc2
        else:
            raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    return {"data": response.data}
