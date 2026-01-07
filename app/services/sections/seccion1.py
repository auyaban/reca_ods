from pydantic import BaseModel

from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client

_PROGRAMAS = {
    "inclusion laboral": "Inclusión Laboral",
    "inclusión laboral": "Inclusión Laboral",
    "interprete": "Interprete",
    "intérprete": "Interprete",
}



def get_orden_clausulada_opciones() -> dict:
    opciones = [
        {"id": "si", "label": "Sí"},
        {"id": "no", "label": "No"},
    ]
    return {"data": opciones}


def get_profesionales(programa: str | None = None) -> dict:
    client = get_supabase_client()
    try:
        profesionales_query = client.table("profesionales").select("nombre_profesional")
        if programa:
            profesionales_query = profesionales_query.eq("programa", programa)
        profesionales = profesionales_query.execute().data or []

        interpretes = []
        if not programa or programa.lower().strip() in {"interprete", "intérprete"}:
            interpretes = (
                client.table("interpretes")
                .select("nombre")
                .execute()
            ).data or []
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    nombres = []
    for item in profesionales:
        nombre = (item.get("nombre_profesional") or "").strip()
        if nombre:
            nombres.append(nombre)
    for item in interpretes:
        nombre = (item.get("nombre") or "").strip()
        if nombre:
            nombres.append(nombre)

    nombres = sorted(set(nombres), key=lambda value: value.lower())
    return {"data": [{"nombre_profesional": nombre} for nombre in nombres]}


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

    programa_key = " ".join(payload.programa.strip().lower().split())
    programa = _PROGRAMAS.get(programa_key)
    if not programa:
        raise ServiceError("programa invalido", status_code=422)

    client = get_supabase_client()
    try:
        if programa.lower() == "interprete":
            client.table("interpretes").insert({"nombre": nombre}).execute()
            return {"data": {"nombre_profesional": nombre}}
        last = (
            client.table("profesionales")
            .select("id")
            .order("id", desc=True)
            .limit(1)
            .execute()
        )
        next_id = int(last.data[0]["id"]) + 1 if last.data else 1
        payload_db = {"id": next_id, "nombre_profesional": nombre, "programa": programa}
        client.table("profesionales").insert(payload_db).execute()
    except Exception as exc:
        message = str(exc)
        if "duplicate key value" in message and programa.lower() != "interprete":
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
                client.table("profesionales").insert(payload_db).execute()
            except Exception as exc2:
                raise ServiceError(f"Supabase error: {exc2}", status_code=502) from exc2
        else:
            raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": {"nombre_profesional": nombre}}
