from pydantic import BaseModel

from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client


def get_empresas() -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("empresas")
            .select("nit_empresa,nombre_empresa,caja_compensacion,asesor,sede_empresa")
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


def get_empresa_por_nit(nit: str) -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("empresas")
            .select(
                "nit_empresa,nombre_empresa,caja_compensacion,asesor,sede_empresa"
            )
            .eq("nit_empresa", nit)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


class Seccion2ConfirmarRequest(BaseModel):
    nit_empresa: str
    nombre_empresa: str
    caja_compensacion: str | None = None
    asesor_empresa: str | None = None
    sede_empresa: str | None = None


def confirmar_seccion_2(payload: Seccion2ConfirmarRequest) -> dict:
    data = {
        "nit_empresa": payload.nit_empresa.strip(),
        "nombre_empresa": payload.nombre_empresa.strip(),
        "caja_compensacion": (payload.caja_compensacion or "").strip() or None,
        "asesor_empresa": (payload.asesor_empresa or "").strip() or None,
        "sede_empresa": (payload.sede_empresa or "").strip() or None,
    }
    return {"data": data}
