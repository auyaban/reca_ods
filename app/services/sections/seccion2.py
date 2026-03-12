from pydantic import BaseModel

from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import execute_with_reauth


def get_empresas() -> dict:
    try:
        page_size = 1000
        offset = 0
        rows = []
        while True:
            response = execute_with_reauth(
                lambda client: (
                    client.table("empresas")
                    .select("nit_empresa,nombre_empresa,caja_compensacion,asesor,zona_empresa")
                    .range(offset, offset + page_size - 1)
                    .execute()
                ),
                context="seccion2.get_empresas",
            )
            batch = list(response.data or [])
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": rows}


def get_empresa_por_nit(nit: str) -> dict:
    try:
        response = execute_with_reauth(
            lambda client: (
                client.table("empresas")
                .select(
                    "nit_empresa,nombre_empresa,caja_compensacion,asesor,zona_empresa"
                )
                .eq("nit_empresa", nit)
                .limit(1)
                .execute()
            ),
            context="seccion2.get_empresa_por_nit",
        )
    except SUPABASE_ERRORS as exc:
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
