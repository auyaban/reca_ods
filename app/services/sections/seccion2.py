from pydantic import BaseModel

from app.catalog_index import get_company_detail_by_nit, get_indexed_empresas
from app.services.errors import SUPABASE_ERRORS, ServiceError


def get_empresas() -> dict:
    try:
        rows = list(get_indexed_empresas())
    except (RuntimeError, ValueError, TypeError, OSError) as exc:
        raise ServiceError(f"No se pudo leer indice local de empresas: {exc}", status_code=500) from exc

    return {"data": rows}


def get_empresa_por_nit(nit: str) -> dict:
    try:
        detail = get_company_detail_by_nit(nit)
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc
    except (RuntimeError, ValueError, TypeError, OSError) as exc:
        raise ServiceError(f"No se pudo leer detalle de empresa: {exc}", status_code=500) from exc

    return {"data": [detail] if detail else []}


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
