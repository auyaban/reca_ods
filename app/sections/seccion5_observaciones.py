from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/wizard/seccion-5", tags=["wizard"])


class Seccion5ConfirmarRequest(BaseModel):
    fecha_servicio: str
    nombre_profesional: str | None = None
    nombre_empresa: str | None = None
    codigo_servicio: str | None = None
    observaciones: str | None = None
    observacion_agencia: str | None = None
    seguimiento_servicio: str | None = None


@router.post("/confirmar")
def confirmar_seccion_5(payload: Seccion5ConfirmarRequest) -> dict:
    try:
        fecha = date.fromisoformat(payload.fecha_servicio.strip())
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail="fecha_servicio debe tener formato YYYY-MM-DD"
        ) from exc

    data = {
        "observaciones": (payload.observaciones or "").strip() or None,
        "observacion_agencia": (payload.observacion_agencia or "").strip() or None,
        "seguimiento_servicio": (payload.seguimiento_servicio or "").strip() or None,
        "mes_servicio": fecha.month,
        "año_servicio": fecha.year,
    }
    return {"data": data}


class ResumenServicioRequest(BaseModel):
    fecha_servicio: str
    nombre_profesional: str
    nombre_empresa: str
    codigo_servicio: str
    valor_total: float


@router.post("/resumen")
def resumen_servicio(payload: ResumenServicioRequest) -> dict:
    try:
        date.fromisoformat(payload.fecha_servicio.strip())
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail="fecha_servicio debe tener formato YYYY-MM-DD"
        ) from exc

    data = {
        "fecha_servicio": payload.fecha_servicio.strip(),
        "nombre_profesional": payload.nombre_profesional.strip(),
        "nombre_empresa": payload.nombre_empresa.strip(),
        "codigo_servicio": payload.codigo_servicio.strip(),
        "valor_total": payload.valor_total,
    }
    return {"data": data}
