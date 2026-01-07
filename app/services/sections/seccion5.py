from datetime import date

from pydantic import BaseModel

from app.services.errors import ServiceError


class Seccion5ConfirmarRequest(BaseModel):
    fecha_servicio: str
    nombre_profesional: str | None = None
    nombre_empresa: str | None = None
    codigo_servicio: str | None = None
    observaciones: str | None = None
    observacion_agencia: str | None = None
    seguimiento_servicio: str | None = None


def confirmar_seccion_5(payload: Seccion5ConfirmarRequest) -> dict:
    try:
        fecha = date.fromisoformat(payload.fecha_servicio.strip())
    except ValueError as exc:
        raise ServiceError(
            "fecha_servicio debe tener formato YYYY-MM-DD", status_code=422
        ) from exc

    data = {
        "observaciones": (payload.observaciones or "").strip() or None,
        "observacion_agencia": (payload.observacion_agencia or "").strip() or None,
        "seguimiento_servicio": (payload.seguimiento_servicio or "").strip() or None,
        "mes_servicio": fecha.month,
        "año_servicio": fecha.year,
    }
    return {"data": data}


