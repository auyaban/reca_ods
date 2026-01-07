from pydantic import BaseModel, Field

from app.excel_sync import update_factura_sheet
from app.services.errors import ServiceError


class CrearFacturaRequest(BaseModel):
    mes: int = Field(ge=1, le=12)
    ano: int = Field(ge=2000)
    tipo: str


def crear_factura(payload: CrearFacturaRequest) -> dict:
    tipo = payload.tipo.strip().lower()
    if tipo not in {"clausulada", "no clausulada"}:
        raise ServiceError("tipo debe ser 'clausulada' o 'no clausulada'", status_code=422)

    try:
        update_factura_sheet(payload.mes, payload.ano, tipo)
    except ServiceError:
        raise
    except PermissionError as exc:
        raise ServiceError(
            "El archivo Excel esta abierto. Cierralo antes de crear la factura.",
            status_code=423,
        ) from exc
    except Exception as exc:
        raise ServiceError(f"No se pudo crear la factura: {exc}", status_code=500) from exc

    return {"data": {"mes": payload.mes, "ano": payload.ano, "tipo": tipo}}
