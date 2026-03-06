from datetime import date
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import math

from pydantic import BaseModel

from app.logging_utils import LOGGER_BACKEND, get_file_logger
from app.domain.service_calculation import CalculoServicioInput, calcular_servicio
from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import execute_with_reauth

_ROOT_DIR = Path(__file__).resolve().parents[3]
_LOG_FILE = _ROOT_DIR / "logs" / "backend.log"

_logger = get_file_logger(LOGGER_BACKEND, _LOG_FILE, announce=True)


def _to_decimal(value: float | int | str) -> Decimal:
    try:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(value)
        parsed = Decimal(str(value))
    except (ValueError, ArithmeticError) as exc:
        raise ServiceError(f"Valor numerico invalido: {value}", status_code=422) from exc
    if not parsed.is_finite():
        raise ServiceError(f"Valor numerico no finito: {value}", status_code=422)
    return parsed


def get_codigos_servicio() -> dict:
    try:
        response = execute_with_reauth(
            lambda client: (
                client.table("tarifas")
                .select(
                    "codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base"
                )
                .execute()
            ),
            context="seccion3.get_codigos_servicio",
        )
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


def get_tarifa_por_codigo(codigo: str) -> dict:
    try:
        response = execute_with_reauth(
            lambda client: (
                client.table("tarifas")
                .select(
                    "codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base"
                )
                .eq("codigo_servicio", codigo)
                .limit(1)
                .execute()
            ),
            context="seccion3.get_tarifa_por_codigo",
        )
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


class Seccion3ConfirmarRequest(BaseModel):
    fecha_servicio: str
    codigo_servicio: str
    referencia_servicio: str
    descripcion_servicio: str
    modalidad_servicio: str
    valor_base: float
    servicio_interpretacion: bool = False
    horas_interprete: int | None = None
    minutos_interprete: int | None = None


def confirmar_seccion_3(payload: Seccion3ConfirmarRequest) -> dict:
    try:
        date.fromisoformat(payload.fecha_servicio.strip())
    except ValueError as exc:
        raise ServiceError(
            "fecha_servicio debe tener formato YYYY-MM-DD", status_code=422
        ) from exc

    try:
        resultado = calcular_servicio(
            CalculoServicioInput(
                fecha_servicio=payload.fecha_servicio.strip(),
                codigo_servicio=payload.codigo_servicio.strip(),
                modalidad_servicio=payload.modalidad_servicio,
                valor_base=payload.valor_base,
                servicio_interpretacion=payload.servicio_interpretacion,
                horas_interprete=payload.horas_interprete,
                minutos_interprete=payload.minutos_interprete,
            )
        )
    except ValueError as exc:
        raise ServiceError(str(exc), status_code=422) from exc

    data = {
        "fecha_servicio": payload.fecha_servicio.strip(),
        "codigo_servicio": payload.codigo_servicio.strip(),
        "referencia_servicio": payload.referencia_servicio.strip(),
        "descripcion_servicio": payload.descripcion_servicio.strip(),
        "modalidad_servicio": payload.modalidad_servicio.strip(),
        "valor_virtual": float(resultado.valor_virtual.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "valor_bogota": float(resultado.valor_bogota.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "valor_otro": float(resultado.valor_otro.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "todas_modalidades": float(resultado.todas_modalidades.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "horas_interprete": (
            float(resultado.horas_interprete.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            if resultado.horas_interprete is not None
            else None
        ),
        "valor_interprete": float(resultado.valor_interprete.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "valor_total": float(resultado.valor_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
    }
    return {"data": data}
