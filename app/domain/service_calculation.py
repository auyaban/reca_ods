from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import math

from app.utils.text import normalize_text

_MONEY_QUANT = Decimal("0.01")
_HOURS_QUANT = Decimal("0.01")


@dataclass(frozen=True)
class CalculoServicioInput:
    fecha_servicio: str = ""
    codigo_servicio: str = ""
    modalidad_servicio: str = ""
    valor_base: Decimal | int | float | str = Decimal("0")
    servicio_interpretacion: bool = False
    horas_interprete: int | None = None
    minutos_interprete: int | None = None


@dataclass(frozen=True)
class CalculoServicioOutput:
    valor_virtual: Decimal
    valor_bogota: Decimal
    valor_otro: Decimal
    todas_modalidades: Decimal
    horas_interprete: Decimal | None
    valor_interprete: Decimal
    valor_total: Decimal


def _to_decimal(value: Decimal | int | float | str, *, field_name: str) -> Decimal:
    try:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(field_name)
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"{field_name}: valor numerico invalido ({value!r})") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name}: valor no finito ({value!r})")
    return parsed


def _normalize_modalidad(modalidad_servicio: str) -> str:
    modalidad = normalize_text(modalidad_servicio)
    if "toda" in modalidad and "modalidad" in modalidad:
        return "todas las modalidades"
    if "fuera" in modalidad or "otro" in modalidad:
        return "fuera de bogota"
    if "bogota" in modalidad:
        return "bogota"
    if "virtual" in modalidad:
        return "virtual"
    raise ValueError("modalidad_servicio invalida")


def _calcular_horas_decimales(horas: int | None, minutos: int | None) -> Decimal:
    if horas is None or minutos is None:
        raise ValueError("Debe indicar horas_interprete y minutos_interprete")
    if horas < 0 or minutos < 0:
        raise ValueError("Horas/minutos invalidos")
    return (
        _to_decimal(horas, field_name="horas_interprete")
        + (_to_decimal(minutos, field_name="minutos_interprete") / Decimal("60"))
    ).quantize(_HOURS_QUANT, rounding=ROUND_HALF_UP)


def calcular_servicio(data: CalculoServicioInput) -> CalculoServicioOutput:
    valor_base = _to_decimal(data.valor_base, field_name="valor_base").quantize(
        _MONEY_QUANT, rounding=ROUND_HALF_UP
    )
    modalidad = _normalize_modalidad(data.modalidad_servicio)

    valor_virtual = Decimal("0")
    valor_bogota = Decimal("0")
    valor_otro = Decimal("0")
    todas_modalidades = Decimal("0")

    if modalidad == "virtual":
        valor_virtual = valor_base
    elif modalidad == "bogota":
        valor_bogota = valor_base
    elif modalidad == "fuera de bogota":
        valor_otro = valor_base
    elif modalidad == "todas las modalidades":
        todas_modalidades = valor_base

    horas_decimales: Decimal | None = None
    valor_interprete = Decimal("0")
    if data.servicio_interpretacion:
        horas_decimales = _calcular_horas_decimales(data.horas_interprete, data.minutos_interprete)
        valor_interprete = (horas_decimales * valor_base).quantize(
            _MONEY_QUANT, rounding=ROUND_HALF_UP
        )

    base_total = (
        valor_virtual + valor_bogota + valor_otro + todas_modalidades
    ).quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)
    valor_total = valor_interprete if (data.servicio_interpretacion and horas_decimales is not None) else base_total

    return CalculoServicioOutput(
        valor_virtual=valor_virtual,
        valor_bogota=valor_bogota,
        valor_otro=valor_otro,
        todas_modalidades=todas_modalidades,
        horas_interprete=horas_decimales,
        valor_interprete=valor_interprete,
        valor_total=valor_total,
    )

