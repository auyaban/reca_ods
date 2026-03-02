from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from app.factura_models import FacturaItem
from app.services.errors import SUPABASE_ERRORS, ServiceError
from app.supabase_client import get_supabase_client


_MONEY_QUANT = Decimal("0.01")
_HOURS_QUANT = Decimal("0.01")
_ODS_PAGE_SIZE = 1000


def _to_decimal(value) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError):
        return Decimal("0")


def _validate_period(mes: int, ano: int) -> None:
    if not 1 <= int(mes) <= 12:
        raise ServiceError("Mes invalido. Debe estar entre 1 y 12.", status_code=422)
    if not 2000 <= int(ano) <= 2100:
        raise ServiceError("Ano invalido. Debe estar entre 2000 y 2100.", status_code=422)


def _decimal_to_float(value: Decimal, quant: Decimal) -> float:
    quantized = value.quantize(quant, rounding=ROUND_HALF_UP)
    return float(format(quantized, "f"))


def _fetch_ods_rows_for_period(mes: int, ano: int) -> list[dict]:
    client = get_supabase_client()
    rows: list[dict] = []
    last_id: int | None = None

    while True:
        try:
            query = (
                client.table("ods")
                .select(
                    "id,codigo_servicio,referencia_servicio,descripcion_servicio,horas_interprete,orden_clausulada,mes_servicio,a\u00f1o_servicio"
                )
                .eq("mes_servicio", mes)
                .eq("a\u00f1o_servicio", ano)
                .order("id")
                .limit(_ODS_PAGE_SIZE)
            )
            if last_id is not None:
                query = query.gt("id", last_id)
            response = query.execute()
        except SUPABASE_ERRORS as exc:
            raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

        batch = list(response.data or [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < _ODS_PAGE_SIZE:
            break

        last_raw = batch[-1].get("id")
        if last_raw in (None, ""):
            raise ServiceError(
                "No se pudo paginar ODS: falta columna id en respuesta de Supabase.",
                status_code=502,
            )
        try:
            next_last_id = int(last_raw)
        except (TypeError, ValueError) as exc:
            raise ServiceError(
                "No se pudo paginar ODS: id invalido en respuesta de Supabase.",
                status_code=502,
            ) from exc
        if last_id is not None and next_last_id <= last_id:
            raise ServiceError(
                "No se pudo paginar ODS: secuencia de id no monotona.",
                status_code=502,
            )
        last_id = next_last_id

    return rows


def calcular_items(mes: int, ano: int, tipo: str) -> list[FacturaItem]:
    _validate_period(mes, ano)
    tipo_clean = tipo.strip().lower()
    clausulada = tipo_clean == "clausulada"

    client = get_supabase_client()
    registros = _fetch_ods_rows_for_period(mes, ano)
    if not registros:
        raise ServiceError("No se encontraron servicios para el periodo", status_code=404)

    filtrados = []
    for row in registros:
        orden = str(row.get("orden_clausulada", "")).strip().lower()
        es_clausulada = orden.startswith("s") or orden == "true"
        if es_clausulada == clausulada:
            filtrados.append(row)

    if not filtrados:
        raise ServiceError("No hay servicios para el tipo solicitado", status_code=404)

    codigos = sorted(
        {str(row.get("codigo_servicio", "")).strip() for row in filtrados if row.get("codigo_servicio")}
    )
    if not codigos:
        raise ServiceError("No hay codigos en el periodo", status_code=422)

    try:
        tarifas = (
            client.table("tarifas")
            .select("codigo_servicio,valor_base")
            .in_("codigo_servicio", codigos)
            .execute()
        )
    except SUPABASE_ERRORS as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    valor_por_codigo = {
        str(item.get("codigo_servicio")): _to_decimal(item.get("valor_base") or 0)
        for item in (tarifas.data or [])
    }

    agrupados = defaultdict(lambda: {"cantidad": Decimal("0"), "horas": Decimal("0")})
    meta = {}
    for row in filtrados:
        codigo = str(row.get("codigo_servicio", "")).strip()
        referencia = str(row.get("referencia_servicio", "")).strip()
        descripcion = str(row.get("descripcion_servicio", "")).strip()
        key = (codigo, referencia, descripcion)
        meta[key] = {"codigo": codigo, "referencia": referencia, "descripcion": descripcion}
        horas = row.get("horas_interprete") or 0
        horas_val = _to_decimal(horas).quantize(_HOURS_QUANT, rounding=ROUND_HALF_UP)
        if horas_val > 0:
            agrupados[key]["horas"] += horas_val
        else:
            agrupados[key]["cantidad"] += Decimal("1")

    items: list[FacturaItem] = []
    for key, agg in agrupados.items():
        codigo = meta[key]["codigo"]
        referencia = meta[key]["referencia"]
        descripcion = meta[key]["descripcion"]
        valor_base = valor_por_codigo.get(codigo, Decimal("0")).quantize(
            _MONEY_QUANT, rounding=ROUND_HALF_UP
        )
        cantidad = agg["horas"] if agg["horas"] > 0 else agg["cantidad"]
        total = (valor_base * cantidad).quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)
        items.append(
            FacturaItem(
                codigo_servicio=codigo,
                referencia_servicio=referencia,
                descripcion_servicio=descripcion,
                valor_base=_decimal_to_float(valor_base, _MONEY_QUANT),
                cantidad=_decimal_to_float(cantidad, _HOURS_QUANT),
                total=_decimal_to_float(total, _MONEY_QUANT),
            )
        )

    items.sort(key=lambda item: item.codigo_servicio)
    return items
