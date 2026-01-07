from collections import defaultdict

from app.supabase_client import get_supabase_client
from app.factura_models import FacturaItem
from app.services.errors import ServiceError


def calcular_items(mes: int, año: int, tipo: str) -> list[FacturaItem]:
    tipo_clean = tipo.strip().lower()
    clausulada = tipo_clean == "clausulada"

    client = get_supabase_client()
    try:
        ods = (
            client.table("ods")
            .select(
                "codigo_servicio,referencia_servicio,descripcion_servicio,horas_interprete,orden_clausulada,mes_servicio,año_servicio"
            )
            .eq("mes_servicio", mes)
            .eq("año_servicio", año)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    registros = ods.data or []
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
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    valor_por_codigo = {
        str(item.get("codigo_servicio")): float(item.get("valor_base") or 0)
        for item in (tarifas.data or [])
    }

    agrupados = defaultdict(lambda: {"cantidad": 0.0, "horas": 0.0})
    meta = {}
    for row in filtrados:
        codigo = str(row.get("codigo_servicio", "")).strip()
        referencia = str(row.get("referencia_servicio", "")).strip()
        descripcion = str(row.get("descripcion_servicio", "")).strip()
        key = (codigo, referencia, descripcion)
        meta[key] = {"codigo": codigo, "referencia": referencia, "descripcion": descripcion}
        horas = row.get("horas_interprete") or 0
        try:
            horas_val = float(horas or 0)
        except (TypeError, ValueError):
            horas_val = 0.0
        if horas_val > 0:
            agrupados[key]["horas"] += horas_val
        else:
            agrupados[key]["cantidad"] += 1

    items: list[FacturaItem] = []
    for key, agg in agrupados.items():
        codigo = meta[key]["codigo"]
        referencia = meta[key]["referencia"]
        descripcion = meta[key]["descripcion"]
        valor_base = valor_por_codigo.get(codigo, 0.0)
        cantidad = agg["horas"] if agg["horas"] > 0 else agg["cantidad"]
        total = valor_base * cantidad
        items.append(
            FacturaItem(
                codigo_servicio=codigo,
                referencia_servicio=referencia,
                descripcion_servicio=descripcion,
                valor_base=valor_base,
                cantidad=cantidad,
                total=total,
            )
        )

    items.sort(key=lambda item: item.codigo_servicio)
    return items
