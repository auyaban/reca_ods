from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from app.google_drive_sync import get_existing_monthly_spreadsheet, resolve_monthly_spreadsheet_name
from app.google_sheets_client import read_sheet_values
from app.supabase_client import execute_with_reauth
from app.utils.text import normalize_text

ODS_CALCULADA_HEADERS = [
    "ID",
    "PROFESIONAL",
    "NUEVO CÓDIGO",
    "EMPRESA",
    "NIT",
    "CCF",
    "FECHA",
    "REFERENCIA",
    "NOMBRE",
    "OFERENTES",
    "CEDULA",
    "TIPO DE DISCAPACIDAD",
    "FECHA INGRESO",
    "VALOR SERVICIO VIRTUAL",
    "VALOR SERVICIO BOGOTÁ",
    "VALOR FUERA DE BOGOTA",
    "TODAS LAS MODALIDADES",
    "TOTAL HORAS",
    "VALOR A PAGAR",
    "TOTAL VALOR SERVICIO SIN IVA",
    "OBSERVACIONES",
    "ASESOR",
    "SEDE",
    "MODALIDAD",
    "OBSERVACION AGENCIA",
    "CLAUSULADA",
    "MES",
    "GENERO",
    "TIPO DE CONTRATO",
    "SEGUIMIENTO",
    "CARGO",
    "PERSONAS",
    "AÑO",
]

SHEET_FIELDS_IN_ORDER = [
    "id",
    "nombre_profesional",
    "codigo_servicio",
    "nombre_empresa",
    "nit_empresa",
    "caja_compensacion",
    "fecha_servicio",
    "referencia_servicio",
    "descripcion_servicio",
    "nombre_usuario",
    "cedula_usuario",
    "discapacidad_usuario",
    "fecha_ingreso",
    "valor_virtual",
    "valor_bogota",
    "valor_otro",
    "todas_modalidades",
    "horas_interprete",
    "valor_interprete",
    "valor_total",
    "observaciones",
    "asesor_empresa",
    "sede_empresa",
    "modalidad_servicio",
    "observacion_agencia",
    "orden_clausulada",
    "mes_servicio",
    "genero_usuario",
    "tipo_contrato",
    "seguimiento_servicio",
    "cargo_servicio",
    "total_personas",
    "ano_servicio",
]

TEXT_FIELDS = {
    "nombre_profesional",
    "codigo_servicio",
    "nombre_empresa",
    "nit_empresa",
    "caja_compensacion",
    "referencia_servicio",
    "descripcion_servicio",
    "nombre_usuario",
    "cedula_usuario",
    "discapacidad_usuario",
    "observaciones",
    "asesor_empresa",
    "sede_empresa",
    "modalidad_servicio",
    "observacion_agencia",
    "genero_usuario",
    "tipo_contrato",
    "seguimiento_servicio",
    "cargo_servicio",
}
DATE_FIELDS = {"fecha_servicio", "fecha_ingreso"}
FLOAT_FIELDS = {
    "valor_virtual",
    "valor_bogota",
    "valor_otro",
    "todas_modalidades",
    "horas_interprete",
    "valor_interprete",
    "valor_total",
}
INT_FIELDS = {"mes_servicio", "ano_servicio", "total_personas"}
BOOL_FIELDS = {"orden_clausulada"}
DB_YEAR_FIELD = "año_servicio"
SELECT_FIELDS = (
    "id,codigo_servicio,referencia_servicio,descripcion_servicio,"
    "nombre_profesional,nombre_empresa,nit_empresa,caja_compensacion,"
    "asesor_empresa,sede_empresa,fecha_servicio,fecha_ingreso,mes_servicio,año_servicio,"
    "nombre_usuario,cedula_usuario,discapacidad_usuario,genero_usuario,modalidad_servicio,"
    "todas_modalidades,horas_interprete,valor_virtual,valor_bogota,valor_otro,"
    "valor_interprete,valor_total,tipo_contrato,cargo_servicio,seguimiento_servicio,"
    "orden_clausulada,total_personas,observaciones,observacion_agencia"
)
_NORMALIZED_HEADERS = [normalize_text(header, lowercase=True) for header in ODS_CALCULADA_HEADERS]


def _normalize_text_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def _normalize_bool(value: Any) -> str:
    text = _normalize_text_value(value).casefold()
    if text in {"si", "sí", "s", "true", "1"}:
        return "SI"
    if text in {"no", "n", "false", "0"}:
        return "NO"
    return _normalize_text_value(value)


def _normalize_number(value: Any) -> str:
    text = _normalize_text_value(value)
    if not text:
        return ""
    cleaned = text.replace("$", "").replace(" ", "")
    cleaned = "".join(ch for ch in cleaned if ch.isdigit() or ch in {",", ".", "-"})
    if not cleaned:
        return ""
    if "." in cleaned and "," in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "")
    elif cleaned.count(",") > 1:
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        before_comma, after_comma = cleaned.split(",", 1)
        if len(after_comma) == 3 and after_comma.isdigit():
            cleaned = before_comma + after_comma
        else:
            cleaned = cleaned.replace(",", ".")
    elif "." in cleaned and cleaned.replace(".", "").isdigit() and cleaned.count(".") == 1:
        left, right = cleaned.split(".", 1)
        if len(right) == 3 and left.replace("-", "").isdigit():
            cleaned = left + right
    try:
        number = float(cleaned)
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return str(number).rstrip("0").rstrip(".")


def _normalize_date(value: Any) -> str:
    text = _normalize_text_value(value)
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def _normalize_field(field_name: str, value: Any) -> str:
    if field_name in BOOL_FIELDS:
        return _normalize_bool(value)
    if field_name in FLOAT_FIELDS or field_name in INT_FIELDS:
        return _normalize_number(value)
    if field_name in DATE_FIELDS:
        return _normalize_date(value)
    return _normalize_text_value(value)


def _typed_value(field_name: str, value: str) -> Any:
    if field_name in TEXT_FIELDS or field_name in DATE_FIELDS:
        return value or None
    if field_name in BOOL_FIELDS:
        if not value:
            return None
        return value == "SI"
    if field_name in FLOAT_FIELDS:
        if not value:
            return None if field_name == "horas_interprete" else 0.0
        return float(value)
    if field_name in INT_FIELDS:
        if not value:
            return 0 if field_name == "total_personas" else None
        return int(float(value))
    return value or None


def _supabase_value(row: dict[str, Any], field_name: str) -> Any:
    if field_name == "ano_servicio":
        return row.get("ano_servicio", row.get(DB_YEAR_FIELD))
    return row.get(field_name)


def _db_field_name(field_name: str) -> str:
    if field_name == "ano_servicio":
        return DB_YEAR_FIELD
    return field_name


def _validate_headers(raw_headers: list[Any], sheet_name: str) -> None:
    headers = [normalize_text(value, lowercase=True) for value in raw_headers]
    if headers != _NORMALIZED_HEADERS:
        raise RuntimeError(
            f"Encabezados inesperados en {sheet_name}. No coincide con ODS_CALCULADA."
        )


def _row_to_record(raw_row: list[Any]) -> dict[str, str]:
    padded = list(raw_row) + [""] * max(0, len(SHEET_FIELDS_IN_ORDER) - len(raw_row))
    record: dict[str, str] = {}
    for idx, field_name in enumerate(SHEET_FIELDS_IN_ORDER):
        record[field_name] = _normalize_field(field_name, padded[idx])
    return record


def _fetch_sheet_records(spreadsheet_id: str, sheet_name: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    raw_rows = read_sheet_values(spreadsheet_id, f"'{sheet_name}'!A:AG")
    if not raw_rows:
        raise RuntimeError(f"La hoja {sheet_name} no devolvio datos.")
    _validate_headers(raw_rows[0], sheet_name)

    records_by_id: dict[str, dict[str, Any]] = {}
    ignored_rows_without_id: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    duplicate_rows_by_id: dict[str, list[dict[str, Any]]] = {}

    for row_number, raw_row in enumerate(raw_rows[1:], start=2):
        record = _row_to_record(raw_row)
        if not any(str(value).strip() for value in raw_row):
            continue
        record_id = record["id"]
        if not record_id:
            ignored_rows_without_id.append(
                {
                    "sheet_row": row_number,
                    "reason": "missing_id",
                    "preview": [str(value).strip() for value in raw_row[:5]],
                }
            )
            continue
        current_entry = {"sheet_row": row_number, "record": record}
        if record_id in duplicate_rows_by_id:
            duplicate_rows_by_id[record_id].append(current_entry)
            continue
        if record_id in records_by_id:
            duplicate_rows_by_id[record_id] = [records_by_id.pop(record_id), current_entry]
            continue
        records_by_id[record_id] = current_entry

    for record_id, entries in duplicate_rows_by_id.items():
        for entry in entries:
            invalid_rows.append(
                {
                    "sheet_row": entry["sheet_row"],
                    "id": record_id,
                    "reason": "duplicate_id",
                }
            )
    invalid_rows.sort(key=lambda item: (item.get("sheet_row") or 0, str(item.get("id") or "")))
    return records_by_id, ignored_rows_without_id, invalid_rows


def _supabase_row_to_record(row: dict[str, Any]) -> dict[str, str]:
    record: dict[str, str] = {}
    for field_name in SHEET_FIELDS_IN_ORDER:
        record[field_name] = _normalize_field(field_name, _supabase_value(row, field_name))
    return record


def _fetch_supabase_rows(month: int, year: int) -> dict[str, dict[str, Any]]:
    page_size = 500
    offset = 0
    rows_by_id: dict[str, dict[str, Any]] = {}
    while True:
        response = execute_with_reauth(
            lambda client, offset=offset: (
                client.table("ods")
                .select(SELECT_FIELDS)
                .eq("mes_servicio", month)
                .eq(DB_YEAR_FIELD, year)
                .order("fecha_servicio")
                .order("created_at")
                .range(offset, offset + page_size - 1)
                .execute()
            ),
            context="google_sheet_supabase_sync.fetch_supabase",
        )
        batch = list(response.data or [])
        if not batch:
            break
        for row in batch:
            record_id = str(row.get("id") or "").strip()
            if not record_id:
                continue
            rows_by_id[record_id] = {
                "row": row,
                "normalized": _supabase_row_to_record(row),
            }
        if len(batch) < page_size:
            break
        offset += page_size
    return rows_by_id


def _build_update_payload(sheet_record: dict[str, str], supabase_record: dict[str, str]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    payload: dict[str, Any] = {}
    diffs: list[dict[str, str]] = []
    for field_name in SHEET_FIELDS_IN_ORDER:
        if field_name == "id":
            continue
        sheet_value = sheet_record[field_name]
        supabase_value = supabase_record[field_name]
        if sheet_value == supabase_value:
            continue
        payload[field_name] = _typed_value(field_name, sheet_value)
        diffs.append(
            {
                "field": field_name,
                "sheet_value": sheet_value,
                "supabase_value": supabase_value,
            }
        )
    return payload, diffs


def preview_google_sheet_supabase_sync(month: int, year: int, *, sheet_name: str = "ODS_CALCULADA") -> dict[str, Any]:
    monthly = get_existing_monthly_spreadsheet(month, year)
    spreadsheet_id = str(monthly.get("id") or "").strip()
    spreadsheet_name = str(monthly.get("name") or resolve_monthly_spreadsheet_name(month, year))

    sheet_records_by_id, ignored_rows_without_id, invalid_rows = _fetch_sheet_records(
        spreadsheet_id,
        sheet_name,
    )
    supabase_rows_by_id = _fetch_supabase_rows(month, year)

    sheet_ids = set(sheet_records_by_id)
    supabase_ids = set(supabase_rows_by_id)
    common_ids = sorted(sheet_ids & supabase_ids)
    only_in_sheet = [
        {"id": record_id, "sheet_row": sheet_records_by_id[record_id]["sheet_row"]}
        for record_id in sorted(sheet_ids - supabase_ids)
    ]
    only_in_supabase = [{"id": record_id} for record_id in sorted(supabase_ids - sheet_ids)]

    changed_field_counts: Counter[str] = Counter()
    changed_records: list[dict[str, Any]] = []

    for record_id in common_ids:
        sheet_entry = sheet_records_by_id[record_id]
        supabase_entry = supabase_rows_by_id[record_id]
        try:
            update_payload, diffs = _build_update_payload(
                sheet_entry["record"],
                supabase_entry["normalized"],
            )
        except (TypeError, ValueError) as exc:
            invalid_rows.append(
                {
                    "sheet_row": sheet_entry["sheet_row"],
                    "id": record_id,
                    "reason": f"invalid_value: {exc}",
                }
            )
            continue
        if not diffs:
            continue
        for diff in diffs:
            changed_field_counts[diff["field"]] += 1
        changed_records.append(
            {
                "id": record_id,
                "sheet_row": sheet_entry["sheet_row"],
                "supabase_id": record_id,
                "diff_count": len(diffs),
                "diffs": diffs,
                "updated_fields": sorted(update_payload),
                "update_payload": update_payload,
            }
        )

    changed_records.sort(key=lambda item: (-item["diff_count"], item["sheet_row"], item["id"]))
    invalid_rows.sort(key=lambda item: (item.get("sheet_row") or 0, str(item.get("id") or "")))

    return {
        "month": month,
        "year": year,
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_name": spreadsheet_name,
        "spreadsheet_target": resolve_monthly_spreadsheet_name(month, year),
        "sheet_name": sheet_name,
        "sheet_record_count": len(sheet_records_by_id),
        "supabase_row_count": len(supabase_rows_by_id),
        "common_id_count": len(common_ids),
        "changed_record_count": len(changed_records),
        "changed_field_counts": dict(changed_field_counts.most_common()),
        "changed_records": changed_records,
        "only_in_sheet_count": len(only_in_sheet),
        "only_in_sheet": only_in_sheet,
        "only_in_supabase_count": len(only_in_supabase),
        "only_in_supabase": only_in_supabase,
        "ignored_rows_without_id": ignored_rows_without_id,
        "invalid_rows": invalid_rows,
    }


def _apply_update(record_id: str, payload: dict[str, Any]) -> None:
    db_payload = {_db_field_name(key): value for key, value in payload.items()}
    execute_with_reauth(
        lambda client, record_id=record_id, payload=db_payload: (
            client.table("ods").update(payload).eq("id", record_id).execute()
        ),
        context=f"google_sheet_supabase_sync.update.{record_id}",
    )


def apply_google_sheet_supabase_sync(
    month: int,
    year: int,
    *,
    selected_ids: list[str] | None = None,
    sheet_name: str = "ODS_CALCULADA",
) -> dict[str, Any]:
    report = preview_google_sheet_supabase_sync(month, year, sheet_name=sheet_name)
    selected_set = {str(item).strip() for item in (selected_ids or []) if str(item).strip()}
    changed_records = list(report["changed_records"])
    if selected_set:
        changed_records = [item for item in changed_records if item["id"] in selected_set]

    updated_records: list[dict[str, Any]] = []
    failed_records: list[dict[str, Any]] = []
    applied_field_count = 0
    for item in changed_records:
        payload = dict(item.get("update_payload") or {})
        if not payload:
            continue
        try:
            _apply_update(item["id"], payload)
        except Exception as exc:
            failed_records.append(
                {
                    "id": item["id"],
                    "sheet_row": item["sheet_row"],
                    "updated_fields": sorted(payload),
                    "error": str(exc),
                }
            )
            continue
        applied_field_count += len(payload)
        updated_records.append(
            {
                "id": item["id"],
                "sheet_row": item["sheet_row"],
                "updated_fields": sorted(payload),
            }
        )

    return {
        "month": month,
        "year": year,
        "spreadsheet_id": report["spreadsheet_id"],
        "spreadsheet_name": report["spreadsheet_name"],
        "sheet_name": sheet_name,
        "preview_changed_record_count": report["changed_record_count"],
        "requested_id_count": len(selected_set) if selected_set else len(changed_records),
        "applied_record_count": len(updated_records),
        "applied_field_count": applied_field_count,
        "updated_records": updated_records,
        "failed_record_count": len(failed_records),
        "failed_records": failed_records,
        "skipped_ids": sorted(selected_set - {item["id"] for item in changed_records}) if selected_set else [],
        "only_in_sheet_count": report["only_in_sheet_count"],
        "only_in_supabase_count": report["only_in_supabase_count"],
        "invalid_row_count": len(report["invalid_rows"]),
        "ignored_rows_without_id_count": len(report["ignored_rows_without_id"]),
    }
