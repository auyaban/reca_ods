from __future__ import annotations

from typing import Any

from app.utils.text import normalize_text

ODS_INPUT_HEADERS = [
    "ID",
    "PROFESIONAL",
    "NUEVO C\u00d3DIGO",
    "EMPRESA",
    "NIT",
    "CCF",
    "FECHA",
    "OFERENTES",
    "CEDULA",
    "TIPO DE DISCAPACIDAD",
    "FECHA INGRESO",
    "OBSERVACIONES",
    "MODALIDAD",
    "CLAUSULADA",
    "GENERO",
    "TIPO DE CONTRATO",
    "ASESOR",
    "SEDE",
    "OBSERVACION AGENCIA",
    "SEGUIMIENTO",
    "CARGO",
    "PERSONAS",
    "TOTAL HORAS",
    "MES",
    "A\u00d1O",
]

_YEAR_FIELD_ALIASES = (
    "ano_servicio",
    "a\u00f1o_servicio",
    "a\u00c3\u00b1o_servicio",
    "a?o_servicio",
    "a\u00ef\u00bf\u00bdo_servicio",
    "a\u00c3\u0192\u00c2\u00b1o_servicio",
)


def to_sheet_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value)


def to_sheet_number_or_blank(value: Any) -> Any:
    if value in (None, ""):
        return ""
    try:
        return float(value)
    except (TypeError, ValueError):
        return ""


def bool_to_si_no(value: Any) -> str:
    if value is True:
        return "SI"
    if value is False:
        return "NO"
    text = str(value or "").strip().lower()
    if text in {"true", "t", "1", "si", "s\u00ed"}:
        return "SI"
    return "NO"


def get_year_value(row: dict[str, Any]) -> Any:
    for key in _YEAR_FIELD_ALIASES:
        if key in row:
            return row.get(key)
    return None


def ods_input_row_from_record(row: dict[str, Any]) -> list[Any]:
    return [
        to_sheet_text(row.get("id")),
        to_sheet_text(row.get("nombre_profesional")),
        to_sheet_text(row.get("codigo_servicio")),
        to_sheet_text(row.get("nombre_empresa")),
        to_sheet_text(row.get("nit_empresa")),
        to_sheet_text(row.get("caja_compensacion")),
        to_sheet_text(row.get("fecha_servicio")),
        to_sheet_text(row.get("nombre_usuario")),
        to_sheet_text(row.get("cedula_usuario")),
        to_sheet_text(row.get("discapacidad_usuario")),
        to_sheet_text(row.get("fecha_ingreso")),
        to_sheet_text(row.get("observaciones")),
        to_sheet_text(row.get("modalidad_servicio")),
        bool_to_si_no(row.get("orden_clausulada")),
        to_sheet_text(row.get("genero_usuario")),
        to_sheet_text(row.get("tipo_contrato")),
        to_sheet_text(row.get("asesor_empresa")),
        to_sheet_text(row.get("sede_empresa")),
        to_sheet_text(row.get("observacion_agencia")),
        to_sheet_text(row.get("seguimiento_servicio")),
        to_sheet_text(row.get("cargo_servicio")),
        int(row.get("total_personas") or 0),
        to_sheet_number_or_blank(row.get("horas_interprete")),
        int(row.get("mes_servicio") or 0),
        int(get_year_value(row) or 0),
    ]


def build_ods_input_values(rows: list[dict[str, Any]]) -> list[list[Any]]:
    values: list[list[Any]] = [ODS_INPUT_HEADERS]
    for row in rows:
        values.append(ods_input_row_from_record(row))
    return values


def normalized_ods_input_headers() -> list[str]:
    return [normalize_text(header, lowercase=True) for header in ODS_INPUT_HEADERS]
