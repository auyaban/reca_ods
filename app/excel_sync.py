import json
from datetime import datetime
from pathlib import Path
from typing import Any

from app.paths import app_data_dir
from app.storage import ensure_appdata_files

_DATA_ROOT = app_data_dir()
_EXCEL_PATH = _DATA_ROOT / "Excel" / "ods_2026.xlsx"
_EXCEL_QUEUE = _DATA_ROOT / "Excel" / "ods_2026_pendiente.jsonl"


def _normalize_header(value: str) -> str:
    clean = " ".join(value.strip().lower().split())
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for src, dst in replacements.items():
        clean = clean.replace(src, dst)
    return clean


_EXCEL_FIELD_MAP = {
    "profesional": "nombre_profesional",
    "#": "id_servicio",
    "nuevo codigo": "codigo_servicio",
    "empresa": "nombre_empresa",
    "nit": "nit_empresa",
    "ccf": "caja_compensacion",
    "fecha": "fecha_servicio",
    "referencia": "referencia_servicio",
    "nombre": "descripcion_servicio",
    "oferentes": "nombre_usuario",
    "cedula": "cedula_usuario",
    "tipo de discapacidad": "discapacidad_usuario",
    "fecha ingreso": "fecha_ingreso",
    "valor servicio virtual": "valor_virtual",
    "valor servicio bogota": "valor_bogota",
    "valor fuera de bogota": "valor_otro",
    "todas las modalidades": "todas_modalidades",
    "total horas": "horas_interprete",
    "valor a pagar": "valor_interprete",
    "total valor servicio sin iva": "valor_total",
    "observaciones": "observaciones",
    "asesor": "asesor_empresa",
    "sede": "sede_empresa",
    "modalidad": "modalidad_servicio",
    "observacion agencia": "observacion_agencia",
    "clausulada": "orden_clausulada",
    "año": "año_servicio",
    "ano": "año_servicio",
    "mes": "mes_servicio",
    "genero": "genero_usuario",
    "tipo de contrato": "tipo_contrato",
    "seguimiento": "seguimiento_servicio",
    "cargo": "cargo_servicio",
    "personas": "total_personas",
}

_MATCH_KEYS = [
    "id_servicio",
    "fecha_servicio",
    "codigo_servicio",
    "nit_empresa",
    "nombre_profesional",
]


def _coerce_to_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _row_to_ods(ws, row_idx: int, headers: list[str], normalized_headers: list[str]) -> dict:
    row_data = {}
    for col_idx, header_key in enumerate(normalized_headers, start=1):
        field = _EXCEL_FIELD_MAP.get(header_key)
        if not field:
            continue
        value = ws.cell(row=row_idx, column=col_idx).value
        row_data[field] = value
    return row_data


def _match_row(original: dict, row_data: dict) -> bool:
    for key in _MATCH_KEYS:
        if _coerce_to_string(original.get(key)) != _coerce_to_string(row_data.get(key)):
            return False
    return True


def _find_target_row(ws, original: dict, headers: list[str], normalized_headers: list[str]) -> int | None:
    for row_idx in range(2, ws.max_row + 1):
        if all(cell.value in (None, "") for cell in ws[row_idx]):
            continue
        row_data = _row_to_ods(ws, row_idx, headers, normalized_headers)
        if _match_row(original, row_data):
            return row_idx
    return None


def _load_excel():
    ensure_appdata_files()
    try:
        import openpyxl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("openpyxl no esta instalado") from exc

    if not _EXCEL_PATH.exists():
        raise RuntimeError(f"No se encontro el archivo Excel: {_EXCEL_PATH}")

    wb = openpyxl.load_workbook(_EXCEL_PATH)
    ws = wb.active
    headers = [cell.value or "" for cell in ws[1]]
    normalized_headers = [_normalize_header(str(h)) for h in headers]
    return wb, ws, headers, normalized_headers


def _build_row_values(ods_data: dict, headers: list[str], normalized_headers: list[str]) -> list[Any]:
    row_values = [None] * len(headers)
    for idx, header_key in enumerate(normalized_headers):
        field = _EXCEL_FIELD_MAP.get(header_key)
        if not field:
            continue
        value = ods_data.get(field)
        if field == "orden_clausulada":
            value = "Sí" if str(value).strip().lower().startswith("s") else "No"
        row_values[idx] = value
    return row_values


def append_row(ods_data: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    row_values = _build_row_values(ods_data, headers, normalized_headers)

    target_row = None
    for row_idx in range(2, ws.max_row + 2):
        cells = ws[row_idx]
        if all(cell.value in (None, "") for cell in cells):
            target_row = row_idx
            break
    if target_row is None:
        target_row = ws.max_row + 1

    for col_idx, value in enumerate(row_values, start=1):
        ws.cell(row=target_row, column=col_idx, value=value)

    wb.save(_EXCEL_PATH)


def update_row(original: dict, ods_data: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para actualizar")

    row_values = _build_row_values(ods_data, headers, normalized_headers)
    for col_idx, value in enumerate(row_values, start=1):
        ws.cell(row=target_row, column=col_idx, value=value)

    wb.save(_EXCEL_PATH)


def delete_row(original: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para eliminar")

    for col_idx in range(1, len(headers) + 1):
        ws.cell(row=target_row, column=col_idx, value=None)

    wb.save(_EXCEL_PATH)


def queue_action(action: str, ods: dict, original: dict | None, reason: str) -> None:
    _EXCEL_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "reason": reason,
        "ods": ods,
        "original": original,
    }
    with _EXCEL_QUEUE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def flush_queue() -> dict:
    if not _EXCEL_QUEUE.exists():
        return {"procesados": 0, "pendientes": 0}

    lines = _EXCEL_QUEUE.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {"procesados": 0, "pendientes": 0}

    pendientes = []
    procesados = 0

    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            pendientes.append(line)
            continue

        action = record.get("action", "append")
        ods = record.get("ods", {})
        original = record.get("original") or ods
        try:
            if action == "append":
                append_row(ods)
            elif action == "update":
                update_row(original, ods)
            elif action == "delete":
                delete_row(original)
            else:
                raise RuntimeError(f"accion desconocida: {action}")
            procesados += 1
        except PermissionError:
            pendientes.append(line)
            pendientes.extend(lines[lines.index(line) + 1 :])
            break
        except Exception:
            pendientes.append(line)

    _EXCEL_QUEUE.write_text("\n".join(pendientes) + ("\n" if pendientes else ""), encoding="utf-8")
    return {"procesados": procesados, "pendientes": len(pendientes)}
