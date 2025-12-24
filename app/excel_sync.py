import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from copy import copy

from app.paths import app_data_dir, resource_path
from app.storage import ensure_appdata_files, _desktop_excel_dir
from app.factura_calc import calcular_items

_DATA_ROOT = _desktop_excel_dir()
_EXCEL_PATH = _DATA_ROOT / "ODS 2026.xlsx"
_EXCEL_QUEUE = _DATA_ROOT / "ODS 2026 pendiente.jsonl"

_LOG_FILE = app_data_dir() / "logs" / "excel.log"
_logger = logging.getLogger("reca_ods_excel")
if not _logger.handlers:
    _LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.info("Excel logger iniciado. Archivo=%s", _LOG_FILE)


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
    "id": "id",
    "profesional": "nombre_profesional",
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
    "id",
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
        from openpyxl.utils import get_column_letter
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("openpyxl no esta instalado") from exc

    if not _EXCEL_PATH.exists():
        _logger.error("Excel no encontrado. Ruta=%s", _EXCEL_PATH)
        raise RuntimeError(f"No se encontro el archivo Excel: {_EXCEL_PATH}")

    _logger.info("Abriendo Excel. Ruta=%s", _EXCEL_PATH)
    wb = openpyxl.load_workbook(_EXCEL_PATH)
    ws = wb.active
    headers = [cell.value or "" for cell in ws[1]]
    normalized_headers = [_normalize_header(str(h)) for h in headers]
    if "id" not in normalized_headers:
        new_col = len(headers) + 1
        ws.cell(row=1, column=new_col, value="id")
        headers.append("id")
        normalized_headers.append("id")
        ws.column_dimensions[get_column_letter(new_col)].hidden = True
    return wb, ws, headers, normalized_headers


def _safe_save_workbook(wb) -> None:
    tmp_path = _EXCEL_PATH.with_suffix(".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    try:
        wb.save(tmp_path)
        wb.close()
        os.replace(tmp_path, _EXCEL_PATH)
        _logger.info("Excel guardado. Ruta=%s", _EXCEL_PATH)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        _logger.exception("Fallo al guardar Excel. Ruta=%s", _EXCEL_PATH)
        raise


def _nombre_hoja_factura(mes: int, año: int, tipo: str) -> str:
    meses = [
        "Ene",
        "Feb",
        "Mar",
        "Abr",
        "May",
        "Jun",
        "Jul",
        "Ago",
        "Sep",
        "Oct",
        "Nov",
        "Dic",
    ]
    nombre_mes = meses[mes - 1] if 1 <= mes <= 12 else "Mes"
    tipo_label = "Claus" if tipo.strip().lower() == "clausulada" else "NoClaus"
    nombre = f"Factura {nombre_mes} {año} {tipo_label}"
    return nombre[:31]


def _render_factura_sheet(wb, mes: int, año: int, tipo: str) -> None:
    try:
        import openpyxl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("openpyxl no esta instalado") from exc

    template_name = "clausulada.xlsx" if tipo.strip().lower() == "clausulada" else "no_clausulada.xlsx"
    template_path = resource_path(f"facturas/{template_name}")
    _logger.info(
        "Actualizando factura. Mes=%s Ano=%s Tipo=%s Plantilla=%s",
        mes,
        año,
        tipo,
        template_path,
    )
    template_wb = openpyxl.load_workbook(template_path)
    template_ws = template_wb.active

    items = calcular_items(mes, año, tipo)

    sheet_name = _nombre_hoja_factura(mes, año, tipo)
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)

    for row in template_ws.iter_rows():
        for cell in row:
            if cell.__class__.__name__ == "MergedCell":
                continue
            target = ws.cell(row=cell.row, column=cell.column, value=cell.value)
            if cell.has_style:
                target._style = copy(cell._style)
            target.number_format = cell.number_format
            target.alignment = copy(cell.alignment)
            target.border = copy(cell.border)
            target.fill = copy(cell.fill)
            target.font = copy(cell.font)

    for merged in template_ws.merged_cells.ranges:
        ws.merge_cells(str(merged))

    for key, dim in template_ws.row_dimensions.items():
        ws.row_dimensions[key].height = dim.height

    for key, dim in template_ws.column_dimensions.items():
        ws.column_dimensions[key].width = dim.width

    header_row = 10
    for idx, item in enumerate(items, start=1):
        row_idx = header_row + (idx - 1)
        ws.cell(row=row_idx, column=1, value=item.codigo_servicio)
        ws.cell(row=row_idx, column=2, value=item.referencia_servicio)
        ws.cell(row=row_idx, column=3, value=item.descripcion_servicio)
        ws.cell(row=row_idx, column=4, value=item.valor_base)
        ws.cell(row=row_idx, column=5, value=item.cantidad)
        ws.cell(row=row_idx, column=6, value=item.total)

    total_sum = sum(item.total for item in items)
    ws["F45"] = total_sum
    ws["F46"] = round(total_sum * 0.19, 2)
    ws["F47"] = round(total_sum + ws["F46"].value, 2)

    template_wb.close()


def update_factura_sheet(mes: int, año: int, tipo: str) -> None:
    ensure_appdata_files()
    try:
        import openpyxl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("openpyxl no esta instalado") from exc

    wb = openpyxl.load_workbook(_EXCEL_PATH)
    _render_factura_sheet(wb, mes, año, tipo)
    _safe_save_workbook(wb)


def update_facturas_from_service(ods_data: dict) -> None:
    mes = int(ods_data.get("mes_servicio", 0) or 0)
    año = int(ods_data.get("año_servicio", 0) or 0)
    if not mes or not año:
        _logger.info("Factura omitida. Mes/Ano vacio. Mes=%s Ano=%s", mes, año)
        return
    orden = str(ods_data.get("orden_clausulada", "")).strip().lower()
    tipo = "clausulada" if orden.startswith("s") or orden == "true" else "no clausulada"
    try:
        update_factura_sheet(mes, año, tipo)
    except PermissionError:
        _logger.warning("Factura en cola por archivo abierto. Mes=%s Ano=%s Tipo=%s", mes, año, tipo)
        queue_action(
            "factura_update",
            {},
            None,
            "archivo_abierto",
            meta={"mes": mes, "año": año, "tipo": tipo},
        )
    except Exception:
        _logger.exception("Factura en cola por error. Mes=%s Ano=%s Tipo=%s", mes, año, tipo)
        queue_action(
            "factura_update",
            {},
            None,
            "error_guardado",
            meta={"mes": mes, "año": año, "tipo": tipo},
        )


def _build_row_values(ods_data: dict, headers: list[str], normalized_headers: list[str]) -> list[Any]:
    row_values = [None] * len(headers)
    for idx, header_key in enumerate(normalized_headers):
        if header_key == "#":
            row_values[idx] = None
            continue
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

    _safe_save_workbook(wb)


def append_row_and_update_factura(ods_data: dict) -> None:
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

    mes = int(ods_data.get("mes_servicio", 0) or 0)
    año = int(ods_data.get("año_servicio", 0) or 0)
    if mes and año:
        orden = str(ods_data.get("orden_clausulada", "")).strip().lower()
        tipo = "clausulada" if orden.startswith("s") or orden == "true" else "no clausulada"
        _render_factura_sheet(wb, mes, año, tipo)

    _safe_save_workbook(wb)


def update_row(original: dict, ods_data: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para actualizar")

    row_values = _build_row_values(ods_data, headers, normalized_headers)
    for col_idx, value in enumerate(row_values, start=1):
        ws.cell(row=target_row, column=col_idx, value=value)

    _safe_save_workbook(wb)


def delete_row(original: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para eliminar")

    for col_idx in range(1, len(headers) + 1):
        ws.cell(row=target_row, column=col_idx, value=None)

    _safe_save_workbook(wb)


def update_row_and_update_factura(original: dict, ods_data: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para actualizar")

    row_values = _build_row_values(ods_data, headers, normalized_headers)
    for col_idx, value in enumerate(row_values, start=1):
        ws.cell(row=target_row, column=col_idx, value=value)

    mes = int(ods_data.get("mes_servicio", 0) or 0)
    año = int(ods_data.get("año_servicio", 0) or 0)
    if mes and año:
        orden = str(ods_data.get("orden_clausulada", "")).strip().lower()
        tipo = "clausulada" if orden.startswith("s") or orden == "true" else "no clausulada"
        _render_factura_sheet(wb, mes, año, tipo)

    _safe_save_workbook(wb)


def delete_row_and_update_factura(original: dict) -> None:
    wb, ws, headers, normalized_headers = _load_excel()
    target_row = _find_target_row(ws, original, headers, normalized_headers)
    if target_row is None:
        raise RuntimeError("No se encontro la fila en Excel para eliminar")

    for col_idx in range(1, len(headers) + 1):
        ws.cell(row=target_row, column=col_idx, value=None)

    mes = int(original.get("mes_servicio", 0) or 0)
    año = int(original.get("año_servicio", 0) or 0)
    if mes and año:
        orden = str(original.get("orden_clausulada", "")).strip().lower()
        tipo = "clausulada" if orden.startswith("s") or orden == "true" else "no clausulada"
        _render_factura_sheet(wb, mes, año, tipo)

    _safe_save_workbook(wb)


def rebuild_excel_from_supabase(rows: list[dict]) -> dict:
    ensure_appdata_files()
    try:
        import openpyxl
        from openpyxl.utils import get_column_letter
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("openpyxl no esta instalado") from exc

    backup_path = None
    if _EXCEL_PATH.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = _EXCEL_PATH.with_name(f"ODS 2026 backup {timestamp}.xlsx")
        shutil.copy2(_EXCEL_PATH, backup_path)

    template_path = resource_path("Excel/ods_2026.xlsx")
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    headers = [cell.value or "" for cell in ws[1]]
    normalized_headers = [_normalize_header(str(h)) for h in headers]
    if "id" not in normalized_headers:
        new_col = len(headers) + 1
        ws.cell(row=1, column=new_col, value="id")
        headers.append("id")
        normalized_headers.append("id")
        ws.column_dimensions[get_column_letter(new_col)].hidden = True

    for row_idx in range(2, ws.max_row + 1):
        for col_idx in range(1, len(headers) + 1):
            ws.cell(row=row_idx, column=col_idx, value=None)

    target_row = 2
    for row in rows:
        row_values = _build_row_values(row, headers, normalized_headers)
        for col_idx, value in enumerate(row_values, start=1):
            ws.cell(row=target_row, column=col_idx, value=value)
        target_row += 1

    _safe_save_workbook(wb)
    return {"rows": len(rows), "backup": str(backup_path) if backup_path else ""}


def queue_action(action: str, ods: dict, original: dict | None, reason: str, meta: dict | None = None) -> None:
    _EXCEL_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "reason": reason,
        "ods": ods,
        "original": original,
        "meta": meta or {},
    }
    with _EXCEL_QUEUE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    _logger.info("Encolado Excel. Accion=%s Motivo=%s Ruta=%s", action, reason, _EXCEL_QUEUE)


def clear_queue() -> None:
    if _EXCEL_QUEUE.exists():
        _EXCEL_QUEUE.write_text("", encoding="utf-8")


def _is_excel_locked() -> bool:
    if not _EXCEL_PATH.exists():
        return False
    if os.name != "nt":
        return False
    try:
        import msvcrt
    except Exception:
        return False
    try:
        handle = open(_EXCEL_PATH, "a")
    except Exception:
        return True
    try:
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        handle.close()
        return False
    except Exception:
        try:
            handle.close()
        except Exception:
            pass
        return True


def get_queue_status() -> dict:
    pendientes = 0
    if _EXCEL_QUEUE.exists():
        lines = _EXCEL_QUEUE.read_text(encoding="utf-8").splitlines()
        pendientes = len([line for line in lines if line.strip()])
    return {"pendientes": pendientes, "locked": _is_excel_locked()}


def flush_queue() -> dict:
    if not _EXCEL_QUEUE.exists():
        _logger.info("Flush Excel sin cola. Ruta=%s", _EXCEL_QUEUE)
        return {"procesados": 0, "pendientes": 0}

    lines = _EXCEL_QUEUE.read_text(encoding="utf-8").splitlines()
    if not lines:
        _logger.info("Flush Excel sin lineas. Ruta=%s", _EXCEL_QUEUE)
        return {"procesados": 0, "pendientes": 0}

    pendientes = []
    procesados = 0
    _logger.info("Flush Excel iniciado. Total=%s", len(lines))

    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            _logger.warning("Linea invalida en cola. Ruta=%s", _EXCEL_QUEUE)
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
            elif action == "factura_update":
                meta = record.get("meta", {})
                update_factura_sheet(
                    int(meta.get("mes", 0) or 0),
                    int(meta.get("año", 0) or 0),
                    str(meta.get("tipo", "")),
                )
            else:
                raise RuntimeError(f"accion desconocida: {action}")
            procesados += 1
            _logger.info("Flush Excel procesado. Accion=%s", action)
        except PermissionError:
            _logger.warning("Flush Excel detenido por archivo abierto.")
            pendientes.append(line)
            pendientes.extend(lines[lines.index(line) + 1 :])
            break
        except Exception:
            _logger.exception("Flush Excel fallo. Accion=%s", action)
            pendientes.append(line)

    _EXCEL_QUEUE.write_text("\n".join(pendientes) + ("\n" if pendientes else ""), encoding="utf-8")
    _logger.info("Flush Excel terminado. Procesados=%s Pendientes=%s", procesados, len(pendientes))
    return {"procesados": procesados, "pendientes": len(pendientes)}
