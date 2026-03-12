from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.config import get_settings
from app.google_sheet_layouts import (
    ODS_INPUT_HEADERS,
    normalized_ods_input_headers,
    ods_input_row_from_record,
)
from app.google_sheets_client import (
    clear_sheet_values,
    copy_drive_file,
    get_spreadsheet,
    list_drive_files,
    read_sheet_values,
    write_sheet_values,
)
from app.logging_utils import LOGGER_GOOGLE_DRIVE, get_file_logger
from app.paths import app_data_dir
from app.utils.text import normalize_text

try:
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover - dependencia opcional en runtime
    HttpError = RuntimeError  # type: ignore[assignment]

_LOG_FILE = app_data_dir() / "logs" / "google_drive.log"
_QUEUE_FILE = app_data_dir() / "queues" / "google_drive_pending.jsonl"
_LOGGER = get_file_logger(LOGGER_GOOGLE_DRIVE, _LOG_FILE, announce=True)
_SPREADSHEET_MIME = "application/vnd.google-apps.spreadsheet"
_MONTH_ABBREVIATIONS = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}
_EXPECTED_HEADERS_NORMALIZED = normalized_ods_input_headers()
_INPUT_SHEET_ALIASES = {"input", "ods_input"}


class GoogleDriveSyncError(RuntimeError):
    pass


class GoogleDriveSyncRetryableError(GoogleDriveSyncError):
    pass


class GoogleDriveSyncWarningError(GoogleDriveSyncError):
    pass


def _new_op_id() -> str:
    return uuid4().hex[:8]


def _log_event(level: str, context: str, op_id: str, message: str, *args, **kwargs) -> None:
    log_fn = getattr(_LOGGER, level, _LOGGER.info)
    log_fn(f"[ctx={context} op={op_id}] {message}", *args, **kwargs)


def _column_letter(index: int) -> str:
    letters: list[str] = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def resolve_monthly_spreadsheet_name(month: int, year: int) -> str:
    month_int = int(month)
    year_int = int(year)
    if month_int not in _MONTH_ABBREVIATIONS:
        raise GoogleDriveSyncWarningError(f"Mes invalido para sincronizacion: {month}")
    if year_int < 2000:
        raise GoogleDriveSyncWarningError(f"Ano invalido para sincronizacion: {year}")
    return f"ODS_{_MONTH_ABBREVIATIONS[month_int]}_{year_int}"


def get_existing_monthly_spreadsheet(month: int, year: int) -> dict[str, Any]:
    folder_id, _template_name = _settings_required()
    target_name = resolve_monthly_spreadsheet_name(month, year)
    monthly = _find_monthly_spreadsheet(folder_id, target_name)
    if monthly is None:
        raise GoogleDriveSyncWarningError(
            f"No existe el spreadsheet mensual '{target_name}' en el Shared Drive."
        )
    return monthly


def _settings_required() -> tuple[str, str]:
    settings = get_settings()
    if not settings.google_service_account_file:
        raise GoogleDriveSyncWarningError("Falta GOOGLE_SERVICE_ACCOUNT_FILE en la configuracion.")
    if not settings.google_drive_shared_folder_id:
        raise GoogleDriveSyncWarningError("Falta GOOGLE_DRIVE_SHARED_FOLDER_ID en la configuracion.")
    if not settings.google_drive_template_spreadsheet_name:
        raise GoogleDriveSyncWarningError(
            "Falta GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME en la configuracion."
        )
    return settings.google_drive_shared_folder_id, settings.google_drive_template_spreadsheet_name


def _normalize_headers(headers: list[Any]) -> list[str]:
    return [normalize_text(value, lowercase=True) for value in headers]


def _get_year_from_record(ods_data: dict[str, Any]) -> int:
    return int(
        ods_data.get("ano_servicio")
        or ods_data.get("a\u00f1o_servicio")
        or 0
    )


def _safe_target_name(ods_data: dict[str, Any]) -> str | None:
    month = int(ods_data.get("mes_servicio") or 0)
    year = _get_year_from_record(ods_data)
    try:
        return resolve_monthly_spreadsheet_name(month, year)
    except GoogleDriveSyncWarningError:
        return None


def _classify_exception(exc: Exception) -> GoogleDriveSyncError:
    if isinstance(exc, GoogleDriveSyncError):
        return exc
    if isinstance(exc, HttpError):
        status = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
        if status in {408, 429, 500, 502, 503, 504} or status >= 500:
            return GoogleDriveSyncRetryableError(f"Google API temporalmente no disponible (HTTP {status}).")
        return GoogleDriveSyncWarningError(f"Google API rechazo la operacion (HTTP {status}).")
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return GoogleDriveSyncRetryableError(str(exc))
    return GoogleDriveSyncWarningError(str(exc))


def _list_single_spreadsheet(folder_id: str, name: str, *, not_found_message: str) -> dict[str, Any]:
    matches = list_drive_files(folder_id=folder_id, name=name, mime_type=_SPREADSHEET_MIME)
    if not matches:
        raise GoogleDriveSyncWarningError(not_found_message)
    if len(matches) > 1:
        raise GoogleDriveSyncWarningError(
            f"Se encontraron multiples archivos '{name}' en el Shared Drive."
        )
    return matches[0]


def _find_monthly_spreadsheet(folder_id: str, target_name: str) -> dict[str, Any] | None:
    matches = list_drive_files(folder_id=folder_id, name=target_name, mime_type=_SPREADSHEET_MIME)
    if not matches:
        return None
    if len(matches) > 1:
        raise GoogleDriveSyncWarningError(
            f"Se encontraron multiples archivos '{target_name}' en el Shared Drive."
        )
    return matches[0]


def _find_input_sheet(spreadsheet_id: str) -> tuple[str, int]:
    spreadsheet = get_spreadsheet(spreadsheet_id, include_grid_data=False)
    input_title = None
    for sheet in spreadsheet.get("sheets", []):
        title = str(sheet.get("properties", {}).get("title") or "")
        if normalize_text(title, lowercase=True) in _INPUT_SHEET_ALIASES:
            input_title = title
            break
    if not input_title:
        raise GoogleDriveSyncWarningError(
            f"El spreadsheet {spreadsheet_id} no contiene una hoja 'input' u 'ODS_INPUT'."
        )

    header_rows = read_sheet_values(spreadsheet_id, f"'{input_title}'!1:1")
    if not header_rows or not header_rows[0]:
        raise GoogleDriveSyncWarningError(
            f"La hoja '{input_title}' no tiene encabezados en la fila 1."
        )
    headers = list(header_rows[0])
    normalized = _normalize_headers(headers)
    if normalized != _EXPECTED_HEADERS_NORMALIZED:
        raise GoogleDriveSyncWarningError(
            f"Encabezados inesperados en la hoja '{input_title}'."
        )
    return input_title, len(headers)


def _clear_input_data(spreadsheet_id: str, sheet_title: str, header_width: int) -> None:
    end_column = _column_letter(header_width)
    clear_sheet_values(spreadsheet_id, f"'{sheet_title}'!A2:{end_column}")


def _ensure_monthly_spreadsheet(
    *,
    folder_id: str,
    template_name: str,
    target_name: str,
    spreadsheet_id: str | None = None,
) -> tuple[str, str, int]:
    if spreadsheet_id:
        input_title, header_width = _find_input_sheet(spreadsheet_id)
        return spreadsheet_id, input_title, header_width

    monthly = _find_monthly_spreadsheet(folder_id, target_name)
    created = False
    if monthly is None:
        template = _list_single_spreadsheet(
            folder_id,
            template_name,
            not_found_message=f"No existe la plantilla '{template_name}' en el Shared Drive.",
        )
        monthly = copy_drive_file(
            template["id"],
            new_name=target_name,
            parent_folder_id=folder_id,
        )
        created = True

    monthly_id = str(monthly.get("id") or "").strip()
    if not monthly_id:
        raise GoogleDriveSyncWarningError(
            f"No se pudo resolver el spreadsheet mensual '{target_name}'."
        )
    input_title, header_width = _find_input_sheet(monthly_id)
    if created:
        _clear_input_data(monthly_id, input_title, header_width)
    return monthly_id, input_title, header_width


def _next_available_row(spreadsheet_id: str, sheet_title: str, header_width: int) -> tuple[int, set[str]]:
    end_column = _column_letter(header_width)
    rows = read_sheet_values(spreadsheet_id, f"'{sheet_title}'!A2:{end_column}")
    existing_ids: set[str] = set()
    last_nonempty_index = -1
    for idx, row in enumerate(rows):
        normalized_row = [str(value).strip() for value in row]
        if not any(normalized_row):
            continue
        last_nonempty_index = idx
        current_id = normalized_row[0]
        if current_id:
            existing_ids.add(current_id)
    return 2 + last_nonempty_index + 1, existing_ids


def _sync_new_ods_record_once(ods_data: dict[str, Any], *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    folder_id, template_name = _settings_required()
    meta = meta or {}
    spreadsheet_id = str(meta.get("spreadsheet_id") or "").strip() or None
    month = int(ods_data.get("mes_servicio") or 0)
    year = _get_year_from_record(ods_data)
    target_name = str(meta.get("target_name") or "").strip() or resolve_monthly_spreadsheet_name(month, year)

    spreadsheet_id, sheet_title, header_width = _ensure_monthly_spreadsheet(
        folder_id=folder_id,
        template_name=template_name,
        target_name=target_name,
        spreadsheet_id=spreadsheet_id,
    )
    row_id = str(ods_data.get("id") or "").strip()
    if not row_id:
        raise GoogleDriveSyncWarningError("La ODS no tiene ID para sincronizar con Google Sheets.")

    target_row, existing_ids = _next_available_row(spreadsheet_id, sheet_title, header_width)
    if row_id in existing_ids:
        return {
            "sync_status": "ok",
            "sync_error": None,
            "sync_target": target_name,
            "spreadsheet_id": spreadsheet_id,
        }

    row_values = ods_input_row_from_record(ods_data)
    end_column = _column_letter(len(ODS_INPUT_HEADERS))
    write_sheet_values(
        spreadsheet_id,
        f"'{sheet_title}'!A{target_row}:{end_column}{target_row}",
        [row_values],
    )
    return {
        "sync_status": "ok",
        "sync_error": None,
        "sync_target": target_name,
        "spreadsheet_id": spreadsheet_id,
    }


def queue_google_drive_sync(
    ods_data: dict[str, Any],
    *,
    reason: str,
    meta: dict[str, Any] | None = None,
) -> None:
    op_id = _new_op_id()
    context = "google_drive.queue"
    _QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": "append",
        "reason": reason,
        "ods": ods_data,
        "meta": meta or {},
    }
    with _QUEUE_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    _log_event("info", context, op_id, "Encolada sincronizacion Drive. Motivo=%s Ruta=%s", reason, _QUEUE_FILE)


def get_google_drive_queue_status() -> dict[str, Any]:
    pending = 0
    if _QUEUE_FILE.exists():
        lines = _QUEUE_FILE.read_text(encoding="utf-8").splitlines()
        pending = len([line for line in lines if line.strip()])
    return {"pendientes": pending}


def sync_new_ods_record(ods_data: dict[str, Any]) -> dict[str, Any]:
    op_id = _new_op_id()
    context = "google_drive.sync"
    try:
        result = _sync_new_ods_record_once(ods_data)
        _log_event(
            "info",
            context,
            op_id,
            "Sincronizacion Drive completada. Target=%s",
            result["sync_target"],
        )
        return result
    except Exception as exc:
        classified = _classify_exception(exc if isinstance(exc, Exception) else RuntimeError(str(exc)))
        if isinstance(classified, GoogleDriveSyncRetryableError):
            target_name = _safe_target_name(ods_data)
            queue_google_drive_sync(
                ods_data,
                reason="retryable_error",
                meta={"target_name": target_name or ""},
            )
            _log_event("warning", context, op_id, "Sincronizacion Drive pendiente: %s", classified)
            return {
                "sync_status": "pending",
                "sync_error": str(classified),
                "sync_target": target_name,
            }
        _log_event("warning", context, op_id, "Sincronizacion Drive con advertencia: %s", classified)
        return {
            "sync_status": "warning",
            "sync_error": str(classified),
            "sync_target": _safe_target_name(ods_data),
        }


def flush_google_drive_queue() -> dict[str, Any]:
    op_id = _new_op_id()
    context = "google_drive.flush"
    if not _QUEUE_FILE.exists():
        return {"procesados": 0, "pendientes": 0, "error": None}

    lines = _QUEUE_FILE.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {"procesados": 0, "pendientes": 0, "error": None}

    pending_lines: list[str] = []
    processed = 0
    last_error: str | None = None
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            _log_event("warning", context, op_id, "Linea de cola con JSON invalido descartada: %s", line[:80])
            continue

        ods_data = record.get("ods") or {}
        meta = record.get("meta") or {}
        try:
            _sync_new_ods_record_once(ods_data, meta=meta)
            processed += 1
        except Exception as exc:
            classified = _classify_exception(exc if isinstance(exc, Exception) else RuntimeError(str(exc)))
            last_error = str(classified)
            pending_lines.append(line)
            pending_lines.extend(lines[index + 1 :])
            _log_event("warning", context, op_id, "Flush detenido: %s", classified)
            break

    _QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _QUEUE_FILE.write_text(
        "\n".join(pending_lines) + ("\n" if pending_lines else ""),
        encoding="utf-8",
    )
    _log_event(
        "info",
        context,
        op_id,
        "Flush Drive terminado. Procesados=%s Pendientes=%s",
        processed,
        len([line for line in pending_lines if line.strip()]),
    )
    return {
        "procesados": processed,
        "pendientes": len([line for line in pending_lines if line.strip()]),
        "error": last_error,
    }
