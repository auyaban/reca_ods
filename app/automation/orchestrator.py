from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from functools import lru_cache
import hashlib
import os
from pathlib import Path
import re
import tempfile

from app.automation.gmail_inbox import GmailInboxGateway
from app.automation.models import (
    AttachmentRef,
    AutomationComponentStatus,
    AutomationNextStep,
    AutomationSkeletonStatus,
    GmailCandidateEmail,
    GmailMessageRef,
)
from app.google_sheet_layouts import ODS_INPUT_HEADERS, ods_input_row_from_record
from app.google_sheets_client import read_sheet_values, write_sheet_values
from app.automation.process_catalog import list_process_template_names
from app.automation.rules_engine import suggest_service_from_analysis
from app.services.acta_import_pipeline import build_import_result_from_parsed
from app.services.excel_acta_import import parse_acta_pdf
from app.automation.staging import AutomationStagingRepository
from app.config import get_settings
from app.services.sections.seccion4 import DISCAPACIDADES, GENEROS
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket
from app.utils.text import normalize_text

_CACHE_TTL_SECONDS = 300
_NO_PARTICIPANT_ROW_KINDS = {
    "program_presentation",
    "program_reactivation",
    "accessibility_assessment",
    "vacancy_review",
    "sensibilizacion",
    "organizational_induction",
    "operational_induction",
    "follow_up",
}
_AUTOMATION_DEFAULT_ORDER = "no"
_AUTOMATION_HEADERS = ODS_INPUT_HEADERS + ["REVISAR"]


@lru_cache
def _professional_email_map_cached(_ttl_bucket: int) -> dict[str, str]:
    response = execute_with_reauth(
        lambda retry_client: retry_client.table("profesionales").select("nombre_profesional,correo_profesional").execute(),
        context="automation.professionals.list",
    )
    allowed: dict[str, str] = {}
    for row in list(response.data or []):
        email = str(row.get("correo_profesional") or "").strip().lower()
        name = str(row.get("nombre_profesional") or "").strip()
        if email and name:
            allowed[email] = name
    return allowed


def _professional_email_map() -> dict[str, str]:
    return _professional_email_map_cached(ttl_bucket(_CACHE_TTL_SECONDS))


def _professional_name_score(candidate: str, professional_name: str) -> float:
    candidate_norm = normalize_text(candidate or "")
    professional_norm = normalize_text(professional_name or "")
    if not candidate_norm or not professional_norm:
        return 0.0
    if candidate_norm == professional_norm:
        return 1.0

    candidate_tokens = [token for token in candidate_norm.split() if len(token) >= 2]
    professional_tokens = [token for token in professional_norm.split() if len(token) >= 2]
    if len(candidate_tokens) >= 2 and all(token in professional_tokens for token in candidate_tokens):
        return 0.985
    if len(professional_tokens) >= 2 and all(token in candidate_tokens for token in professional_tokens):
        return 0.95
    return SequenceMatcher(None, candidate_norm, professional_norm).ratio()


def _canonicalize_professional_candidates(candidates: list[str]) -> list[str]:
    professional_names = list(dict.fromkeys(_professional_email_map().values()))
    resolved: list[str] = []
    for raw_candidate in candidates:
        candidate = str(raw_candidate or "").strip()
        if not candidate:
            continue
        best_name = ""
        best_score = 0.0
        for professional_name in professional_names:
            score = _professional_name_score(candidate, professional_name)
            if score > best_score:
                best_score = score
                best_name = professional_name
        final_name = best_name if best_name and best_score >= 0.72 else candidate
        if final_name not in resolved:
            resolved.append(final_name)
    return resolved


def _resolve_interpreter_names(analysis: dict) -> list[str]:
    candidates: list[str] = []
    for raw_candidate in list(analysis.get("interpretes") or []):
        candidate = str(raw_candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    if not candidates:
        for raw_candidate in list(analysis.get("asistentes") or []):
            candidate = str(raw_candidate or "").strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    if not candidates:
        candidate = str(analysis.get("nombre_profesional") or "").strip()
        if candidate:
            candidates.append(candidate)
    return _canonicalize_professional_candidates(candidates)


def _resolve_professional_name(analysis: dict, *, sender_match: str = "") -> str:
    candidates: list[str] = []
    for raw_candidate in list(analysis.get("asistentes") or []):
        candidate = str(raw_candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    for raw_candidate in list(analysis.get("candidatos_profesional") or []):
        candidate = str(raw_candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    for raw_candidate in [analysis.get("nombre_profesional"), sender_match]:
        candidate = str(raw_candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    resolved = _canonicalize_professional_candidates(candidates)
    if resolved:
        return resolved[0]
    return str(sender_match or analysis.get("nombre_profesional") or "").strip()


@lru_cache
def _usuarios_reca_cached(_ttl_bucket: int) -> tuple[dict[str, str], ...]:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("usuarios_reca")
            .select(
                "nombre_usuario,cedula_usuario,discapacidad_usuario,genero_usuario,"
                "tipo_contrato,fecha_firma_contrato,cargo_oferente"
            )
            .order("cedula_usuario")
            .execute()
        ),
        context="automation.usuarios_reca.list",
    )
    return tuple(dict(row) for row in list(response.data or []))


def _usuarios_reca_by_cedula() -> dict[str, dict[str, str]]:
    rows = _usuarios_reca_cached(ttl_bucket(_CACHE_TTL_SECONDS))
    return {
        str(item.get("cedula_usuario") or "").strip(): dict(item)
        for item in rows
        if str(item.get("cedula_usuario") or "").strip()
    }


def _normalize_import_name(value: str) -> str:
    clean = " ".join(str(value or "").split())
    if not clean:
        return ""
    if clean.isupper() or clean.islower():
        return clean.title()
    return clean


def _normalize_import_discapacidad(value: str) -> str:
    normalized = normalize_text(value or "")
    for key, label in DISCAPACIDADES.items():
        if key != "n/a" and key in normalized:
            return label
    return DISCAPACIDADES["n/a"]


def _normalize_import_genero(value: str) -> str:
    normalized = normalize_text(value or "")
    if "masculino" in normalized:
        return GENEROS["hombre"]
    if "femenino" in normalized:
        return GENEROS["mujer"]
    for key, label in GENEROS.items():
        if key in normalized:
            return label
    return GENEROS["otro"]


def _build_minimum_user(persona: dict) -> dict | None:
    cedula = str(persona.get("cedula_usuario") or "").strip()
    nombre = _normalize_import_name(str(persona.get("nombre_usuario") or "").strip())
    if not cedula or not nombre:
        return None
    return {
        "nombre_usuario": nombre,
        "cedula_usuario": cedula,
        "discapacidad_usuario": _normalize_import_discapacidad(str(persona.get("discapacidad_usuario") or "").strip()),
        "genero_usuario": _normalize_import_genero(str(persona.get("genero_usuario") or "").strip()),
        "fecha_ingreso": "",
        "tipo_contrato": "",
        "cargo_servicio": "",
    }


def _prepare_automation_participants(parsed: dict) -> tuple[list[dict], list[str], list[str]]:
    participantes_raw = list(parsed.get("participantes") or [])
    usuarios_by_cedula = _usuarios_reca_by_cedula()

    participantes: list[dict] = []
    descartados: list[str] = []
    warnings: list[str] = []
    nuevos_preparados = 0

    for persona in participantes_raw:
        ced = str(persona.get("cedula_usuario") or "").strip()
        if not ced:
            continue
        existing = usuarios_by_cedula.get(ced)
        if existing:
            participantes.append(
                {
                    "nombre_usuario": _normalize_import_name(str(existing.get("nombre_usuario") or persona.get("nombre_usuario") or "").strip()),
                    "cedula_usuario": ced,
                    "discapacidad_usuario": str(existing.get("discapacidad_usuario") or persona.get("discapacidad_usuario") or "").strip(),
                    "genero_usuario": str(existing.get("genero_usuario") or persona.get("genero_usuario") or "").strip(),
                    "fecha_ingreso": str(existing.get("fecha_firma_contrato") or "").strip(),
                    "tipo_contrato": str(existing.get("tipo_contrato") or "").strip(),
                    "cargo_servicio": str(existing.get("cargo_oferente") or "").strip(),
                    "_usuario_accion": "existente",
                }
            )
            continue

        user_minimo = _build_minimum_user(persona)
        if user_minimo is None:
            descartados.append(ced)
            continue
        user_minimo["_usuario_accion"] = "crear"
        participantes.append(user_minimo)
        nuevos_preparados += 1

    if descartados:
        warnings.append(
            f"Se descartaron {len(descartados)} cedula(s) que no existen en BD y no traian datos suficientes para crearlas."
        )
    if nuevos_preparados:
        warnings.append(
            f"Se prepararon {nuevos_preparados} usuarios nuevos con datos minimos para persistir al guardar el servicio."
        )
    if not participantes:
        warnings.append(
            "No se detectaron cédulas válidas en el acta. Se importarán empresa, fecha, modalidad y profesional, pero los oferentes deben completarse manualmente."
        )
    return participantes, descartados, warnings


def _participant_summary(participants: list[dict], discarded_ids: list[str]) -> dict[str, int]:
    existentes = 0
    crear = 0
    for item in participants:
        accion = str(item.get("_usuario_accion") or "").strip().lower()
        if accion == "existente":
            existentes += 1
        elif accion == "crear":
            crear += 1
    return {
        "total_detectados": len(participants) + len(discarded_ids),
        "existentes": existentes,
        "crear": crear,
        "descartados": len(discarded_ids),
        "validos": len(participants),
    }


@lru_cache
def _companies_cached(_ttl_bucket: int) -> tuple[dict[str, str], ...]:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("empresas")
            .select(
                "nit_empresa,nombre_empresa,caja_compensacion,asesor,zona_empresa,sede_empresa,ciudad_empresa"
            )
            .execute()
        ),
        context="automation.empresas.list",
    )
    return tuple(dict(row) for row in list(response.data or []))


def _companies() -> tuple[dict[str, str], ...]:
    return _companies_cached(ttl_bucket(_CACHE_TTL_SECONDS))


def _company_by_nit_details(nit: str) -> dict[str, str] | None:
    nit_clean = str(nit or "").strip()
    if not nit_clean:
        return None
    for row in _companies():
        if str(row.get("nit_empresa") or "").strip() == nit_clean:
            return dict(row)
    return None


def _company_by_name_strong(name: str) -> dict[str, str] | None:
    name_normalized = normalize_text(name or "")
    if not name_normalized:
        return None
    best_match: dict[str, str] | None = None
    best_score = 0.0
    for row in _companies():
        candidate_name = normalize_text(row.get("nombre_empresa") or "")
        if not candidate_name:
            continue
        score = SequenceMatcher(None, name_normalized, candidate_name).ratio()
        if name_normalized == candidate_name:
            score = 1.0
        elif name_normalized in candidate_name or candidate_name in name_normalized:
            score = max(score, 0.96)
        if score > best_score:
            best_score = score
            best_match = dict(row)
    if best_match and best_score >= 0.93:
        return best_match
    return None


def _canonicalize_company_in_analysis(analysis: dict) -> dict[str, str] | None:
    warnings = list(analysis.get("warnings") or [])
    nit = str(analysis.get("nit_empresa") or "").strip()
    name = str(analysis.get("nombre_empresa") or "").strip()
    company_by_nit = _company_by_nit_details(nit)
    company_by_name = _company_by_name_strong(name) if name else None
    company: dict[str, str] | None = None

    if company_by_nit:
        company = dict(company_by_nit)
        if name and normalize_text(name) != normalize_text(company.get("nombre_empresa") or ""):
            warnings.append("Empresa ajustada al registro canonico de Supabase por NIT.")
    elif company_by_name:
        company = dict(company_by_name)
        if nit and str(company.get("nit_empresa") or "").strip() != nit:
            warnings.append("NIT ajustado al registro canonico de Supabase por nombre de empresa.")
    else:
        warnings.append("No se pudo conciliar empresa/NIT con Supabase.")

    if company:
        analysis["nit_empresa"] = str(company.get("nit_empresa") or "").strip()
        analysis["nombre_empresa"] = str(company.get("nombre_empresa") or "").strip()
        analysis["caja_compensacion"] = str(company.get("caja_compensacion") or "").strip()
        analysis["asesor_empresa"] = str(company.get("asesor") or "").strip()
        analysis["sede_empresa"] = str(company.get("sede_empresa") or company.get("zona_empresa") or "").strip()
        analysis["ciudad_empresa"] = str(company.get("ciudad_empresa") or "").strip()
        analysis["_company_match_required"] = False
    else:
        analysis["_company_match_required"] = True

    analysis["warnings"] = list(dict.fromkeys(warnings))
    return company


def _desktop_dir() -> Path:
    one_drive = os.getenv("OneDrive")
    if one_drive:
        for folder in ("Desktop", "Escritorio"):
            candidate = Path(one_drive) / folder
            if candidate.exists():
                return candidate
    for folder in ("Desktop", "Escritorio"):
        candidate = Path.home() / folder
        if candidate.exists():
            return candidate
    return Path.home()


def _decisions_log_path() -> Path:
    settings = get_settings()
    configured = str(settings.automation_decisions_log_path or "").strip()
    if configured:
        return Path(configured).expanduser()
    return _desktop_dir() / "decisiones.txt"


def _normalize_iso_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split("T", 1)[0]


def _message_received_iso(message: dict) -> str:
    raw = str(message.get("received_at") or "").strip()
    if not raw:
        return ""
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return raw


def _month_year_from_analysis(analysis: dict, message: dict) -> tuple[int, int]:
    fecha = _normalize_iso_date(str(analysis.get("fecha_servicio") or ""))
    if fecha:
        try:
            parsed = datetime.strptime(fecha, "%Y-%m-%d")
            return parsed.month, parsed.year
        except ValueError:
            pass
    received = _message_received_iso(message)
    if received:
        try:
            parsed = datetime.strptime(received, "%Y-%m-%d")
            return parsed.month, parsed.year
        except ValueError:
            pass
    now = datetime.now(timezone.utc)
    return now.month, now.year


def _stable_upload_id(message_id: str, filename: str, codigo: str, cedula: str, profesional: str = "") -> str:
    base = "|".join(
        [
            str(message_id or "").strip(),
            str(filename or "").strip().lower(),
            str(codigo or "").strip(),
            str(cedula or "").strip(),
            str(profesional or "").strip().lower(),
        ]
    )
    return "auto-" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _build_decision_log_entry(*, kind: str, message: dict, attachment: dict | None = None, row: dict | None = None, details: list[str] | None = None) -> str:
    timestamp = datetime.now(timezone.utc).isoformat()
    values = [
        f"[{timestamp}]",
        f"estado={kind}",
        f"message_id={str(message.get('message_id') or '-')}",
        f"subject={str(message.get('subject') or '-')}",
        f"remitente={str(message.get('sender_email') or '-')}",
    ]
    if attachment:
        values.append(f"documento={str(attachment.get('filename') or '-')}")
    if row:
        values.extend(
            [
                f"codigo={str(row.get('codigo_servicio') or '-')}",
                f"empresa={str(row.get('nombre_empresa') or '-')}",
                f"nit={str(row.get('nit_empresa') or '-')}",
                f"cedula={str(row.get('cedula_usuario') or '-')}",
                f"horas={str(row.get('horas_interprete') or '-')}",
            ]
        )
    if details:
        values.append("detalle=" + " | ".join(str(item) for item in details if str(item).strip()))
    return " ".join(values)


def _append_decision_log(entries: list[str]) -> Path:
    path = _decisions_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(entry.rstrip() + "\n")
    return path


def _existing_upload_ids(spreadsheet_id: str, sheet_name: str) -> set[str]:
    rows = read_sheet_values(spreadsheet_id, f"'{sheet_name}'!A2:A")
    result: set[str] = set()
    for row in rows:
        if not row:
            continue
        value = str(row[0] or "").strip()
        if value:
            result.add(value)
    return result


def _next_sheet_row(spreadsheet_id: str, sheet_name: str) -> int:
    end_col = chr(64 + len(_AUTOMATION_HEADERS))
    rows = read_sheet_values(spreadsheet_id, f"'{sheet_name}'!A:{end_col}")
    return len(rows) + 1


def _write_rows_to_sheet(*, spreadsheet_id: str, sheet_name: str, rows: list[dict]) -> int:
    """Write upload rows to Google Sheets. Returns the number of rows written."""
    if not rows:
        return 0
    if len(_AUTOMATION_HEADERS) > 26:
        raise RuntimeError(
            f"Las columnas de automatizacion tienen {len(_AUTOMATION_HEADERS)} columnas; "
            f"el maximo soportado es 26 (A-Z)."
        )
    start_row = _next_sheet_row(spreadsheet_id, sheet_name)
    values = [
        ods_input_row_from_record(row) + [str(row.get("revisar_flag") or "")]
        for row in rows
    ]
    end_row = start_row + len(values) - 1
    end_column = chr(64 + len(_AUTOMATION_HEADERS))
    write_sheet_values(
        spreadsheet_id,
        f"'{sheet_name}'!A{start_row}:{end_column}{end_row}",
        values,
    )
    return len(values)


def _gmail_gateway(*, limit: int | None = None) -> GmailInboxGateway:
    settings = get_settings()
    delegated_user = str(settings.google_gmail_delegated_user or "").strip()
    if not delegated_user:
        raise RuntimeError("Falta GOOGLE_GMAIL_DELEGATED_USER para probar Gmail en Aaron TEST.")
    max_results = int(limit or settings.google_gmail_fetch_limit or 20)
    return GmailInboxGateway(
        delegated_user=delegated_user,
        to_filter=settings.google_gmail_to_filter,
        max_results=max_results,
    )


def _download_and_parse_attachment(*, gateway: GmailInboxGateway, message_ref: GmailMessageRef, attachment: dict) -> dict:
    attachment_ref = AttachmentRef(
        attachment_id=str(attachment.get("attachment_id") or "").strip(),
        filename=str(attachment.get("filename") or "").strip(),
        mime_type=str(attachment.get("mime_type") or "application/pdf").strip(),
        size_bytes=int(attachment.get("size_bytes") or 0),
        process_hint=str(attachment.get("process_hint") or "").strip(),
        process_score=float(attachment.get("process_score") or 0),
        document_kind=str(attachment.get("document_kind") or "").strip(),
        document_label=str(attachment.get("document_label") or "").strip(),
        is_ods_candidate=bool(attachment.get("is_ods_candidate")),
        classification_score=float(attachment.get("classification_score") or 0),
        classification_reason=str(attachment.get("classification_reason") or "").strip(),
    )
    temp_path: Path | None = None
    try:
        suffix = Path(attachment_ref.filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            temp_path = Path(tmp.name)
            tmp.write(gateway.download_attachment_bytes(message_ref, attachment_ref))

        parsed = parse_acta_pdf(str(temp_path))
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

    parsed["file_path"] = attachment_ref.filename
    return parsed


def _match_sender_and_prepare_analysis(*, parsed: dict, attachment: dict, message: dict, allowed_senders: dict[str, str]) -> dict:
    _ = allowed_senders
    import_result = build_import_result_from_parsed(
        parsed,
        source_label=str(attachment.get("filename") or parsed.get("file_path") or "acta.pdf"),
        message=message,
        attachment=attachment,
        create_missing_interpreter=True,
    )
    analysis = dict(import_result.get("analysis") or {})
    discarded_ids = list(analysis.get("_cedulas_descartadas") or [])
    analysis["participant_summary"] = _participant_summary(list(analysis.get("participantes") or []), discarded_ids)
    return analysis


def _resolve_interpreter_context(documents: list[dict]) -> None:
    non_interpreters = [
        item for item in documents if str(dict(item.get("attachment") or {}).get("document_kind") or "") != "interpreter_service"
    ]
    for item in documents:
        if str(dict(item.get("attachment") or {}).get("document_kind") or "") != "interpreter_service":
            continue
        analysis = dict(item.get("analysis") or {})
        company_name = str(analysis.get("nombre_empresa") or "").strip()
        nit = str(analysis.get("nit_empresa") or "").strip()

        if (not nit or not company_name) and non_interpreters:
            for other in non_interpreters:
                other_analysis = dict(other.get("analysis") or {})
                other_name = str(other_analysis.get("nombre_empresa") or "").strip()
                other_nit = str(other_analysis.get("nit_empresa") or "").strip()
                if not other_nit and not other_name:
                    continue
                if company_name:
                    score = SequenceMatcher(None, normalize_text(company_name), normalize_text(other_name)).ratio()
                    if score < 0.85 and normalize_text(company_name) != normalize_text(other_name):
                        continue
                if not nit and other_nit:
                    analysis["nit_empresa"] = other_nit
                if not company_name and other_name:
                    analysis["nombre_empresa"] = other_name
                break

        if not analysis.get("nit_empresa"):
            company_match = _company_by_name_strong(str(analysis.get("nombre_empresa") or ""))
            if company_match:
                analysis["nit_empresa"] = str(company_match.get("nit_empresa") or "").strip()
                if not analysis.get("nombre_empresa"):
                    analysis["nombre_empresa"] = str(company_match.get("nombre_empresa") or "").strip()

        item["analysis"] = analysis


def _base_upload_row(*, message: dict, attachment: dict, analysis: dict, suggestion: dict, company: dict | None) -> dict:
    month, year = _month_year_from_analysis(analysis, message)
    profesional = (
        str(analysis.get("nombre_profesional") or "").strip()
        or str(analysis.get("matched_professional_sender") or "").strip()
    )
    modalidad = str(suggestion.get("modalidad_servicio") or analysis.get("modalidad_servicio") or "").strip()
    company_data = dict(company or {})
    return {
        "id": "",
        "nombre_profesional": profesional,
        "codigo_servicio": str(suggestion.get("codigo_servicio") or "").strip(),
        "nombre_empresa": str(company_data.get("nombre_empresa") or analysis.get("nombre_empresa") or "").strip(),
        "nit_empresa": str(company_data.get("nit_empresa") or analysis.get("nit_empresa") or "").strip(),
        "caja_compensacion": str(company_data.get("caja_compensacion") or analysis.get("caja_compensacion") or "").strip(),
        "fecha_servicio": _normalize_iso_date(str(analysis.get("fecha_servicio") or "")),
        "nombre_usuario": "",
        "cedula_usuario": "",
        "discapacidad_usuario": "",
        "fecha_ingreso": "",
        "observaciones": str(suggestion.get("observaciones") or "").strip(),
        "modalidad_servicio": modalidad,
        "orden_clausulada": _AUTOMATION_DEFAULT_ORDER,
        "genero_usuario": "",
        "tipo_contrato": "",
        "asesor_empresa": str(company_data.get("asesor") or analysis.get("asesor_empresa") or "").strip(),
        "sede_empresa": str(company_data.get("sede_empresa") or company_data.get("zona_empresa") or analysis.get("sede_empresa") or "").strip(),
        "observacion_agencia": str(suggestion.get("observacion_agencia") or "").strip(),
        "seguimiento_servicio": str(suggestion.get("seguimiento_servicio") or "").strip(),
        "cargo_servicio": "",
        "total_personas": 0,
        "horas_interprete": (
            analysis["sumatoria_horas_interpretes"]
            if analysis.get("sumatoria_horas_interpretes") is not None and analysis.get("sumatoria_horas_interpretes") != ""
            else analysis["total_horas_interprete"]
            if analysis.get("total_horas_interprete") is not None and analysis.get("total_horas_interprete") != ""
            else ""
        ),
        "mes_servicio": month,
        "ano_servicio": year,
        "referencia_servicio": str(suggestion.get("referencia_servicio") or "").strip(),
        "descripcion_servicio": str(suggestion.get("descripcion_servicio") or "").strip(),
        "_document_filename": str(attachment.get("filename") or "").strip(),
        "_document_kind": str(attachment.get("document_kind") or "").strip(),
        "_decision_rationale": list(suggestion.get("rationale") or []),
        "_analysis_warnings": list(analysis.get("warnings") or []),
        "_suggestion_confidence": str(suggestion.get("confidence") or "").strip(),
    }


def _should_flag_revisar(row: dict) -> bool:
    if list(row.get("_analysis_warnings") or []):
        return True
    if str(row.get("_suggestion_confidence") or "") == "low":
        return True
    return False


def _rows_for_document(*, message: dict, attachment: dict, analysis: dict, suggestion: dict) -> tuple[list[dict], list[dict], list[str]]:
    upload_rows: list[dict] = []
    preview_rows: list[dict] = []
    decision_entries: list[str] = []
    codigo = str(suggestion.get("codigo_servicio") or "").strip()
    blocking_errors = list(analysis.get("_blocking_errors") or [])
    if blocking_errors:
        decision_entries.append(
            _build_decision_log_entry(
                kind="omitido",
                message=message,
                attachment=attachment,
                details=blocking_errors,
            )
        )
        return upload_rows, preview_rows, decision_entries
    if not codigo:
        decision_entries.append(
            _build_decision_log_entry(
                kind="omitido",
                message=message,
                attachment=attachment,
                details=["No se pudo proponer codigo_servicio para este documento."],
            )
        )
        return upload_rows, preview_rows, decision_entries

    company = _canonicalize_company_in_analysis(analysis)
    if not company:
        decision_entries.append(
            _build_decision_log_entry(
                kind="omitido",
                message=message,
                attachment=attachment,
                details=["No se generaron filas porque empresa/NIT no coinciden con Supabase."],
            )
        )
        return upload_rows, preview_rows, decision_entries

    base_row = _base_upload_row(
        message=message,
        attachment=attachment,
        analysis=analysis,
        suggestion=suggestion,
        company=company,
    )
    participants = list(analysis.get("participantes") or [])
    allow_blank = str(attachment.get("document_kind") or "") in _NO_PARTICIPANT_ROW_KINDS
    source_rows: list[dict] = []
    interpreter_names = list(analysis.get("interpretes") or []) if str(attachment.get("document_kind") or "") == "interpreter_service" else []
    if participants:
        for participant in participants:
            target_professionals = interpreter_names or [str(base_row.get("nombre_profesional") or "").strip()]
            for professional_name in target_professionals:
                row = dict(base_row)
                if professional_name:
                    row["nombre_profesional"] = professional_name
                row["nombre_usuario"] = str(participant.get("nombre_usuario") or "").strip()
                row["cedula_usuario"] = str(participant.get("cedula_usuario") or "").strip()
                row["discapacidad_usuario"] = str(participant.get("discapacidad_usuario") or "").strip()
                row["genero_usuario"] = str(participant.get("genero_usuario") or "").strip()
                row["fecha_ingreso"] = _normalize_iso_date(str(participant.get("fecha_ingreso") or ""))
                row["tipo_contrato"] = str(participant.get("tipo_contrato") or "").strip()
                row["cargo_servicio"] = str(participant.get("cargo_servicio") or "").strip()
                row["total_personas"] = 1
                source_rows.append(row)
    elif allow_blank:
        row = dict(base_row)
        row["total_personas"] = 0
        source_rows.append(row)
        decision_entries.append(
            _build_decision_log_entry(
                kind="sin_oferente",
                message=message,
                attachment=attachment,
                details=["Se genero fila sin oferente porque la familia de servicio lo permite."],
            )
        )
    else:
        decision_entries.append(
            _build_decision_log_entry(
                kind="omitido",
                message=message,
                attachment=attachment,
                details=["No se generaron filas porque el documento requiere oferentes validos."],
            )
        )
        return upload_rows, preview_rows, decision_entries

    for row in source_rows:
        row["id"] = _stable_upload_id(
            str(message.get("message_id") or ""),
            str(attachment.get("filename") or ""),
            codigo,
            str(row.get("cedula_usuario") or ""),
            str(row.get("nombre_profesional") or ""),
        )
        row["revisar_flag"] = "REVISAR" if _should_flag_revisar(row) else ""
        upload_rows.append(row)
        preview_rows.append(
            {
                "documento": str(attachment.get("filename") or "-"),
                "codigo": codigo,
                "empresa": str(row.get("nombre_empresa") or "-"),
                "nit": str(row.get("nit_empresa") or "-"),
                "fecha": str(row.get("fecha_servicio") or "-"),
                "oferente": str(row.get("nombre_usuario") or "-"),
                "cedula": str(row.get("cedula_usuario") or "-"),
                "modalidad": str(row.get("modalidad_servicio") or "-"),
                "horas": str(row.get("horas_interprete") or "-"),
                "observaciones": str(row.get("observaciones") or "-"),
                "revisar": row["revisar_flag"],
            }
        )
        decision_entries.append(
            _build_decision_log_entry(
                kind="preparado",
                message=message,
                attachment=attachment,
                row=row,
                details=list(row.get("_decision_rationale") or []) + list(row.get("_analysis_warnings") or []),
            )
        )

    return upload_rows, preview_rows, decision_entries


def _process_email_preview_internal(message_id: str) -> dict:
    if not message_id:
        raise RuntimeError("Debe indicar message_id para procesar el correo.")

    gateway = _gmail_gateway()
    allowed_senders = _professional_email_map()
    message_ref = gateway.get_message_ref(message_id)
    message = message_ref.to_dict()
    attachments = [item.to_dict() for item in gateway.list_pdf_attachments(message_ref)]
    if not attachments:
        raise RuntimeError("El correo seleccionado no trae adjuntos PDF procesables.")

    document_results: list[dict] = []
    skipped_entries: list[str] = []
    for attachment in attachments:
        if not bool(attachment.get("is_ods_candidate")):
            skipped_entries.append(
                _build_decision_log_entry(
                    kind="omitido",
                    message=message,
                    attachment=attachment,
                    details=[str(attachment.get("classification_reason") or "Documento clasificado como soporte o anexo.")],
                )
            )
            continue
        try:
            parsed = _download_and_parse_attachment(
                gateway=gateway,
                message_ref=message_ref,
                attachment=attachment,
            )
        except Exception as exc:
            skipped_entries.append(
                _build_decision_log_entry(
                    kind="error_parse",
                    message=message,
                    attachment=attachment,
                    details=[f"Error al descargar o parsear el PDF: {exc}"],
                )
            )
            continue
        parsed = _match_sender_and_prepare_analysis(
            parsed=parsed,
            attachment=attachment,
            message=message,
            allowed_senders=allowed_senders,
        )
        document_results.append(
            {
                "attachment": attachment,
                "analysis": parsed,
            }
        )

    _resolve_interpreter_context(document_results)

    upload_rows: list[dict] = []
    preview_rows: list[dict] = []
    decision_entries = list(skipped_entries)
    for item in document_results:
        attachment = dict(item.get("attachment") or {})
        analysis = dict(item.get("analysis") or {})
        suggestion = suggest_service_from_analysis(analysis=analysis, message=message).to_dict()
        item["suggestion"] = suggestion
        rows, preview, logs = _rows_for_document(
            message=message,
            attachment=attachment,
            analysis=analysis,
            suggestion=suggestion,
        )
        upload_rows.extend(rows)
        preview_rows.extend(preview)
        decision_entries.extend(logs)

    return {
        "message": message,
        "upload_rows": upload_rows,
        "preview_rows": preview_rows,
        "skipped_count": len(skipped_entries),
        "ready_to_upload": bool(upload_rows),
        "decision_log_entries": decision_entries,
    }


def _publish_email_preview_internal(payload: dict) -> dict:
    settings = get_settings()
    spreadsheet_id = str(settings.google_sheets_automation_test_spreadsheet_id or "").strip()
    sheet_name = str(settings.google_sheets_automation_test_sheet_name or "ODS_INPUT").strip() or "ODS_INPUT"
    if not spreadsheet_id:
        raise RuntimeError("Falta GOOGLE_SHEETS_AUTOMATION_TEST_SPREADSHEET_ID para publicar en Sheets.")

    message = dict(payload.get("message") or {})
    upload_rows = [dict(item) for item in list(payload.get("upload_rows") or [])]
    decision_entries = [str(item) for item in list(payload.get("decision_log_entries") or [])]
    if not upload_rows:
        raise RuntimeError("No hay filas preparadas para publicar.")

    existing_ids = _existing_upload_ids(spreadsheet_id, sheet_name)
    to_write: list[dict] = []
    for row in upload_rows:
        if str(row.get("id") or "").strip() in existing_ids:
            decision_entries.append(
                _build_decision_log_entry(
                    kind="duplicado",
                    message=message,
                    row=row,
                    details=["La fila ya existia en ODS_INPUT y no se volvio a escribir."],
                )
            )
            continue
        to_write.append(row)

    written_count = _write_rows_to_sheet(
        spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, rows=to_write,
    )
    if to_write:
        for row in to_write:
            decision_entries.append(
                _build_decision_log_entry(
                    kind="subido",
                    message=message,
                    row=row,
                    details=["Fila escrita en Google Sheets ODS_INPUT."],
                )
            )

    try:
        log_path = _append_decision_log(decision_entries)
        log_path_str = str(log_path)
    except OSError:
        log_path_str = str(_decisions_log_path())
    return {
        "written_count": written_count,
        "skipped_existing_count": len(upload_rows) - written_count,
        "decisions_log_path": log_path_str,
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": sheet_name,
    }


def build_automation_skeleton_status() -> AutomationSkeletonStatus:
    settings = get_settings()
    process_templates = list_process_template_names()
    gmail_state = "ready" if str(settings.google_gmail_delegated_user or "").strip() else "blocked"
    gmail_description = (
        "Lectura Gmail lista para prueba read-only desde Aaron TEST."
        if gmail_state == "ready"
        else "Falta GOOGLE_GMAIL_DELEGATED_USER para probar acceso al buzón delegado."
    )
    return AutomationSkeletonStatus(
        title="Aaron TEST",
        summary=(
            "Flujo interno para leer un correo, procesar todos sus PDFs candidatos "
            "y confirmar la subida directa a Google Sheets sin escribir en Supabase."
        ),
        environment="test_only",
        components=(
            AutomationComponentStatus(
                key="gmail_inbox",
                title="Bandeja Gmail",
                state=gmail_state,
                description=gmail_description,
            ),
            AutomationComponentStatus(
                key="acta_parser",
                title="Extraccion de Acta",
                state="ready",
                description="La app ya tiene parser de actas PDF/Excel reutilizable desde el flujo de automatizacion.",
            ),
            AutomationComponentStatus(
                key="process_catalog",
                title="Catalogo de Procesos",
                state="ready" if process_templates else "blocked",
                description=(
                    f"Se detectaron {len(process_templates)} nombres de proceso base desde templates."
                    if process_templates
                    else "No se detectaron templates para sugerir el proceso por nombre de archivo."
                ),
            ),
            AutomationComponentStatus(
                key="decision_rules",
                title="Motor de Reglas",
                state="ready",
                description="Reglas activas para codificacion, defaults y correlacion entre PDFs del mismo correo.",
            ),
            AutomationComponentStatus(
                key="google_sheets",
                title="Salida Google Sheets",
                state="ready",
                description="La salida interna apunta a la pestaña ODS_INPUT del spreadsheet de testing.",
            ),
            AutomationComponentStatus(
                key="decisions_log",
                title="Log de Decisiones",
                state="ready",
                description="Las decisiones tecnicas se registran en la ruta configurada del log en lugar de mostrarse en pantalla.",
            ),
        ),
        next_steps=(
            AutomationNextStep(
                title="1. Leer Gmail",
                detail="Listar correos candidatos desde Aaron TEST.",
            ),
            AutomationNextStep(
                title="2. Procesar correo",
                detail="Analizar todos los PDFs del correo, preparar filas ODS_INPUT y mostrar solo el resumen minimo.",
            ),
            AutomationNextStep(
                title="3. Confirmar subida",
                detail="Escribir las filas en ODS_INPUT y registrar las decisiones en el log local.",
            ),
        ),
    )


def get_automation_test_status() -> dict:
    return {"data": build_automation_skeleton_status().to_dict()}


def get_automation_gmail_preview(*, limit: int | None = None) -> dict:
    gateway = _gmail_gateway(limit=limit)
    allowed_senders = _professional_email_map()
    try:
        messages = gateway.list_candidate_messages()
    except Exception as exc:
        error_text = str(exc or "").lower()
        if "unauthorized_client" in error_text:
            raise RuntimeError(
                "Google rechazo la impersonacion Gmail. Revisa Domain-Wide Delegation, "
                "el Client ID autorizado en Admin Console y el scope gmail.readonly."
            ) from exc
        raise

    candidates: list[GmailCandidateEmail] = []
    total_pdf_count = 0
    matched_sender_count = 0
    ods_candidate_count = 0
    for message in messages:
        attachments = tuple(gateway.list_pdf_attachments(message))
        if not attachments:
            continue
        matched_professional = allowed_senders.get(message.sender_email.lower(), "")
        warnings: list[str] = []
        if not matched_professional:
            warnings.append("Remitente no encontrado en la tabla de profesionales.")
        if len(attachments) > 1:
            warnings.append("El correo trae multiples PDFs; cada adjunto debera revisarse por separado.")
        total_pdf_count += len(attachments)
        ods_candidate_count += len([item for item in attachments if item.is_ods_candidate])
        if matched_professional:
            matched_sender_count += 1
        candidates.append(
            GmailCandidateEmail(
                message=message,
                attachments=attachments,
                sender_allowed=bool(matched_professional),
                matched_professional=matched_professional,
                warnings=tuple(warnings),
            )
        )

    return {
        "data": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "delegated_user": gateway.delegated_user,
            "to_filter": gateway.to_filter,
            "message_count": len(candidates),
            "pdf_count": total_pdf_count,
            "ods_candidate_count": ods_candidate_count,
            "matched_sender_count": matched_sender_count,
            "process_template_count": len(list_process_template_names()),
            "messages": [item.to_dict() for item in candidates],
        }
    }


def process_automation_email_preview(payload: dict) -> dict:
    message_id = str(payload.get("message_id") or "").strip()
    return {"data": _process_email_preview_internal(message_id)}


def publish_automation_email_preview(payload: dict) -> dict:
    return {"data": _publish_email_preview_internal(payload)}


def run_batch_eod_scan(*, limit: int | None = None) -> dict:
    """Fetch all unprocessed Gmail candidate emails and parse each one.
    Does NOT write anything to Sheets.  Returns a summary for the confirmation step."""
    from app.automation.processed_log import ProcessedEmailLog

    log = ProcessedEmailLog()
    already_processed_ids = log.get_processed_ids()
    gateway = _gmail_gateway(limit=limit)

    try:
        messages = gateway.list_candidate_messages()
    except Exception as exc:
        error_text = str(exc or "").lower()
        if "unauthorized_client" in error_text:
            raise RuntimeError(
                "Google rechazo la impersonacion Gmail. Revisa Domain-Wide Delegation y el scope gmail.readonly."
            ) from exc
        raise

    results: list[dict] = []
    stats = {
        "total_fetched": len(messages),
        "already_processed": 0,
        "ignored": 0,
        "errors": 0,
        "ready": 0,
        "warnings": 0,
    }

    for message_ref in messages:
        message_id = str(message_ref.message_id or "").strip()
        subject = str(message_ref.subject or "").strip()

        if message_id in already_processed_ids:
            stats["already_processed"] += 1
            continue

        try:
            result = _process_email_preview_internal(message_id)
        except Exception as exc:
            stats["errors"] += 1
            results.append({
                "status": "error",
                "message_id": message_id,
                "subject": subject,
                "error": str(exc),
                "upload_rows": [],
                "preview_rows": [],
                "decision_log_entries": [],
            })
            continue

        upload_rows = list(result.get("upload_rows") or [])
        if not upload_rows:
            stats["ignored"] += 1
            result["status"] = "ignored"
        else:
            stats["ready"] += 1
            result["status"] = "ready"
            if any(str(row.get("revisar_flag") or "") == "REVISAR" for row in upload_rows):
                stats["warnings"] += 1

        result.setdefault("message_id", message_id)
        result.setdefault("subject", subject)
        results.append(result)

    return {"data": {"stats": stats, "results": results}}


def confirm_batch_eod_upload(payload: dict) -> dict:
    """Write all ready scan results to Sheets and mark each email as processed."""
    from app.automation.processed_log import ProcessedEmailLog

    log = ProcessedEmailLog()
    settings = get_settings()
    spreadsheet_id = str(settings.google_sheets_automation_test_spreadsheet_id or "").strip()
    sheet_name = str(settings.google_sheets_automation_test_sheet_name or "ODS_INPUT").strip() or "ODS_INPUT"
    if not spreadsheet_id:
        raise RuntimeError("Falta GOOGLE_SHEETS_AUTOMATION_TEST_SPREADSHEET_ID para publicar en Sheets.")
    results = list(payload.get("results") or [])
    existing_ids = _existing_upload_ids(spreadsheet_id, sheet_name)

    totals: dict[str, int] = {"uploaded": 0, "duplicates": 0, "ignored": 0, "errors": 0, "warnings": 0}
    all_decision_entries: list[str] = []

    for result in results:
        status = str(result.get("status") or "")
        message = dict(result.get("message") or {})
        message_id = str(result.get("message_id") or message.get("message_id") or "").strip()
        subject = str(result.get("subject") or message.get("subject") or "").strip()

        all_decision_entries.extend(list(result.get("decision_log_entries") or []))

        if status == "error":
            totals["errors"] += 1
            continue

        if status == "ignored":
            totals["ignored"] += 1
            log.mark_processed(message_id, subject)
            continue

        upload_rows = [dict(r) for r in list(result.get("upload_rows") or [])]
        to_write: list[dict] = []
        for row in upload_rows:
            row_id = str(row.get("id") or "").strip()
            if row_id in existing_ids:
                totals["duplicates"] += 1
                all_decision_entries.append(
                    _build_decision_log_entry(
                        kind="duplicado",
                        message=message,
                        row=row,
                        details=["La fila ya existia en ODS_INPUT y no se volvio a escribir."],
                    )
                )
            else:
                to_write.append(row)

        _write_rows_to_sheet(
            spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, rows=to_write,
        )
        for row in to_write:
            row_id = str(row.get("id") or "").strip()
            existing_ids.add(row_id)
            totals["uploaded"] += 1
            if str(row.get("revisar_flag") or "") == "REVISAR":
                totals["warnings"] += 1
            all_decision_entries.append(
                _build_decision_log_entry(
                    kind="subido",
                    message=message,
                    row=row,
                    details=["Fila escrita en Google Sheets ODS_INPUT (batch fin de dia)."],
                )
            )

        log.mark_processed(message_id, subject)

    try:
        log_path = _append_decision_log(all_decision_entries)
        log_path_str = str(log_path)
    except OSError:
        log_path_str = str(_decisions_log_path())

    return {
        "data": {
            **totals,
            "decisions_log_path": log_path_str,
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
        }
    }


def get_automation_attachment_analysis(payload: dict) -> dict:
    message_id = str(payload.get("message_id") or "").strip()
    attachment_id = str(payload.get("attachment_id") or "").strip()
    attachment_index = payload.get("attachment_index")
    filename = str(payload.get("filename") or "adjunto.pdf").strip() or "adjunto.pdf"
    if not message_id:
        raise RuntimeError("Debe indicar message_id para analizar el PDF.")
    if attachment_id == "" and attachment_index in (None, ""):
        raise RuntimeError("Debe indicar attachment_id o attachment_index para analizar el PDF.")

    gateway = _gmail_gateway()
    allowed_senders = _professional_email_map()
    message = gateway.get_message_ref(message_id)
    attachments = list(gateway.list_pdf_attachments(message))
    attachment = None
    if attachment_index not in (None, ""):
        try:
            idx = int(attachment_index)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("attachment_index invalido.") from exc
        if idx < 0 or idx >= len(attachments):
            raise RuntimeError("El attachment_index seleccionado no existe en Gmail.")
        attachment = attachments[idx]
    elif attachment_id:
        attachment = next((item for item in attachments if item.attachment_id == attachment_id), None)
    if attachment is None:
        raise RuntimeError("No se encontro el adjunto PDF seleccionado en Gmail.")

    temp_path: Path | None = None
    try:
        suffix = Path(filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            temp_path = Path(tmp.name)
            tmp.write(gateway.download_attachment_bytes(message, attachment))

        parsed = parse_acta_pdf(str(temp_path))
        parsed["file_path"] = attachment.filename
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

    import_result = build_import_result_from_parsed(
        parsed,
        source_label=attachment.filename,
        message=message.to_dict(),
        attachment=attachment.to_dict(),
        create_missing_interpreter=True,
    )
    parsed = dict(import_result.get("analysis") or {})
    discarded_ids = list(parsed.get("_cedulas_descartadas") or [])
    parsed["participant_summary"] = _participant_summary(list(parsed.get("participantes") or []), discarded_ids)
    suggestion = dict(import_result.get("service_suggestion") or {})

    return {
        "data": {
            "message": message.to_dict(),
            "attachment": attachment.to_dict(),
            "analysis": parsed,
            "suggestion": suggestion,
        }
    }


def get_automation_staging_cases() -> dict:
    repo = AutomationStagingRepository()
    cases = repo.list_cases()
    return {
        "data": {
            "count": len(cases),
            "cases": [item.to_dict() for item in cases],
        }
    }


def save_automation_staging_case(payload: dict) -> dict:
    analyzed = get_automation_attachment_analysis(payload)
    data = dict(analyzed.get("data") or {})
    repo = AutomationStagingRepository()
    staged = repo.save_case(
        message=dict(data.get("message") or {}),
        attachment=dict(data.get("attachment") or {}),
        analysis=dict(data.get("analysis") or {}),
        suggestion=dict(data.get("suggestion") or {}),
    )
    return {"data": staged.to_dict()}


def get_automation_staging_case(case_id: str) -> dict:
    repo = AutomationStagingRepository()
    case = repo.get_case(case_id)
    if case is None:
        raise RuntimeError(f"No se encontro el caso de staging {case_id}.")
    return {"data": case.to_dict()}


def update_automation_staging_case(payload: dict) -> dict:
    case_id = str(payload.get("case_id") or "").strip()
    suggestion_updates = dict(payload.get("suggestion") or {})
    status = str(payload.get("status") or "").strip() or None
    repo = AutomationStagingRepository()
    case = repo.update_case(
        case_id=case_id,
        suggestion_updates=suggestion_updates,
        status=status,
    )
    return {"data": case.to_dict()}
