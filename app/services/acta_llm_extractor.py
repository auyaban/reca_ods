from __future__ import annotations

import base64
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import re
from pathlib import Path
from typing import Any
import urllib.error
import urllib.request

from app.automation.process_profiles import (
    build_detailed_extraction_instructions,
    build_profile_prompt_context,
    get_process_profile,
    get_profile_priority_labels,
)
from app.config import get_settings
from app.paths import app_data_dir
from app.services.excel_acta_import import _extract_pdf_text_pages, parse_acta_pdf
from app.utils.text import normalize_search_text

_EDGE_TIMEOUT_SECONDS = 240
_LLM_LONG_SECTION_MAX_CHARS = 300
_PDF_BASE64_MAX_BYTES = 10 * 1024 * 1024  # 10 MB limit before base64 encoding

_LONG_TEXT_SECTION_PATTERNS = (
    "descripcion",
    "desarrollo",
    "observaciones",
    "conclusiones",
    "recomendaciones",
    "compromisos",
    "hallazgos",
    "analisis",
    "resumen",
)

_HEADING_LINE_RE = re.compile(r"^\s*(?:\d+(?:\.\d+)*[.):-]?\s*)?[A-Za-z0-9][^:]{0,120}:?\s*$")

_STRUCTURED_ACTA_SCHEMA: dict[str, Any] = {
    "name": "acta_ods_extraction",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "schema_version": {"type": "string"},
            "extraction_status": {
                "type": "string",
                "enum": ["ok", "needs_review", "invalid"],
            },
            "document_type_hint": {"type": "string"},
            "process_name_hint": {"type": "string"},
            "nit_empresa": {"type": "string"},
            "nits_empresas": {
                "type": "array",
                "items": {"type": "string"},
            },
            "nombre_empresa": {"type": "string"},
            "fecha_servicio": {"type": "string"},
            "nombre_profesional": {"type": "string"},
            "interpretes": {
                "type": "array",
                "items": {"type": "string"},
            },
            "asistentes": {
                "type": "array",
                "items": {"type": "string"},
            },
            "modalidad_servicio": {"type": "string"},
            "gestion_empresarial": {"type": "string"},
            "tamano_empresa": {"type": "string"},
            "cargo_objetivo": {"type": "string"},
            "total_vacantes": {"type": "integer"},
            "numero_seguimiento": {"type": "string"},
            "total_empresas": {"type": "integer"},
            "is_fallido": {"type": "boolean"},
            "total_horas_interprete": {"type": "number"},
            "sumatoria_horas_interpretes": {"type": "number"},
            "participantes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "nombre_usuario": {"type": "string"},
                        "cedula_usuario": {"type": "string"},
                        "discapacidad_usuario": {"type": "string"},
                        "genero_usuario": {"type": "string"},
                    },
                    "required": [
                        "nombre_usuario",
                        "cedula_usuario",
                        "discapacidad_usuario",
                        "genero_usuario",
                    ],
                },
            },
            "warnings": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": [
            "schema_version",
            "extraction_status",
            "document_type_hint",
            "process_name_hint",
            "nit_empresa",
            "nits_empresas",
            "nombre_empresa",
            "fecha_servicio",
            "nombre_profesional",
            "interpretes",
            "asistentes",
            "modalidad_servicio",
            "gestion_empresarial",
            "tamano_empresa",
            "cargo_objetivo",
            "total_vacantes",
            "numero_seguimiento",
            "total_empresas",
            "is_fallido",
            "total_horas_interprete",
            "sumatoria_horas_interpretes",
            "participantes",
            "warnings",
        ],
    },
}


def get_acta_llm_schema() -> dict[str, Any]:
    return deepcopy(_STRUCTURED_ACTA_SCHEMA)


def _clean_text_value(value: Any) -> str:
    return str(value or "").strip()


def _normalize_date_value(value: Any) -> str:
    text = _clean_text_value(value)
    if not text:
        return ""
    # Strip trailing time component (e.g. "2026-03-02T00:00:00" → "2026-03-02")
    if "T" in text:
        text = text.split("T")[0].strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _normalize_string_list(values: Any) -> list[str]:
    result: list[str] = []
    for item in list(values or []):
        text = _clean_text_value(item)
        if text and text not in result:
            result.append(text)
    return result


def _normalize_participants(rows: Any) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for item in list(rows or []):
        if not isinstance(item, dict):
            continue
        participant = {
            "nombre_usuario": _clean_text_value(item.get("nombre_usuario")),
            "cedula_usuario": _clean_text_value(item.get("cedula_usuario")),
            "discapacidad_usuario": _clean_text_value(item.get("discapacidad_usuario")),
            "genero_usuario": _clean_text_value(item.get("genero_usuario")),
        }
        if not participant["nombre_usuario"] and not participant["cedula_usuario"]:
            continue
        result.append(participant)
    return result


def _compact_normalized(value: str) -> str:
    return " ".join(normalize_search_text(value).split())


def _cargo_has_explicit_source(source_text: str, cargo_objetivo: str, profile: dict[str, Any]) -> bool:
    cargo_norm = _compact_normalized(cargo_objetivo)
    if not cargo_norm:
        return True
    priority_map = dict(profile.get("field_priority") or {})
    labels = [str(item or "").strip() for item in list(priority_map.get("cargo_objetivo") or []) if str(item or "").strip()]
    if not labels:
        return True
    label_norms = [_compact_normalized(label) for label in labels]
    lines = [str(line or "").strip() for line in str(source_text or "").splitlines() if str(line or "").strip()]
    normalized_lines = [_compact_normalized(line) for line in lines]
    for index, line_norm in enumerate(normalized_lines):
        if not any(label in line_norm for label in label_norms):
            continue
        window = " ".join(normalized_lines[index:index + 3])
        if cargo_norm in window:
            return True
    return False


def _is_heading_line(line: str) -> bool:
    stripped = str(line or "").strip()
    if not stripped or len(stripped) > 140:
        return False
    if stripped.count(" ") > 14:
        return False
    return bool(_HEADING_LINE_RE.match(stripped))


def _is_long_text_heading(line: str) -> bool:
    normalized = normalize_search_text(line)
    return any(token in normalized for token in _LONG_TEXT_SECTION_PATTERNS)


def _match_profile_section(line: str, profile: dict[str, Any] | None) -> str:
    if not profile:
        return ""
    normalized_line = normalize_search_text(line)
    aliases = dict(profile.get("normalized_section_aliases") or {})
    for section_key, alias in aliases.items():
        if alias and re.search(r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])", normalized_line):
            return section_key
    return ""


def _trim_long_inline_line(line: str) -> str:
    text = str(line or "").strip()
    if not text or ":" not in text:
        return text
    prefix, suffix = text.split(":", 1)
    if not _is_long_text_heading(prefix):
        return text
    suffix = re.sub(r"\s+", " ", suffix).strip()
    if len(suffix) > _LLM_LONG_SECTION_MAX_CHARS:
        suffix = suffix[:_LLM_LONG_SECTION_MAX_CHARS].rstrip() + "..."
    return f"{prefix.strip()}: {suffix}".strip() if suffix else f"{prefix.strip()}:"


def _apply_line_mode(lines: list[str], profile: dict[str, Any] | None) -> list[str]:
    if not profile or str(profile.get("line_mode") or "") != "labeled_only":
        return lines
    filtered: list[str] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        normalized = normalize_search_text(text)
        if _match_profile_section(text, profile):
            filtered.append(text)
            continue
        if ":" in text:
            filtered.append(text)
            continue
        if normalized.startswith("nombre completo"):
            filtered.append(text)
            continue
    return filtered


def _collect_priority_snippets(pages: list[str], labels: list[str]) -> list[str]:
    normalized_labels = [normalize_search_text(label) for label in labels if str(label or "").strip()]
    if not normalized_labels:
        return []
    snippets: list[str] = []
    seen: set[str] = set()
    for page in pages:
        page_lines = [str(item or "").strip() for item in str(page or "").splitlines()]
        for line in page_lines:
            if not line:
                continue
            normalized_line = normalize_search_text(line)
            if not normalized_line:
                continue
            if any(label in normalized_line for label in normalized_labels):
                compact = re.sub(r"\s+", " ", line).strip()
                if compact and compact not in seen:
                    seen.add(compact)
                    snippets.append(compact)
    return snippets


def _filter_pages_by_profile(pages: list[str], profile: dict[str, Any] | None) -> list[str]:
    if not profile:
        return pages
    keep_sections = set(profile.get("keep_sections") or [])
    if not keep_sections:
        return pages

    filtered_pages: list[str] = []
    for page in pages:
        page_lines = [str(item or "").rstrip() for item in str(page or "").splitlines()]
        current_section = ""
        kept_lines: list[str] = []
        for raw_line in page_lines:
            line = raw_line.strip()
            if not line:
                if kept_lines and kept_lines[-1] != "":
                    kept_lines.append("")
                continue
            matched_section = _match_profile_section(line, profile)
            if matched_section:
                current_section = matched_section if matched_section in keep_sections else ""
                if matched_section in keep_sections:
                    kept_lines.append(line)
                continue
            if _is_heading_line(line) and not _is_long_text_heading(line):
                current_section = ""
                continue
            if current_section in keep_sections:
                kept_lines.append(line)
        filtered_page = "\n".join(kept_lines).strip()
        if filtered_page:
            filtered_pages.append(filtered_page)
    return filtered_pages or pages


def _prepare_llm_source_text(pages: list[str], *, document_kind: str = "") -> str:
    profile = get_process_profile(document_kind)
    filtered_pages = _filter_pages_by_profile(pages, profile)
    priority_lines = _collect_priority_snippets(pages, get_profile_priority_labels(document_kind))
    lines: list[str] = []
    for page in filtered_pages:
        page_lines = [str(item or "").rstrip() for item in str(page or "").splitlines()]
        i = 0
        while i < len(page_lines):
            line = page_lines[i].strip()
            if not line:
                i += 1
                continue
            inline_trimmed = _trim_long_inline_line(line)
            if inline_trimmed != line:
                lines.append(inline_trimmed)
                i += 1
                continue
            if _is_long_text_heading(line):
                section_lines: list[str] = []
                j = i + 1
                while j < len(page_lines):
                    candidate_raw = page_lines[j]
                    candidate = candidate_raw.strip()
                    if not candidate:
                        if section_lines:
                            break
                        j += 1
                        continue
                    if _is_heading_line(candidate) and not _is_long_text_heading(candidate):
                        break
                    section_lines.append(candidate)
                    j += 1
                section_text = re.sub(r"\s+", " ", " ".join(section_lines)).strip()
                if len(section_text) > _LLM_LONG_SECTION_MAX_CHARS:
                    section_text = section_text[:_LLM_LONG_SECTION_MAX_CHARS].rstrip() + "..."
                lines.append(line)
                if section_text:
                    lines.append(section_text)
                i = j
                continue
            lines.append(line)
            i += 1
    combined: list[str] = []
    seen_headers: set[str] = set()
    for item in [*priority_lines, *lines]:
        text_line = str(item or "").strip()
        if not text_line:
            continue
        # Only deduplicate pure section headers (heading pattern, no colon separating label from value).
        # Data lines (field labels like "Nombre completo:" or value lines) are always kept so that
        # multi-page documents with repeated section structures preserve all participant/company data.
        is_pure_header = ":" not in text_line and _is_heading_line(text_line) and not _is_long_text_heading(text_line)
        if is_pure_header:
            if text_line in seen_headers:
                continue
            seen_headers.add(text_line)
        combined.append(text_line)
    combined = _apply_line_mode(combined, profile)
    return "\n".join(combined).strip()


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


def _json_output_dir() -> Path:
    settings = get_settings()
    configured = str(getattr(settings, "automation_decisions_log_path", "") or "").strip()
    if configured:
        path = Path(configured).expanduser().resolve().parent / "JSONs"
    else:
        path = app_data_dir() / "logs" / "JSONs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_json_stem(value: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(value or "acta"))
    clean = clean.strip("._") or "acta"
    return clean[:80]


def _save_llm_json(*, source_name: str, filename: str, subject: str, function_name: str, raw_data: dict[str, Any], normalized: dict[str, Any]) -> str:
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    path = _json_output_dir() / f"{stamp}_{_safe_json_stem(filename or source_name)}.json"
    payload = {
        "saved_at": now.isoformat(),
        "source_label": source_name,
        "filename": filename,
        "subject": subject,
        "function_name": function_name,
        "raw": raw_data,
        "normalized": normalized,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _normalize_llm_result(raw_data: dict[str, Any], *, source_label: str, source_text: str) -> dict[str, Any]:
    extraction_status = _clean_text_value(raw_data.get("extraction_status")) or "invalid"
    warnings = [_clean_text_value(item) for item in list(raw_data.get("warnings") or []) if _clean_text_value(item)]
    asistentes = _normalize_string_list(raw_data.get("asistentes"))
    interpretes = _normalize_string_list(raw_data.get("interpretes"))
    document_type_hint = _clean_text_value(raw_data.get("document_type_hint"))
    nombre_profesional = _clean_text_value(raw_data.get("nombre_profesional"))
    if not interpretes and document_type_hint == "interpreter_service" and nombre_profesional:
        interpretes = [nombre_profesional]
    result = {
        "file_path": source_label,
        "source_type": "llm_edge",
        "llm_extraction_used": True,
        "llm_extraction_status": extraction_status,
        "llm_review_required": extraction_status != "ok",
        "llm_source_text_length": len(source_text),
        "llm_schema_version": _clean_text_value(raw_data.get("schema_version")) or "v1",
        "document_type_hint": document_type_hint,
        "process_name_hint": _clean_text_value(raw_data.get("process_name_hint")),
        "nit_empresa": _clean_text_value(raw_data.get("nit_empresa")),
        "nits_empresas": _normalize_string_list(raw_data.get("nits_empresas")),
        "nombre_empresa": _clean_text_value(raw_data.get("nombre_empresa")),
        "fecha_servicio": _normalize_date_value(raw_data.get("fecha_servicio")),
        "nombre_profesional": nombre_profesional,
        "interpretes": interpretes,
        "asistentes": asistentes,
        "candidatos_profesional": asistentes,
        "modalidad_servicio": _clean_text_value(raw_data.get("modalidad_servicio")),
        "gestion_empresarial": _clean_text_value(raw_data.get("gestion_empresarial")),
        "tamano_empresa": _clean_text_value(raw_data.get("tamano_empresa")),
        "cargo_objetivo": _clean_text_value(raw_data.get("cargo_objetivo")),
        "total_vacantes": int(raw_data.get("total_vacantes") or 0),
        "numero_seguimiento": _clean_text_value(raw_data.get("numero_seguimiento")),
        "total_empresas": int(raw_data.get("total_empresas") or 0),
        "is_fallido": bool(raw_data.get("is_fallido")),
        "total_horas_interprete": raw_data["total_horas_interprete"] if raw_data.get("total_horas_interprete") is not None else "",
        "sumatoria_horas_interpretes": raw_data["sumatoria_horas_interpretes"] if raw_data.get("sumatoria_horas_interpretes") is not None else "",
        "participantes": _normalize_participants(raw_data.get("participantes")),
        "warnings": warnings,
    }
    if result["llm_review_required"]:
        result["warnings"] = list(dict.fromkeys(result["warnings"] + ["Extraccion LLM marcada para revision manual."]))
    return result


def _apply_profile_postprocessing(result: dict[str, Any], *, document_kind: str, source_text: str = "") -> dict[str, Any]:
    profile = get_process_profile(document_kind)
    if not profile:
        return result
    warnings = list(result.get("warnings") or [])
    for field_name in list(profile.get("forbid_fields") or []):
        if field_name not in result:
            continue
        current = result.get(field_name)
        if current in ("", 0, False, [], None):
            continue
        result[field_name] = "" if isinstance(current, str) else 0 if isinstance(current, (int, float)) else [] if isinstance(current, list) else False
        warnings.append(f"Campo omitido por perfil del documento: {field_name}.")
        result["llm_review_required"] = True
        result["llm_extraction_status"] = "needs_review"

    cargo_objetivo = _clean_text_value(result.get("cargo_objetivo"))
    asistentes = {normalize_search_text(_clean_text_value(item)) for item in list(result.get("asistentes") or []) if _clean_text_value(item)}
    if cargo_objetivo and normalize_search_text(cargo_objetivo) in asistentes:
        result["cargo_objetivo"] = ""
        warnings.append("Cargo objetivo omitido porque coincide con un asistente.")
        result["llm_review_required"] = True
        result["llm_extraction_status"] = "needs_review"
    elif len(cargo_objetivo) > 80:
        result["cargo_objetivo"] = ""
        warnings.append("Cargo objetivo omitido por longitud sospechosa.")
        result["llm_review_required"] = True
        result["llm_extraction_status"] = "needs_review"
    elif document_kind != "interpreter_service" and cargo_objetivo and not _cargo_has_explicit_source(source_text, cargo_objetivo, profile):
        result["cargo_objetivo"] = ""
        warnings.append("Cargo objetivo omitido porque no proviene de una etiqueta explicita valida.")
        result["llm_review_required"] = True
        result["llm_extraction_status"] = "needs_review"

    result["warnings"] = list(dict.fromkeys(warnings))
    return result


def _extract_json_payload(response: Any) -> dict[str, Any]:
    if isinstance(response, dict):
        for key in ("data", "result", "output"):
            nested = response.get(key)
            if isinstance(nested, dict):
                return nested
        return response
    raise RuntimeError("La Edge Function no devolvio JSON util para la extraccion del acta.")


def _invoke_edge_function_http(function_name: str, request_payload: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    supabase_url = str(settings.supabase_url or "").strip().rstrip("/")
    supabase_anon_key = str(settings.supabase_anon_key or "").strip()
    if not supabase_url or not supabase_anon_key:
        raise RuntimeError("Falta SUPABASE_URL o SUPABASE_ANON_KEY para invocar la Edge Function.")

    url = f"{supabase_url}/functions/v1/{function_name}"
    body = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "apikey": supabase_anon_key,
            "Authorization": f"Bearer {supabase_anon_key}",
            "Content-Type": "application/json",
            **(
                {"x-acta-extraction-secret": str(getattr(settings, "supabase_edge_acta_extraction_secret", "") or "").strip()}
                if str(getattr(settings, "supabase_edge_acta_extraction_secret", "") or "").strip()
                else {}
            ),
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=_EDGE_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"error": raw or f"HTTP {exc.code}"}
        raise RuntimeError(f"Edge Function HTTP {exc.code}: {payload}") from exc
    except TimeoutError as exc:
        raise RuntimeError("Edge Function timeout excedido.") from exc
    except OSError as exc:
        raise RuntimeError(f"No se pudo invocar la Edge Function: {exc}") from exc

    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError("La Edge Function devolvio una respuesta no JSON.") from exc


def extract_structured_acta_pdf(
    file_path: str,
    *,
    filename: str = "",
    subject: str = "",
    source_label: str = "",
    model_override: str = "",
    provider_override: str = "",
    use_full_pdf: bool = True,
) -> dict[str, Any]:
    path = Path(file_path)
    settings = get_settings()
    source_name = source_label or filename or path.name or str(file_path)

    fallback = parse_acta_pdf(file_path)
    fallback.setdefault("warnings", [])
    fallback["llm_extraction_used"] = False
    fallback["llm_extraction_status"] = "disabled"
    fallback["llm_review_required"] = False

    if not settings.automation_llm_extraction_enabled:
        return fallback

    function_name = _clean_text_value(settings.supabase_edge_acta_extraction_function)
    if not function_name:
        fallback["warnings"] = list(dict.fromkeys(list(fallback.get("warnings") or []) + ["No se configuro la Edge Function de extraccion LLM."]))
        return fallback

    from app.automation.document_classifier import classify_document

    pages = _extract_pdf_text_pages(file_path)
    non_empty_pages = [page for page in pages if str(page or "").strip()]
    classified = classify_document(filename=filename or path.name, subject=subject)
    document_kind_hint = str(classified.document_kind or "").strip()
    source_text = _prepare_llm_source_text(
        non_empty_pages,
        document_kind=document_kind_hint,
    )
    full_page_text = "\n".join(non_empty_pages)

    priority_labels = get_profile_priority_labels(document_kind_hint)
    profile_prompt = build_profile_prompt_context(document_kind_hint)
    detailed_prompt = build_detailed_extraction_instructions(document_kind_hint)
    text_parts: list[str] = []
    if document_kind_hint:
        text_parts.append(f"document_kind_hint: {document_kind_hint}")
    if priority_labels:
        text_parts.append("campos_prioritarios: " + ", ".join(priority_labels[:10]))
    if profile_prompt:
        text_parts.append("perfil_documento:\n" + profile_prompt)
    if detailed_prompt:
        text_parts.append(detailed_prompt)
    llm_instruction_text = "\n".join(part for part in text_parts if str(part or "").strip())

    request_payload = {
        "schema_name": _STRUCTURED_ACTA_SCHEMA["name"],
        "schema": get_acta_llm_schema()["schema"],
        "source_label": source_name,
        "filename": filename or path.name,
        "subject": subject,
        "text": llm_instruction_text,
    }
    if model_override.strip():
        request_payload["model_override"] = model_override.strip()
    request_payload["provider_override"] = provider_override.strip() or "openai"
    if use_full_pdf:
        pdf_size = path.stat().st_size
        if pdf_size > _PDF_BASE64_MAX_BYTES:
            fallback["warnings"] = list(
                dict.fromkeys(
                    list(fallback.get("warnings") or [])
                    + [f"PDF demasiado grande para enviar completo ({pdf_size // (1024 * 1024)} MB); se usara solo el texto extraido."]
                )
            )
        else:
            request_payload["pdf_base64"] = base64.b64encode(path.read_bytes()).decode("ascii")

    try:
        response = _invoke_edge_function_http(function_name, request_payload)
        raw_payload = _extract_json_payload(response)
        normalized = _normalize_llm_result(
            raw_payload,
            source_label=source_name,
            source_text=source_text,
        )
        profile_document_kind = document_kind_hint
        llm_document_kind_hint = _clean_text_value(raw_payload.get("document_type_hint"))
        if not get_process_profile(profile_document_kind) and get_process_profile(llm_document_kind_hint):
            profile_document_kind = llm_document_kind_hint
        if not get_process_profile(profile_document_kind):
            inferred_profile = classify_document(
                filename=f"{filename or path.name} {raw_payload.get('document_type_hint', '')} {raw_payload.get('process_name_hint', '')}",
                subject=subject,
            )
            profile_document_kind = str(inferred_profile.document_kind or "").strip()
        normalized = _apply_profile_postprocessing(normalized, document_kind=profile_document_kind, source_text=full_page_text)
        normalized["llm_json_path"] = _save_llm_json(
            source_name=source_name,
            filename=filename or path.name,
            subject=subject,
            function_name=function_name,
            raw_data=raw_payload,
            normalized=normalized,
        )
        return normalized
    except Exception as exc:
        fallback["warnings"] = list(
            dict.fromkeys(
                list(fallback.get("warnings") or [])
                + [f"No se pudo usar extraccion LLM: {exc}"]
            )
        )
        fallback["llm_extraction_used"] = False
        fallback["llm_extraction_status"] = "fallback_local"
        fallback["llm_review_required"] = False
        return fallback

