from __future__ import annotations

import difflib
from functools import lru_cache
from typing import Any

from app.automation.document_classifier import classify_document
from app.automation.rules_engine import suggest_service_from_analysis
from app.services.excel_acta_import import parse_acta_source
from app.services.sections import seccion1
from app.services.sections.seccion4 import DISCAPACIDADES, GENEROS
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket
from app.utils.text import normalize_text

_CACHE_TTL_SECONDS = 300


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


def _company_name_matches(detected_name: str, company_name: str) -> bool:
    detected = normalize_text(detected_name or "")
    company = normalize_text(company_name or "")
    if not detected or not company:
        return False
    if detected == company or detected in company or company in detected:
        return True
    detected_tokens = set(detected.split())
    company_tokens = set(company.split())
    overlap = len(detected_tokens & company_tokens) / max(len(detected_tokens), len(company_tokens), 1)
    ratio = difflib.SequenceMatcher(None, detected, company).ratio()
    return overlap >= 0.6 or ratio >= 0.8


def _professional_name_matches(candidate: str, professional_name: str) -> float:
    candidate_norm = normalize_text(candidate or "")
    professional_norm = normalize_text(professional_name or "")
    if not candidate_norm or not professional_norm:
        return 0.0
    if candidate_norm == professional_norm:
        return 1.0
    if candidate_norm in professional_norm or professional_norm in candidate_norm:
        return 0.97
    candidate_tokens = set(candidate_norm.split())
    professional_tokens = set(professional_norm.split())
    if len(candidate_tokens) >= 2 and candidate_tokens.issubset(professional_tokens):
        return 0.96
    if len(professional_tokens) >= 2 and professional_tokens.issubset(candidate_tokens):
        return 0.94
    overlap = len(candidate_tokens & professional_tokens) / max(len(candidate_tokens), len(professional_tokens), 1)
    ratio = difflib.SequenceMatcher(None, candidate_norm, professional_norm).ratio()
    return max(overlap, ratio)


@lru_cache
def _professionals_cached(_ttl_bucket: int) -> tuple[dict[str, Any], ...]:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("profesionales")
            .select("nombre_profesional,correo_profesional,programa")
            .execute()
        ),
        context="acta_import_pipeline.professionals",
    )
    return tuple(dict(row) for row in list(response.data or []))


@lru_cache
def _interpreters_cached(_ttl_bucket: int) -> tuple[dict[str, Any], ...]:
    response = execute_with_reauth(
        lambda retry_client: retry_client.table("interpretes").select("nombre").execute(),
        context="acta_import_pipeline.interpretes",
    )
    return tuple(dict(row) for row in list(response.data or []))


@lru_cache
def _users_cached(_ttl_bucket: int) -> tuple[dict[str, Any], ...]:
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
        context="acta_import_pipeline.usuarios_reca",
    )
    return tuple(dict(row) for row in list(response.data or []))


@lru_cache
def _companies_cached(_ttl_bucket: int) -> tuple[dict[str, Any], ...]:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("empresas")
            .select("nit_empresa,nombre_empresa,caja_compensacion,asesor,zona_empresa,sede_empresa,ciudad_empresa")
            .execute()
        ),
        context="acta_import_pipeline.empresas",
    )
    return tuple(dict(row) for row in list(response.data or []))


def clear_import_pipeline_caches() -> None:
    _professionals_cached.cache_clear()
    _interpreters_cached.cache_clear()
    _users_cached.cache_clear()
    _companies_cached.cache_clear()


def _professionals() -> tuple[dict[str, Any], ...]:
    return _professionals_cached(ttl_bucket(_CACHE_TTL_SECONDS))


def _interpreters() -> tuple[dict[str, Any], ...]:
    return _interpreters_cached(ttl_bucket(_CACHE_TTL_SECONDS))


def _users_by_cedula() -> dict[str, dict[str, Any]]:
    return {
        str(item.get("cedula_usuario") or "").strip(): dict(item)
        for item in _users_cached(ttl_bucket(_CACHE_TTL_SECONDS))
        if str(item.get("cedula_usuario") or "").strip()
    }


def _companies() -> tuple[dict[str, Any], ...]:
    return _companies_cached(ttl_bucket(_CACHE_TTL_SECONDS))


def _professional_email_map() -> dict[str, str]:
    allowed: dict[str, str] = {}
    for row in _professionals():
        email = str(row.get("correo_profesional") or "").strip().lower()
        name = str(row.get("nombre_profesional") or "").strip()
        if email and name:
            allowed[email] = name
    return allowed


def _resolve_non_interpreter_professional(analysis: dict, *, sender_match: str = "") -> str:
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

    best_name = ""
    best_score = 0.0
    for candidate in candidates:
        for row in _professionals():
            professional_name = str(row.get("nombre_profesional") or "").strip()
            score = _professional_name_matches(candidate, professional_name)
            if score > best_score:
                best_score = score
                best_name = professional_name
    return best_name if best_score >= 0.55 else ""


def _normalize_interpreter_storage_name(value: str) -> str:
    clean = " ".join(str(value or "").split())
    if not clean:
        return ""
    return " ".join(part.capitalize() for part in clean.split(" "))


def _resolve_or_create_interpreter(analysis: dict, *, create_missing: bool) -> str:
    candidates: list[str] = []
    for raw_candidate in list(analysis.get("interpretes") or []):
        candidate = str(raw_candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    if not candidates:
        candidate = str(analysis.get("nombre_profesional") or "").strip()
        if candidate:
            candidates.append(candidate)
    if not candidates:
        for raw_candidate in list(analysis.get("asistentes") or []):
            candidate = str(raw_candidate or "").strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    if not candidates:
        return ""

    best_name = ""
    best_score = 0.0
    interpreter_names = [str(item.get("nombre") or "").strip() for item in _interpreters()]
    for candidate in candidates:
        for interpreter_name in interpreter_names:
            score = _professional_name_matches(candidate, interpreter_name)
            if score > best_score:
                best_score = score
                best_name = interpreter_name
    if best_name and best_score >= 0.85:
        return best_name

    candidate = _normalize_interpreter_storage_name(candidates[0])
    if not candidate:
        return ""
    if create_missing:
        seccion1.crear_profesional(
            seccion1.CrearProfesionalRequest(
                nombre_profesional=candidate,
                programa="Interprete",
            )
        )
        clear_import_pipeline_caches()
    return candidate


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


def _prepare_participants(parsed: dict) -> tuple[list[dict], list[str], list[str]]:
    participantes_raw = list(parsed.get("participantes") or [])
    usuarios_by_cedula = _users_by_cedula()
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
                    "nombre_usuario": _normalize_import_name(
                        str(existing.get("nombre_usuario") or persona.get("nombre_usuario") or "").strip()
                    ),
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


def _resolve_company(parsed: dict) -> tuple[dict[str, Any] | None, list[str]]:
    nit = str(parsed.get("nit_empresa") or "").strip()
    name = str(parsed.get("nombre_empresa") or "").strip()
    warnings: list[str] = []

    if nit:
        company_by_nit = next(
            (dict(row) for row in _companies() if str(row.get("nit_empresa") or "").strip() == nit),
            None,
        )
        if not company_by_nit:
            return None, [f"El NIT {nit} no existe en Supabase."]
        canonical_name = str(company_by_nit.get("nombre_empresa") or "").strip()
        if name and not _company_name_matches(name, canonical_name):
            return None, [f"El NIT {nit} existe en Supabase, pero el nombre '{name}' no coincide con la empresa registrada."]
        return company_by_nit, warnings

    if name:
        best_match: dict[str, Any] | None = None
        best_score = 0.0
        for row in _companies():
            company_name = str(row.get("nombre_empresa") or "").strip()
            if not company_name:
                continue
            score = 1.0 if _company_name_matches(name, company_name) else 0.0
            if score > best_score:
                best_score = score
                best_match = dict(row)
        if best_match:
            warnings.append("NIT completado desde Supabase por coincidencia de nombre de empresa.")
            return best_match, warnings

    return None, ["No se pudo conciliar empresa/NIT con Supabase."]


def _attachment_context(*, source_label: str, message: dict | None = None, attachment: dict | None = None) -> dict:
    message_data = dict(message or {})
    attachment_data = dict(attachment or {})
    if not attachment_data:
        classification = classify_document(
            filename=source_label,
            subject=str(message_data.get("subject") or ""),
        )
        attachment_data = {
            "filename": source_label,
            "document_kind": classification.document_kind,
            "document_label": classification.document_label,
            "is_ods_candidate": classification.is_ods_candidate,
            "classification_score": classification.classification_score,
            "classification_reason": classification.classification_reason,
            "process_hint": "",
            "process_score": 0.0,
        }
    return attachment_data


def build_import_result_from_parsed(
    parsed: dict,
    *,
    source_label: str,
    message: dict | None = None,
    attachment: dict | None = None,
    create_missing_interpreter: bool = True,
) -> dict:
    analysis = dict(parsed or {})
    message_data = dict(message or {})
    attachment_data = _attachment_context(source_label=source_label, message=message_data, attachment=attachment)

    sender_email = str(message_data.get("sender_email") or "").strip().lower()
    sender_match = _professional_email_map().get(sender_email, "")
    warnings = list(analysis.get("warnings") or [])
    if sender_email and not sender_match:
        warnings.append("El remitente del correo no coincide con la tabla de profesionales.")
    elif sender_match:
        analysis["matched_professional_sender"] = sender_match

    for key in (
        "process_hint",
        "process_score",
        "document_kind",
        "document_label",
        "is_ods_candidate",
        "classification_score",
        "classification_reason",
    ):
        analysis[key] = attachment_data.get(key)

    is_interpreter = str(attachment_data.get("document_kind") or "") == "interpreter_service"
    professional_resolved = (
        _resolve_or_create_interpreter(analysis, create_missing=create_missing_interpreter)
        if is_interpreter
        else _resolve_non_interpreter_professional(analysis, sender_match=sender_match)
    )
    if professional_resolved:
        analysis["nombre_profesional"] = professional_resolved
        if is_interpreter:
            analysis["interpretes"] = [professional_resolved]
            analysis["candidatos_profesional"] = [professional_resolved]
    elif not is_interpreter:
        analysis["nombre_profesional"] = ""

    participants, discarded_ids, participant_warnings = _prepare_participants(analysis)
    analysis["participantes"] = participants
    if discarded_ids:
        analysis["_cedulas_descartadas"] = discarded_ids
    warnings.extend(participant_warnings)

    company, company_warnings = _resolve_company(analysis)
    warnings.extend(company_warnings)
    blocking_errors = list(company_warnings if company is None else [])
    if company:
        analysis["nit_empresa"] = str(company.get("nit_empresa") or "").strip()
        analysis["nombre_empresa"] = str(company.get("nombre_empresa") or "").strip()
        analysis["caja_compensacion"] = str(company.get("caja_compensacion") or "").strip()
        analysis["asesor_empresa"] = str(company.get("asesor") or "").strip()
        analysis["sede_empresa"] = str(company.get("sede_empresa") or company.get("zona_empresa") or "").strip()
        analysis["ciudad_empresa"] = str(company.get("ciudad_empresa") or "").strip()

    analysis["warnings"] = list(dict.fromkeys(warnings))
    analysis["_blocking_errors"] = list(blocking_errors)
    suggestion = suggest_service_from_analysis(analysis=analysis, message=message_data).to_dict()

    return {
        "source_label": source_label,
        "parsed_raw": dict(parsed or {}),
        "analysis": analysis,
        "attachment": attachment_data,
        "empresa_resolved": dict(company or {}),
        "professional_resolved": professional_resolved,
        "is_interpreter": is_interpreter,
        "interpreter_hours": analysis.get("sumatoria_horas_interpretes") or analysis.get("total_horas_interprete") or "",
        "participants_prepared": participants,
        "service_suggestion": suggestion,
        "blocking_errors": blocking_errors,
        "warnings": list(analysis.get("warnings") or []),
    }


def build_import_result_from_source(
    source: str,
    *,
    source_label: str,
    message: dict | None = None,
    attachment: dict | None = None,
    create_missing_interpreter: bool = True,
) -> dict:
    parsed = parse_acta_source(source)
    return build_import_result_from_parsed(
        parsed,
        source_label=source_label,
        message=message,
        attachment=attachment,
        create_missing_interpreter=create_missing_interpreter,
    )
