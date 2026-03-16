from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from app.utils.cache import ttl_bucket
from app.utils.text import normalize_search_text

_PROFILES_CACHE_TTL_SECONDS = 300

_PROFILES_PATH = Path(__file__).with_name("process_profiles.json")
_PROFILE_ALIASES: dict[str, str] = {}

_DETAILED_INSTRUCTION_OVERRIDES: dict[str, dict[str, Any]] = {
    "vacancy_review": {
        "description": "Revision de condiciones de la vacante.",
        "extract_sections": [
            "1. DATOS GENERALES",
            "2. CARACTERISTICAS DE LA VACANTE",
            "8. ASISTENTES",
        ],
        "ignore_sections": [
            "3. HABILIDADES Y CAPACIDADES REQUERIDAS PARA EL CARGO",
            "4. POSTURAS Y MOVIMIENTOS",
            "5. PELIGROS Y RIESGOS EN EL DESARROLLO DE LA LABOR",
            "6. LA VACANTE ES ACCESIBLE Y COMPATIBLE...",
            "7. OBSERVACIONES / RECOMENDACIONES",
        ],
        "field_rules": [
            "fecha_servicio sale de 'Fecha de la Visita' en DATOS GENERALES",
            "modalidad_servicio sale siempre de 'Modalidad' en DATOS GENERALES",
            "nombre_empresa sale de 'Nombre de la Empresa'",
            "nit_empresa sale de 'Numero de NIT'",
            "nombre_profesional sale de ASISTENTES, no del asesor ni de 'Profesional asignado RECA'",
            "cargo_objetivo sale solo de 'Nombre de la vacante'",
            "total_vacantes sale solo de 'Numero de vacantes'",
        ],
        "hard_rules": [
            "Ignora modalidad o cargo que aparezcan en otras secciones si contradicen DATOS GENERALES o Nombre de la vacante",
            "numero_seguimiento debe ir vacio en este formato",
            "total_empresas debe ir vacio o 0 en este formato",
            "cargo_objetivo nunca sale de asistentes",
        ],
    },
    "inclusive_selection": {
        "description": "Proceso de seleccion incluyente.",
        "extract_sections": [
            "1. DATOS DE LA EMPRESA",
            "2. DATOS DEL OFERENTE",
            "6. ASISTENTES",
        ],
        "ignore_sections": [
            "3. DESARROLLO DE LA ACTIVIDAD",
            "4. CARACTERIZACION DEL OFERENTE",
            "4.1 Condiciones medicas y de salud",
            "4.2 Habilidades basicas de la vida diaria",
            "5. AJUSTES RAZONABLES / RECOMENDACIONES AL PROCESO DE SELECCION",
        ],
        "field_rules": [
            "fecha_servicio sale de 'Fecha de la Visita'",
            "modalidad_servicio sale siempre de DATOS DE LA EMPRESA o DATOS GENERALES",
            "participantes salen de la tabla de DATOS DEL OFERENTE",
            "cargo_objetivo sale solo del campo rotulado 'Cargo' dentro de DATOS DEL OFERENTE, no del Cargo del contacto de empresa",
            "en seleccion incluyente, el cargo objetivo suele aparecer muy cerca de Nombre oferente y Cedula, en la fila inferior de la misma tabla",
            "nombre_profesional sale de asistentes, pero cargo_objetivo nunca sale de asistentes",
        ],
        "hard_rules": [
            "numero_seguimiento debe ir vacio",
            "ignora cualquier 'Cargo' que aparezca en DATOS DE LA EMPRESA o junto al contacto de la empresa",
            "si el cargo aparece solo como texto libre fuera de una etiqueta valida, dejar cargo_objetivo vacio",
        ],
    },
    "inclusive_hiring": {
        "description": "Proceso de contratacion incluyente.",
        "extract_sections": [
            "1. DATOS DE LA EMPRESA",
            "2. DATOS DEL VINCULADO",
            "3. DATOS ADICIONALES",
            "7. ASISTENTES",
        ],
        "ignore_sections": [
            "4. DESARROLLO DE LA ACTIVIDAD",
            "5. ACOMPANAMIENTO AL PROCESO",
            "5.1 Condiciones de la vacante",
            "5.2 Prestaciones de ley",
            "5.3 Deberes y derechos del trabajador",
            "5. AJUSTES RAZONABLES / RECOMENDACIONES",
        ],
        "field_rules": [
            "modalidad_servicio sale de DATOS DE LA EMPRESA o DATOS GENERALES",
            "participantes salen de DATOS DEL VINCULADO",
            "cargo_objetivo sale solo del campo 'Cargo'",
            "tipo de contrato sale de DATOS ADICIONALES",
        ],
        "hard_rules": [
            "numero_seguimiento debe ir vacio",
            "cargo_objetivo nunca sale de asistentes",
        ],
    },
    "follow_up": {
        "description": "Seguimiento al proceso IL.",
        "extract_sections": [
            "1. DATOS DE LA EMPRESA",
            "2. DATOS DEL VINCULADO",
            "3. DATOS DEL CARGO OCUPADO POR EL VINCULADO",
            "5. FECHAS DE SEGUIMIENTO Y ACOMPANAMIENTO",
        ],
        "ignore_sections": [
            "4. FUNCIONES DEL VINCULADO",
        ],
        "field_rules": [
            "modalidad_servicio sale de DATOS DE LA EMPRESA o DATOS GENERALES",
            "numero_seguimiento solo aplica si el documento trae un seguimiento identificado",
            "cargo_objetivo sale de 'Cargo que ocupa'",
        ],
        "hard_rules": [
            "si no hay numero de seguimiento claro, dejar numero_seguimiento vacio y marcar needs_review",
        ],
    },
    "interpreter_service": {
        "description": "Servicio interprete LSC.",
        "extract_sections": [
            "DATOS GENERALES",
            "DATOS DEL INTERPRETE",
            "DATOS DEL OFERENTE",
            "ASISTENTES",
        ],
        "ignore_sections": [
            "DESARROLLO DE LA ACTIVIDAD",
            "OBSERVACIONES / RECOMENDACIONES",
        ],
        "field_rules": [
            "nombre_profesional sale de 'Nombre interprete'",
            "si hay varios interpretes, devuelvelos todos en 'interpretes'",
            "prioriza 'SUMATORIA HORAS INTERPRETES'; si no existe usa 'Total Tiempo'",
            "nombre_empresa sale de 'Nombre de la empresa' en DATOS GENERALES",
            "fecha_servicio sale de 'Fecha de la visita' en DATOS GENERALES",
            "participantes salen de DATOS DEL OFERENTE usando Nombre oferente y Cedula",
            "modalidad_servicio sale de DATOS GENERALES",
        ],
        "hard_rules": [
            "cargo_objetivo debe ir vacio",
            "no tomes nombre_interprete, nombre_oferente ni cedula desde ASISTENTES si el documento trae secciones especificas",
            "si falta nit_empresa, usa solo el nombre de empresa; la conciliacion final se hace afuera",
            "si falta Nombre interprete y no existe un campo explicito, solo entonces usa asistentes como fallback",
        ],
    },
}


def _normalize_profile(document: dict[str, Any]) -> dict[str, Any]:
    profile = dict(document)
    aliases = dict(profile.get("section_aliases") or {})
    profile["section_aliases"] = aliases
    profile["normalized_section_aliases"] = {
        key: normalize_search_text(value) for key, value in aliases.items() if str(value or "").strip()
    }
    profile["keep_sections"] = list(profile.get("keep_sections") or [])
    profile["ignore_sections"] = list(profile.get("ignore_sections") or [])
    profile["required_fields"] = list(profile.get("required_fields") or [])
    profile["field_sources"] = list(profile.get("field_sources") or [])
    profile["field_priority"] = dict(profile.get("field_priority") or {})
    profile["forbid_fields"] = list(profile.get("forbid_fields") or [])
    profile["line_mode"] = str(profile.get("line_mode") or "").strip()
    return profile


@lru_cache(maxsize=None)
def _load_profiles_cached(_ttl_bucket: int) -> dict[str, dict[str, Any]]:
    if not _PROFILES_PATH.exists():
        return {}
    payload = json.loads(_PROFILES_PATH.read_text(encoding="utf-8"))
    documents = list(payload.get("documents") or [])
    profiles: dict[str, dict[str, Any]] = {}
    for document in documents:
        if not isinstance(document, dict):
            continue
        document_kind = str(document.get("document_kind") or "").strip()
        if not document_kind:
            continue
        profiles[document_kind] = _normalize_profile(document)
    return profiles


def _load_profiles() -> dict[str, dict[str, Any]]:
    return _load_profiles_cached(ttl_bucket(_PROFILES_CACHE_TTL_SECONDS))


def get_process_profile(document_kind: str) -> dict[str, Any] | None:
    kind = str(document_kind or "").strip()
    if not kind:
        return None
    profiles = _load_profiles()
    if kind in profiles:
        return profiles[kind]
    alias = _PROFILE_ALIASES.get(kind)
    if alias:
        return profiles.get(alias)
    return None


def get_profile_priority_labels(document_kind: str) -> list[str]:
    profile = get_process_profile(document_kind)
    if not profile:
        return []
    labels: list[str] = []
    for values in dict(profile.get("field_priority") or {}).values():
        for value in list(values or []):
            text = str(value or "").strip()
            if text and text not in labels:
                labels.append(text)
    for field in list(profile.get("required_fields") or []):
        if not isinstance(field, dict):
            continue
        label = str(field.get("label") or "").strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def build_profile_prompt_context(document_kind: str) -> str:
    profile = get_process_profile(document_kind)
    if not profile:
        return ""

    keep_sections = [str(item or "").strip() for item in list(profile.get("keep_sections") or []) if str(item or "").strip()]
    ignore_sections = [str(item or "").strip() for item in list(profile.get("ignore_sections") or []) if str(item or "").strip()]
    aliases = dict(profile.get("section_aliases") or {})
    field_priority = dict(profile.get("field_priority") or {})
    forbid_fields = [str(item or "").strip() for item in list(profile.get("forbid_fields") or []) if str(item or "").strip()]

    lines: list[str] = [f"document_kind_profile: {document_kind}"]
    if keep_sections:
        rendered = [f"{section}={aliases.get(section, section)}" for section in keep_sections]
        lines.append("usar_solo_secciones: " + "; ".join(rendered))
    if ignore_sections:
        rendered = [f"{section}={aliases.get(section, section)}" for section in ignore_sections]
        lines.append("ignorar_secciones: " + "; ".join(rendered))
    if field_priority:
        priority_parts: list[str] = []
        for field_name, labels in field_priority.items():
            clean_labels = [str(label or "").strip() for label in list(labels or []) if str(label or "").strip()]
            if clean_labels:
                priority_parts.append(f"{field_name}=>{' > '.join(clean_labels)}")
        if priority_parts:
            lines.append("prioridades_campos: " + "; ".join(priority_parts))
    if forbid_fields:
        lines.append("campos_que_deben_ir_vacios: " + ", ".join(forbid_fields))
    return "\n".join(lines)


def build_detailed_extraction_instructions(document_kind: str) -> str:
    profile = get_process_profile(document_kind)
    override = _DETAILED_INSTRUCTION_OVERRIDES.get(document_kind) or {}
    if not profile and not override:
        return ""

    aliases = dict(profile.get("section_aliases") or {}) if profile else {}
    keep_sections = [str(item or "").strip() for item in list(profile.get("keep_sections") or []) if str(item or "").strip()] if profile else []
    ignore_sections = [str(item or "").strip() for item in list(profile.get("ignore_sections") or []) if str(item or "").strip()] if profile else []
    required_fields = list(profile.get("required_fields") or []) if profile else []
    field_sources = list(profile.get("field_sources") or []) if profile else []
    field_priority = dict(profile.get("field_priority") or {}) if profile else {}

    lines: list[str] = ["guia_extraccion_especifica:"]
    description = str(override.get("description") or "").strip()
    if description:
        lines.append(f"- tipo: {description}")

    extract_sections = list(override.get("extract_sections") or [])
    if not extract_sections and keep_sections:
        extract_sections = [aliases.get(section, section) for section in keep_sections]
    if extract_sections:
        lines.append("- extraer_solo_de:")
        for section in extract_sections:
            lines.append(f"  * {section}")

    ignored = list(override.get("ignore_sections") or [])
    if not ignored and ignore_sections:
        ignored = [aliases.get(section, section) for section in ignore_sections]
    if ignored:
        lines.append("- ignorar:")
        for section in ignored:
            lines.append(f"  * {section}")

    field_rules = list(override.get("field_rules") or [])
    if field_rules:
        lines.append("- reglas_campos:")
        for rule in field_rules:
            lines.append(f"  * {rule}")

    hard_rules = list(override.get("hard_rules") or [])
    if hard_rules:
        lines.append("- reglas_duras:")
        for rule in hard_rules:
            lines.append(f"  * {rule}")

    if required_fields:
        lines.append("- etiquetas_clave:")
        for field in required_fields[:12]:
            if not isinstance(field, dict):
                continue
            label = str(field.get("label") or "").strip()
            section = str(field.get("section") or "").strip()
            if label:
                section_name = aliases.get(section, section) if section else ""
                if section_name:
                    lines.append(f"  * {label} [{section_name}]")
                else:
                    lines.append(f"  * {label}")

    if field_priority:
        lines.append("- mapa_campos_prioritarios:")
        for field_name, labels in field_priority.items():
            clean_labels = [str(label or "").strip() for label in list(labels or []) if str(label or "").strip()]
            if clean_labels:
                lines.append(f"  * {field_name}: " + " > ".join(clean_labels))

    if field_sources:
        lines.append("- campos_de_apoyo_o_heredados:")
        for field in field_sources[:10]:
            if not isinstance(field, dict):
                continue
            field_name = str(field.get("field_key") or "").strip()
            label = str(field.get("label") or "").strip()
            section = str(field.get("section") or "").strip()
            source_type = str(field.get("source_type") or "").strip()
            section_name = aliases.get(section, section) if section else ""
            pieces = [piece for piece in [field_name, label, section_name, source_type] if piece]
            if pieces:
                lines.append("  * " + " | ".join(pieces))

    lines.append("- regla_global: modalidad_servicio siempre sale de DATOS GENERALES o DATOS DE LA EMPRESA, no de secciones posteriores")
    lines.append("- regla_global: cargo_objetivo nunca puede salir de asistentes")
    lines.append("- regla_global: cargo_objetivo solo es valido si viene junto a una etiqueta explicita como Cargo, Nombre de la vacante o Cargo que ocupa")
    if document_kind != "interpreter_service":
        lines.append("- regla_global: nombre_profesional siempre sale de la seccion ASISTENTES; no usar asesor ni profesional asignado RECA")
    lines.append("- regla_global: usa el PDF como fuente primaria; ignora cualquier OCR local faltante o desordenado")
    return "\n".join(lines)
