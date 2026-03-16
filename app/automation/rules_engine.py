from __future__ import annotations

from functools import lru_cache
import re
from typing import Any

from app.automation.models import DecisionSuggestion
from app.config import get_settings
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket
from app.utils.text import normalize_text

_CACHE_TTL_SECONDS = 300
_NIT_RE = re.compile(r"\b\d{6,12}(?:-\d)?\b")


@lru_cache
def _get_tarifas_cached(_ttl_bucket: int) -> tuple[dict[str, Any], ...]:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("tarifas")
            .select("codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base")
            .execute()
        ),
        context="automation.rules.tarifas",
    )
    return tuple(dict(row) for row in list(response.data or []))


def _tarifas() -> tuple[dict[str, Any], ...]:
    return _get_tarifas_cached(ttl_bucket(_CACHE_TTL_SECONDS))


@lru_cache
def _get_company_by_nit_cached(nit: str, _ttl_bucket: int) -> dict[str, Any] | None:
    response = execute_with_reauth(
        lambda retry_client: (
            retry_client.table("empresas")
            .select(
                "nombre_empresa,nit_empresa,ciudad_empresa,sede_empresa,zona_empresa,"
                "caja_compensacion,correo_profesional,profesional_asignado,asesor"
            )
            .eq("nit_empresa", nit)
            .limit(1)
            .execute()
        ),
        context="automation.rules.company_by_nit",
    )
    rows = list(response.data or [])
    if not rows:
        return None
    return dict(rows[0])


def _company_by_nit(nit: str) -> dict[str, Any] | None:
    nit_clean = str(nit or "").strip()
    if not nit_clean:
        return None
    return _get_company_by_nit_cached(nit_clean, ttl_bucket(_CACHE_TTL_SECONDS))


def _normalized_modalidad(value: str) -> str:
    text = normalize_text(value or "")
    if "virtual" in text:
        return "Virtual"
    if "bogota" in text:
        return "Bogotá"
    if "fuera" in text or "otro" in text:
        return "Fuera de Bogotá"
    return ""


def _infer_modalidad(*, analysis: dict[str, Any], message: dict[str, Any], company: dict[str, Any] | None) -> tuple[str, str]:
    parsed_modalidad = _normalized_modalidad(str(analysis.get("modalidad_servicio") or ""))
    if parsed_modalidad:
        return parsed_modalidad, "Modalidad detectada directamente en el PDF."

    subject = normalize_text(message.get("subject") or "")
    if "virtual" in subject:
        return "Virtual", "Modalidad inferida desde el asunto del correo."

    document_kind = str(analysis.get("document_kind") or "")
    if document_kind in {"accessibility_assessment", "vacancy_review", "program_presentation", "program_reactivation", "sensibilizacion", "inclusive_selection", "inclusive_hiring", "organizational_induction", "operational_induction", "follow_up"}:
        city = normalize_text((company or {}).get("ciudad_empresa") or "")
        if city:
            if "bogota" in city:
                return "Bogotá", "Modalidad inferida desde la ciudad registrada de la empresa."
            return "Fuera de Bogotá", "Modalidad inferida desde la ciudad registrada de la empresa."

    return "", "No fue posible inferir modalidad con suficiente confianza."


def _is_compensar_company(company: dict[str, Any] | None) -> bool:
    caja = normalize_text((company or {}).get("caja_compensacion") or "")
    return "compensar" in caja


def _first_non_empty(analysis: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(analysis.get(key) or "").strip()
        if value:
            return value
    return ""


def _management_family(analysis: dict[str, Any]) -> tuple[str, str, bool]:
    raw_value = _first_non_empty(
        analysis,
        "gestion_servicio",
        "gestion_empresarial",
        "tipo_gestion",
        "gestion",
    )
    normalized = normalize_text(raw_value)
    if "compensar" in normalized:
        return "compensar", "Gestion detectada en el acta/templete: COMPENSAR.", False
    if "reca" in normalized:
        return "reca", "Gestion detectada en el acta/templete: RECA.", False
    return "reca", "No se detecto gestion en el acta; se aplica valor por defecto RECA.", True


def _company_size_bucket(analysis: dict[str, Any]) -> tuple[str, str, bool]:
    raw_value = _first_non_empty(
        analysis,
        "tamano_empresa",
        "tamano_empresa_servicio",
        "size_bucket",
    )
    normalized = normalize_text(raw_value)
    if any(token in normalized for token in ("hasta 50", "menos de 50", "<50", "micro", "pequena")):
        return "hasta_50", "Tamano de empresa detectado: hasta 50 trabajadores.", False
    if any(token in normalized for token in ("desde 51", "51", ">50", "mas de 50", "grande")):
        return "desde_51", "Tamano de empresa detectado: desde 51 trabajadores.", False

    workers_raw = _first_non_empty(
        analysis,
        "cantidad_trabajadores",
        "numero_trabajadores",
        "total_trabajadores",
    )
    workers_match = re.match(r"\d+", workers_raw.strip())
    workers_digits = workers_match.group() if workers_match else ""
    if workers_digits:
        workers = int(workers_digits)
        if workers <= 50:
            return "hasta_50", f"Tamano de empresa inferido desde total de trabajadores: {workers}.", False
        return "desde_51", f"Tamano de empresa inferido desde total de trabajadores: {workers}.", False

    return "hasta_50", "No se detecto tamano de empresa; se aplica valor por defecto hasta 50 trabajadores.", True


def _extract_company_nits(analysis: dict[str, Any]) -> list[str]:
    raw_sources: list[Any] = [
        analysis.get("nits_empresas"),
        analysis.get("nit_empresas"),
        analysis.get("nits_detectados"),
        analysis.get("multi_nits"),
        analysis.get("nit_empresa"),
    ]
    collected: list[str] = []
    seen: set[str] = set()
    for source in raw_sources:
        if not source:
            continue
        values = source if isinstance(source, (list, tuple, set)) else [source]
        for value in values:
            for nit in _NIT_RE.findall(str(value or "")):
                if nit in seen:
                    continue
                seen.add(nit)
                collected.append(nit)
    return collected


def _promotion_company_count(analysis: dict[str, Any]) -> tuple[int, str, bool]:
    explicit_count = _first_non_empty(
        analysis,
        "cantidad_empresas",
        "numero_empresas",
        "company_count",
    )
    explicit_digits = re.sub(r"\D", "", explicit_count)
    if explicit_digits:
        count = max(1, int(explicit_digits))
        return count, f"Cantidad de empresas detectada en el acta/templete: {count}.", False

    nit_count = len(_extract_company_nits(analysis))
    if nit_count > 1:
        return nit_count, f"Cantidad de empresas inferida desde {nit_count} NIT(s) detectados.", False

    return 1, "No se detecto cantidad de empresas; se aplica valor por defecto 1 empresa.", True


def _promotion_bucket_token(count: int) -> tuple[str, str]:
    if count <= 1:
        return "individual", "Promocion clasificada como individual."
    if 2 <= count <= 3:
        return "2-3 empresas", "Promocion clasificada en rango de 2 a 3 empresas."
    if 4 <= count <= 5:
        return "4-5 empresas", "Promocion clasificada en rango de 4 a 5 empresas."
    if 6 <= count <= 10:
        return "6-10 empresas", "Promocion clasificada en rango de 6 a 10 empresas."
    if 11 <= count <= 15:
        return "11-15 empresas", "Promocion clasificada en rango de 11 a 15 empresas."
    return "mas de 15 empresas", "Promocion clasificada en rango de mas de 15 empresas."


def _select_tarifa(predicate) -> dict[str, Any] | None:
    for row in _tarifas():
        if predicate(row):
            return row
    return None


def _selection_size_bucket(participants: list[dict[str, Any]]) -> tuple[str, str]:
    count = len(participants)
    if count <= 1:
        return "individual", "Cantidad de oferentes detectada: 1."
    if 2 <= count <= 4:
        return "2-4", "Cantidad de oferentes detectada entre 2 y 4."
    if 5 <= count <= 7:
        return "5-7", "Cantidad de oferentes detectada entre 5 y 7."
    return "8+", "Cantidad de oferentes detectada mayor o igual a 8."


def _clean_observation_text(value: str) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text).strip()


def _extract_vacancy_count(analysis: dict[str, Any]) -> int:
    for key in ("total_vacantes", "cantidad_vacantes", "numero_vacantes", "vacantes"):
        raw = str(analysis.get(key) or "").strip()
        if not raw:
            continue
        digits = re.sub(r"\D", "", raw)
        if digits:
            return int(digits)
    process_text = _clean_observation_text(
        " ".join(
            [
                str(analysis.get("cargo_objetivo") or ""),
                str(analysis.get("process_name_hint") or ""),
                str(analysis.get("file_path") or ""),
            ]
        )
    )
    match = re.search(r"\((\d+)\)", process_text)
    if match:
        return int(match.group(1))
    return 0


def _extract_cargo_objetivo(analysis: dict[str, Any]) -> str:
    for key in ("cargo_objetivo", "cargo_servicio", "cargo", "nombre_cargo"):
        value = _clean_observation_text(str(analysis.get(key) or ""))
        if value:
            return re.sub(r"\s*\(\d+\)\s*$", "", value).strip()
    process_name = _clean_observation_text(str(analysis.get("process_name_hint") or ""))
    if process_name:
        process_name = re.sub(r"\s*\(\d+\)\s*$", "", process_name).strip()
        cleaned = process_name
        cleaned = re.sub(r"(?i)^proceso de seleccion incluyente(?: individual)?", "", cleaned).strip(" -:")
        cleaned = re.sub(r"(?i)^proceso de contratacion incluyente(?: individual)?", "", cleaned).strip(" -:")
        cleaned = re.sub(r"(?i)^revision de las condiciones de la vacante", "", cleaned).strip(" -:")
        cleaned = re.sub(r"(?i)^revision condicion de vacante", "", cleaned).strip(" -:")
        if cleaned and cleaned != process_name:
            return cleaned
    return ""


def _extract_follow_up_number(analysis: dict[str, Any]) -> str:
    for key in ("numero_seguimiento", "seguimiento_numero", "seguimiento_servicio"):
        raw = _clean_observation_text(str(analysis.get(key) or ""))
        if raw:
            digits = re.sub(r"\D", "", raw)
            return digits or raw
    combined = " ".join(
        [
            str(analysis.get("process_name_hint") or ""),
            str(analysis.get("file_path") or ""),
        ]
    )
    match = re.search(r"(?i)seguimiento\s*(?:no\.?|numero|nro\.?|#)?\s*(\d+)", combined)
    if match:
        return match.group(1)
    return ""


def _build_document_observaciones(*, analysis: dict[str, Any], document_kind: str) -> str:
    if document_kind in {"vacancy_review", "inclusive_selection", "inclusive_hiring"}:
        cargo = _extract_cargo_objetivo(analysis)
        vacantes = _extract_vacancy_count(analysis)
        if cargo and vacantes > 0:
            return f"{cargo} ({vacantes})"
        if cargo:
            return cargo
        return ""
    if document_kind == "follow_up":
        return _extract_follow_up_number(analysis)
    return ""


def _selection_bucket_token(bucket: str) -> str:
    if bucket == "individual":
        return "individual"
    if bucket == "2-4":
        return "2 a 4"
    if bucket == "5-7":
        return "5 a 7"
    return "8 oferentes"


def _analysis_signal_text(*, analysis: dict[str, Any], message: dict[str, Any]) -> str:
    return normalize_text(
        " ".join(
            [
                str(message.get("subject") or ""),
                str(analysis.get("file_path") or ""),
                str(analysis.get("process_hint") or ""),
                str(analysis.get("document_label") or ""),
                str(analysis.get("interpreter_total_time_raw") or ""),
                str(analysis.get("sumatoria_horas_interpretes_raw") or ""),
            ]
        )
    )


def _interpreter_tarifa_from_hours(hours_value: Any) -> tuple[dict[str, Any] | None, str]:
    try:
        hours = float(hours_value)
    except (TypeError, ValueError):
        return None, ""

    if hours <= 0:
        return None, ""
    if hours >= 1:
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "hora" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, f"Se detecto servicio de interprete por {hours:g} hora(s)."
    if abs(hours - 0.75) <= 0.02:
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "45" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 45 minutos para el servicio de interprete."
    if abs(hours - 0.5) <= 0.02:
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "30" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 30 minutos para el servicio de interprete."
    if abs(hours - 0.25) <= 0.02:
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "15" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 15 minutos para el servicio de interprete."
    return None, ""


def _interpreter_tarifa_from_text(signal_text: str) -> tuple[dict[str, Any] | None, str]:
    if "visita fallida" in signal_text:
        row = _select_tarifa(lambda item: "visita fallida" in normalize_text(item.get("descripcion_servicio")))
        return row, "Se detecto visita fallida en el servicio de interprete."
    if any(token in signal_text for token in ("15 min", "15 mn", "15 minuto")):
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "15" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 15 minutos para el servicio de interprete."
    if any(token in signal_text for token in ("30 min", "30 minuto")):
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "30" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 30 minutos para el servicio de interprete."
    if any(token in signal_text for token in ("45 min", "45 minuto")):
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "45" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto duracion de 45 minutos para el servicio de interprete."
    if any(token in signal_text for token in ("1 hora", "60 min", "60 minuto", "por hora")):
        row = _select_tarifa(
            lambda item: "interprete" in normalize_text(item.get("descripcion_servicio"))
            and "hora" in normalize_text(item.get("descripcion_servicio"))
        )
        return row, "Se detecto una hora de servicio para interprete."
    return None, ""


def suggest_service_from_analysis(*, analysis: dict[str, Any], message: dict[str, Any]) -> DecisionSuggestion:
    rationale: list[str] = []
    nit = str(analysis.get("nit_empresa") or "").strip()
    company = _company_by_nit(nit)
    if company:
        rationale.append(f"Empresa encontrada en BD: {company.get('nombre_empresa')}.")
        rationale.append(
            "Caja de compensacion: "
            + (str(company.get("caja_compensacion") or "Sin dato"))
            + "."
        )
    else:
        rationale.append("Empresa no encontrada en BD por NIT; algunas reglas pueden quedar incompletas.")

    modalidad, modalidad_reason = _infer_modalidad(analysis=analysis, message=message, company=company)
    if modalidad:
        rationale.append(modalidad_reason)
    else:
        rationale.append(modalidad_reason)

    document_kind = str(analysis.get("document_kind") or "")
    process_hint = str(analysis.get("process_hint") or "")
    participants = list(analysis.get("participantes") or [])
    signal_text = _analysis_signal_text(analysis=analysis, message=message)

    suggestion = DecisionSuggestion(
        observaciones="",
        observacion_agencia="",
        seguimiento_servicio="",
        confidence="low",
        rationale=tuple(rationale),
    )

    def finalize(row: dict[str, Any], *, confidence: str, extra_rationale: list[str], observaciones: str = "") -> DecisionSuggestion:
        auto_observaciones = _build_document_observaciones(analysis=analysis, document_kind=document_kind)
        final_observaciones = observaciones or auto_observaciones
        return DecisionSuggestion(
            codigo_servicio=str(row.get("codigo_servicio") or ""),
            referencia_servicio=str(row.get("referencia_servicio") or ""),
            descripcion_servicio=str(row.get("descripcion_servicio") or ""),
            modalidad_servicio=str(row.get("modalidad_servicio") or modalidad or ""),
            valor_base=float(row.get("valor_base") or 0),
            observaciones=final_observaciones,
            observacion_agencia="",
            seguimiento_servicio="",
            confidence=confidence,
            rationale=tuple([*rationale, *extra_rationale]),
        )

    if document_kind == "attendance_support":
        return DecisionSuggestion(
            confidence="low",
            observaciones="Adjunto de soporte. No usar como acta principal ODS.",
            rationale=tuple([*rationale, "El documento fue clasificado como control de asistencia."]),
        )

    if document_kind == "interpreter_service":
        if bool(analysis.get("is_fallido")) or "fallido" in signal_text:
            row = _select_tarifa(lambda item: "visita fallida" in normalize_text(item.get("descripcion_servicio")))
            if row:
                return finalize(
                    row,
                    confidence="medium",
                    extra_rationale=[
                        "Se detecto documento de interprete LSC.",
                        "Se detecto visita fallida en el asunto o en el acta.",
                    ],
                )
        row, interpreter_reason = _interpreter_tarifa_from_hours(
            analysis.get("sumatoria_horas_interpretes") or analysis.get("total_horas_interprete")
        )
        if row:
            return finalize(
                row,
                confidence="medium",
                extra_rationale=["Se detecto documento de interprete LSC.", interpreter_reason],
            )
        row, interpreter_reason = _interpreter_tarifa_from_text(signal_text)
        if row:
            return finalize(
                row,
                confidence="medium",
                extra_rationale=["Se detecto documento de interprete LSC.", interpreter_reason],
            )
        return DecisionSuggestion(
            confidence="low",
            observaciones="Servicio interprete detectado. Falta duracion exacta para definir codigo 86/87/88/89 o visita fallida 90.",
            rationale=tuple([*rationale, "Se detecto documento de interprete LSC."]),
        )

    if document_kind == "vacancy_review" and modalidad:
        row = _select_tarifa(
            lambda item: "vacante" in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="high" if modalidad == "Virtual" else "medium",
                extra_rationale=["Se asigno familia de codigo de revision de vacante."],
            )

    if document_kind == "sensibilizacion" and modalidad:
        row = _select_tarifa(
            lambda item: "sensibilizacion" in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="high" if modalidad == "Virtual" else "medium",
                extra_rationale=["Se asigno familia de codigo de sensibilizacion."],
            )

    if document_kind in {"organizational_induction", "operational_induction"} and modalidad:
        keyword = "organizacional" if document_kind == "organizational_induction" else "operativa"
        row = _select_tarifa(
            lambda item: keyword in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="high" if modalidad == "Virtual" else "medium",
                extra_rationale=[f"Se asigno familia de codigo de induccion {keyword}."],
            )

    if document_kind == "inclusive_selection" and modalidad:
        bucket, bucket_reason = _selection_size_bucket(participants)
        extra = [bucket_reason, "Se asigno familia de codigo de seleccion incluyente."]
        token = _selection_bucket_token(bucket)
        row = _select_tarifa(
            lambda item: "seleccion incluyente" in normalize_text(item.get("descripcion_servicio"))
            and token in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="medium" if participants else "low",
                extra_rationale=extra,
            )

    if document_kind == "inclusive_hiring" and modalidad:
        bucket, bucket_reason = _selection_size_bucket(participants)
        token = _selection_bucket_token(bucket)
        row = _select_tarifa(
            lambda item: "contratacion incluyente" in normalize_text(item.get("descripcion_servicio"))
            and token in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="medium" if participants else "low",
                extra_rationale=[bucket_reason, "Se asigno familia de codigo de contratacion incluyente."],
            )

    if document_kind == "program_reactivation" and modalidad:
        family, family_reason, family_is_default = _management_family(analysis)
        row = _select_tarifa(
            lambda item: "reactivacion" in normalize_text(item.get("descripcion_servicio"))
            and family in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="low" if family_is_default else "medium",
                extra_rationale=[
                    family_reason,
                    "Se asigno familia de codigo de mantenimiento/reactivacion.",
                ],
                observaciones=(
                    "Gestion asumida por defecto como RECA al no venir en el templete."
                    if family_is_default
                    else ""
                ),
            )

    if document_kind == "program_presentation" and modalidad:
        family, family_reason, family_is_default = _management_family(analysis)
        company_count, count_reason, count_is_default = _promotion_company_count(analysis)
        bucket_token, bucket_reason = _promotion_bucket_token(company_count)
        row = _select_tarifa(
            lambda item: "promocion" in normalize_text(item.get("descripcion_servicio"))
            and bucket_token in normalize_text(item.get("descripcion_servicio"))
            and family in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            observaciones_parts: list[str] = []
            if family_is_default:
                observaciones_parts.append("Gestion asumida por defecto como RECA.")
            if count_is_default:
                observaciones_parts.append("Cantidad de empresas asumida por defecto como 1.")
            return finalize(
                row,
                confidence="low" if (family_is_default or count_is_default) else "medium",
                extra_rationale=[
                    family_reason,
                    count_reason,
                    bucket_reason,
                    "Se asigno familia de codigo de promocion del programa.",
                ],
                observaciones=" ".join(observaciones_parts),
            )

    if document_kind == "follow_up" and modalidad:
        is_special_follow_up = any(token in signal_text for token in ("visita adicional", "casos especiales", "apoyo"))
        description_token = "visita adicional" if is_special_follow_up else "seguimiento y acompanamiento"
        row = _select_tarifa(
            lambda item: description_token in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="medium",
                extra_rationale=[
                    "Se asigno familia de codigo de seguimiento."
                    if not is_special_follow_up
                    else "Se asigno familia de visita adicional de seguimiento/apoyo.",
                ],
            )

    if document_kind == "accessibility_assessment":
        size_bucket, size_reason, size_is_default = _company_size_bucket(analysis)
        if modalidad:
            row = _select_tarifa(
                lambda item: "accesibilidad" in normalize_text(item.get("descripcion_servicio"))
                and (
                    "hasta 50" in normalize_text(item.get("descripcion_servicio"))
                    if size_bucket == "hasta_50"
                    else "desde 51" in normalize_text(item.get("descripcion_servicio"))
                )
                and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
            )
            if row:
                return finalize(
                    row,
                    confidence="low" if size_is_default else "medium",
                    extra_rationale=[
                        size_reason,
                        "Se asigno familia de codigo de evaluacion de accesibilidad.",
                    ],
                    observaciones=(
                        "Tamano de empresa asumido por defecto como hasta 50 trabajadores."
                        if size_is_default
                        else ""
                    ),
                )

        rationale.extend(
            [
                "La familia de codigo es evaluacion de accesibilidad.",
                size_reason,
            ]
        )
        return DecisionSuggestion(
            modalidad_servicio=modalidad,
            observaciones=(
                "No fue posible asignar codigo de accesibilidad con la modalidad detectada."
                if modalidad
                else "Falta modalidad para escoger codigo exacto de accesibilidad."
            ),
            confidence="low",
            rationale=tuple(rationale),
        )

    if process_hint:
        rationale.append(f"Proceso sugerido por nombre de archivo: {process_hint}.")

    return DecisionSuggestion(
        modalidad_servicio=modalidad,
        observaciones="No hubo suficientes señales para proponer un codigo_servicio confiable.",
        confidence="low",
        rationale=tuple(rationale),
    )
