from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.automation.models import DecisionSuggestion
from app.config import get_settings
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket
from app.utils.text import normalize_text

_CACHE_TTL_SECONDS = 300


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
            .select("nombre_empresa,nit_empresa,ciudad_empresa,caja_compensacion,correo_profesional,profesional_asignado")
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
    if document_kind in {"accessibility_assessment", "vacancy_review", "program_presentation", "program_reactivation", "sensibilizacion", "inclusive_selection", "organizational_induction", "operational_induction"}:
        city = normalize_text((company or {}).get("ciudad_empresa") or "")
        if city:
            if "bogota" in city:
                return "Bogotá", "Modalidad inferida desde la ciudad registrada de la empresa."
            return "Fuera de Bogotá", "Modalidad inferida desde la ciudad registrada de la empresa."

    return "", "No fue posible inferir modalidad con suficiente confianza."


def _is_compensar_company(company: dict[str, Any] | None) -> bool:
    caja = normalize_text((company or {}).get("caja_compensacion") or "")
    return "compensar" in caja


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
            ]
        )
    )


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
    is_compensar = _is_compensar_company(company)
    signal_text = _analysis_signal_text(analysis=analysis, message=message)

    suggestion = DecisionSuggestion(
        observaciones="",
        observacion_agencia="",
        seguimiento_servicio="",
        confidence="low",
        rationale=tuple(rationale),
    )

    def finalize(row: dict[str, Any], *, confidence: str, extra_rationale: list[str], observaciones: str = "") -> DecisionSuggestion:
        return DecisionSuggestion(
            codigo_servicio=str(row.get("codigo_servicio") or ""),
            referencia_servicio=str(row.get("referencia_servicio") or ""),
            descripcion_servicio=str(row.get("descripcion_servicio") or ""),
            modalidad_servicio=str(row.get("modalidad_servicio") or modalidad or ""),
            valor_base=float(row.get("valor_base") or 0),
            observaciones=observaciones,
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
        family = "compensar" if is_compensar else "reca"
        row = _select_tarifa(
            lambda item: "reactivacion" in normalize_text(item.get("descripcion_servicio"))
            and family in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="medium",
                extra_rationale=["Se asigno familia de codigo de mantenimiento/reactivacion."],
            )

    if document_kind == "program_presentation" and modalidad:
        family = "compensar" if is_compensar else "reca"
        row = _select_tarifa(
            lambda item: "promocion" in normalize_text(item.get("descripcion_servicio"))
            and "individual" in normalize_text(item.get("descripcion_servicio"))
            and family in normalize_text(item.get("descripcion_servicio"))
            and normalize_text(item.get("modalidad_servicio")) == normalize_text(modalidad)
        )
        if row:
            return finalize(
                row,
                confidence="medium",
                extra_rationale=[
                    "Se uso promocion individual como aproximacion para presentacion del programa.",
                ],
                observaciones="Validar si corresponde promocion individual o una modalidad grupal antes de publicar.",
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
                observaciones=(
                    ""
                    if is_special_follow_up
                    else "Validar si corresponde seguimiento estandar o visita adicional de casos especiales."
                ),
            )

    if document_kind == "accessibility_assessment":
        rationale.extend(
            [
                "La familia de codigo es evaluacion de accesibilidad.",
                "Falta el tamano de empresa (hasta 50 o desde 51 trabajadores) para escoger codigo exacto.",
            ]
        )
        return DecisionSuggestion(
            modalidad_servicio=modalidad,
            observaciones="Validar tamano de empresa para escoger entre codigos 43/44 o 45/46.",
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
