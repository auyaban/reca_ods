from __future__ import annotations

from dataclasses import asdict, dataclass

from app.utils.text import normalize_search_text


@dataclass(frozen=True)
class DocumentClassification:
    document_kind: str
    document_label: str
    is_ods_candidate: bool
    classification_score: float
    classification_reason: str

    def to_dict(self) -> dict:
        return asdict(self)


_EXPLICIT_RULES: tuple[tuple[tuple[str, ...], DocumentClassification], ...] = (
    (
        ("interprete lsc", "interprete", "servicio interprete"),
        DocumentClassification(
            document_kind="interpreter_service",
            document_label="Servicio interprete",
            is_ods_candidate=True,
            classification_score=0.95,
            classification_reason="El archivo parece corresponder a un servicio de interprete LSC.",
        ),
    ),
    (
        ("control de asistencia",),
        DocumentClassification(
            document_kind="attendance_support",
            document_label="Control de asistencia",
            is_ods_candidate=False,
            classification_score=0.99,
            classification_reason="El nombre del archivo corresponde a soporte de asistencia, no al acta principal.",
        ),
    ),
    (
        ("levantamiento del perfil", "condiciones de la vacante", "revision de las condiciones"),
        DocumentClassification(
            document_kind="vacancy_review",
            document_label="Revision de condicion o vacante",
            is_ods_candidate=True,
            classification_score=0.92,
            classification_reason="El archivo parece corresponder a una revision de condicion/vacante util para ODS.",
        ),
    ),
    (
        ("presentacion del programa",),
        DocumentClassification(
            document_kind="program_presentation",
            document_label="Presentacion del programa",
            is_ods_candidate=True,
            classification_score=0.92,
            classification_reason="El archivo parece ser una presentacion del programa.",
        ),
    ),
    (
        ("evaluacion de accesibilidad",),
        DocumentClassification(
            document_kind="accessibility_assessment",
            document_label="Evaluacion de accesibilidad",
            is_ods_candidate=True,
            classification_score=0.92,
            classification_reason="El archivo parece ser una evaluacion de accesibilidad.",
        ),
    ),
    (
        ("reactivacion del programa", "reactivacion programa"),
        DocumentClassification(
            document_kind="program_reactivation",
            document_label="Reactivacion del programa",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una reactivacion del programa.",
        ),
    ),
    (
        ("seguimiento", "seguimientos"),
        DocumentClassification(
            document_kind="follow_up",
            document_label="Seguimiento",
            is_ods_candidate=True,
            classification_score=0.88,
            classification_reason="El archivo parece ser un seguimiento.",
        ),
    ),
    (
        ("sensibilizacion",),
        DocumentClassification(
            document_kind="sensibilizacion",
            document_label="Sensibilizacion",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una sensibilizacion.",
        ),
    ),
    (
        ("seleccion incluyente", "seleccion_incluyente"),
        DocumentClassification(
            document_kind="inclusive_selection",
            document_label="Seleccion incluyente",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una seleccion incluyente.",
        ),
    ),
    (
        ("contratacion incluyente", "contratacion_incluyente"),
        DocumentClassification(
            document_kind="inclusive_hiring",
            document_label="Contratacion incluyente",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una contratacion incluyente.",
        ),
    ),
    (
        ("induccion operativa",),
        DocumentClassification(
            document_kind="operational_induction",
            document_label="Induccion operativa",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una induccion operativa.",
        ),
    ),
    (
        ("induccion organizacional",),
        DocumentClassification(
            document_kind="organizational_induction",
            document_label="Induccion organizacional",
            is_ods_candidate=True,
            classification_score=0.9,
            classification_reason="El archivo parece ser una induccion organizacional.",
        ),
    ),
)


def classify_document(*, filename: str, subject: str = "", process_hint: str = "", process_score: float = 0.0) -> DocumentClassification:
    text = normalize_search_text(f"{filename} {subject}")
    for tokens, classification in _EXPLICIT_RULES:
        if any(token in text for token in tokens):
            return classification

    if process_hint and float(process_score or 0) >= 0.5:
        return DocumentClassification(
            document_kind="process_match",
            document_label=process_hint.replace("_", " ").strip().title(),
            is_ods_candidate=True,
            classification_score=float(process_score),
            classification_reason="El nombre del archivo coincide de forma razonable con un proceso conocido.",
        )

    return DocumentClassification(
        document_kind="needs_review",
        document_label="Requiere revision",
        is_ods_candidate=False,
        classification_score=0.0,
        classification_reason="No hubo señales suficientes para clasificar el PDF de forma confiable.",
    )
