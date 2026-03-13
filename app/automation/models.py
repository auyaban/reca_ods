from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

AutomationState = Literal["pending", "ready", "blocked"]


@dataclass(frozen=True)
class AutomationComponentStatus:
    key: str
    title: str
    state: AutomationState
    description: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class AutomationNextStep:
    title: str
    detail: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class AutomationSkeletonStatus:
    title: str
    summary: str
    environment: str
    components: tuple[AutomationComponentStatus, ...] = field(default_factory=tuple)
    next_steps: tuple[AutomationNextStep, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "summary": self.summary,
            "environment": self.environment,
            "components": [item.to_dict() for item in self.components],
            "next_steps": [item.to_dict() for item in self.next_steps],
        }


@dataclass(frozen=True)
class GmailMessageRef:
    message_id: str
    thread_id: str
    subject: str
    sender: str
    sender_email: str
    to_address: str
    received_at: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class AttachmentRef:
    attachment_id: str
    filename: str
    mime_type: str
    size_bytes: int = 0
    process_hint: str = ""
    process_score: float = 0.0
    document_kind: str = "unknown"
    document_label: str = ""
    is_ods_candidate: bool = False
    classification_score: float = 0.0
    classification_reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class GmailCandidateEmail:
    message: GmailMessageRef
    attachments: tuple[AttachmentRef, ...] = field(default_factory=tuple)
    sender_allowed: bool = False
    matched_professional: str = ""
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "message": self.message.to_dict(),
            "attachments": [item.to_dict() for item in self.attachments],
            "sender_allowed": self.sender_allowed,
            "matched_professional": self.matched_professional,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class ExtractedActaData:
    source_label: str
    nit_empresa: str = ""
    nombre_empresa: str = ""
    fecha_servicio: str = ""
    nombre_profesional: str = ""
    modalidad_servicio: str = ""
    participantes: tuple[dict[str, str], ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DecisionSuggestion:
    codigo_servicio: str = ""
    referencia_servicio: str = ""
    descripcion_servicio: str = ""
    modalidad_servicio: str = ""
    valor_base: float | None = None
    observaciones: str = ""
    observacion_agencia: str = ""
    seguimiento_servicio: str = ""
    confidence: str = "low"
    rationale: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return asdict(self)
