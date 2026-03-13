from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile

from app.automation.gmail_inbox import GmailInboxGateway
from app.automation.models import (
    AutomationComponentStatus,
    AutomationNextStep,
    AutomationSkeletonStatus,
    GmailCandidateEmail,
)
from app.automation.process_catalog import list_process_template_names
from app.automation.rules_engine import suggest_service_from_analysis
from app.config import get_settings
from app.services.excel_acta_import import parse_acta_pdf
from app.supabase_client import execute_with_reauth


def _professional_email_map() -> dict[str, str]:
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
            "Base de automatizacion ODS preparada para conectar Gmail, extraccion, "
            "reglas de decision y staging sin tocar produccion."
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
                description="Reglas preliminares activas para clasificar proceso, sugerir codigo y dejar observaciones con confianza.",
            ),
            AutomationComponentStatus(
                key="staging",
                title="Staging de Revision",
                state="pending",
                description="Pendiente definir almacenamiento temporal para aprobar antes de publicar en ODS.",
            ),
            AutomationComponentStatus(
                key="publish",
                title="Publicacion",
                state="ready",
                description="El flujo actual ya inserta ODS y sincroniza a Google Drive/Sheets cuando el caso este aprobado.",
            ),
        ),
        next_steps=(
            AutomationNextStep(
                title="1. Leer Gmail",
                detail="Listar correos candidatos y adjuntos PDF desde Aaron TEST sin descargar ni publicar.",
            ),
            AutomationNextStep(
                title="2. Ejecutar parser actual",
                detail="Transformar adjuntos PDF en datos estructurados reutilizando el importador existente.",
            ),
            AutomationNextStep(
                title="3. Refinar reglas",
                detail="Cerrar familias pendientes y subir confianza con nuevas senales del PDF y del correo.",
            ),
            AutomationNextStep(
                title="4. Guardar en staging",
                detail="Enviar el resultado a revision manual antes de usar el flujo normal de publicacion.",
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

    sender_match = allowed_senders.get(message.sender_email.lower(), "")
    warnings = list(parsed.get("warnings") or [])
    if sender_match:
        parsed["matched_professional_sender"] = sender_match
        detected_professional = str(parsed.get("nombre_profesional") or "").strip().lower()
        if detected_professional and detected_professional != sender_match.strip().lower():
            warnings.append("El profesional detectado en el PDF no coincide con el remitente del correo.")
    else:
        warnings.append("El remitente del correo no coincide con la tabla de profesionales.")
    if attachment.process_hint:
        parsed["process_hint"] = attachment.process_hint
        parsed["process_score"] = attachment.process_score
    parsed["document_kind"] = attachment.document_kind
    parsed["document_label"] = attachment.document_label
    parsed["is_ods_candidate"] = attachment.is_ods_candidate
    parsed["classification_score"] = attachment.classification_score
    parsed["classification_reason"] = attachment.classification_reason
    if not attachment.is_ods_candidate:
        warnings.append("Este PDF parece soporte o anexo; no se recomienda tratarlo como acta ODS principal.")
    warnings = list(dict.fromkeys(warnings))
    parsed["warnings"] = warnings
    suggestion = suggest_service_from_analysis(analysis=parsed, message=message.to_dict())

    return {
        "data": {
            "message": message.to_dict(),
            "attachment": attachment.to_dict(),
            "analysis": parsed,
            "suggestion": suggestion.to_dict(),
        }
    }
