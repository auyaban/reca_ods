from __future__ import annotations

import base64
from email.utils import parseaddr
from typing import Any

from app.automation.document_classifier import classify_document
from app.automation.models import AttachmentRef, GmailMessageRef
from app.automation.process_catalog import guess_process_from_filename, list_process_template_names
from app.google_sheets_client import get_google_gmail_service

_PDF_MIME_TYPES = {"application/pdf"}


def _header_map(payload: dict[str, Any]) -> dict[str, str]:
    headers = payload.get("headers") or []
    result: dict[str, str] = {}
    for item in headers:
        name = str(item.get("name") or "").strip().lower()
        value = str(item.get("value") or "").strip()
        if name and value:
            result[name] = value
    return result


def _collect_pdf_parts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parts = list(payload.get("parts") or [])
    collected: list[dict[str, Any]] = []
    if not parts:
        mime_type = str(payload.get("mimeType") or "").strip().lower()
        if payload.get("filename") and payload.get("body", {}).get("attachmentId") and mime_type in _PDF_MIME_TYPES:
            return [payload]
        return []

    for part in parts:
        mime_type = str(part.get("mimeType") or "").strip().lower()
        filename = str(part.get("filename") or "").strip()
        body = part.get("body") or {}
        if filename and body.get("attachmentId") and mime_type in _PDF_MIME_TYPES:
            collected.append(part)
        if part.get("parts"):
            collected.extend(_collect_pdf_parts(part))
    return collected


class GmailInboxGateway:
    """Read-only Gmail gateway for Aaron TEST."""

    def __init__(self, *, delegated_user: str, to_filter: str, max_results: int = 20) -> None:
        self.delegated_user = str(delegated_user or "").strip()
        self.to_filter = str(to_filter or "").strip()
        self.max_results = max(1, int(max_results or 20))
        self._template_names = list_process_template_names()

    def _service(self):
        return get_google_gmail_service(delegated_subject=self.delegated_user)

    def get_message_ref(self, message_id: str) -> GmailMessageRef:
        service = self._service()
        full = (
            service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute()
        )
        payload = full.get("payload") or {}
        headers = _header_map(payload)
        sender = headers.get("from", "")
        _sender_name, sender_email = parseaddr(sender)
        return GmailMessageRef(
            message_id=str(full.get("id") or message_id).strip(),
            thread_id=str(full.get("threadId") or "").strip(),
            subject=headers.get("subject", ""),
            sender=sender or sender_email,
            sender_email=sender_email.strip().lower(),
            to_address=headers.get("to", ""),
            received_at=headers.get("date", ""),
        )

    def _query(self) -> str:
        filters = ["has:attachment", "filename:pdf", "-in:trash"]
        if self.to_filter:
            filters.append(f'to:"{self.to_filter}"')
        return " ".join(filters)

    def list_candidate_messages(self) -> list[GmailMessageRef]:
        service = self._service()
        response = (
            service.users()
            .messages()
            .list(
                userId="me",
                q=self._query(),
                maxResults=self.max_results,
                includeSpamTrash=False,
            )
            .execute()
        )
        messages: list[GmailMessageRef] = []
        for item in list(response.get("messages") or []):
            message_id = str(item.get("id") or "").strip()
            if not message_id:
                continue
            messages.append(self.get_message_ref(message_id))
        return messages

    def list_pdf_attachments(self, message: GmailMessageRef) -> list[AttachmentRef]:
        service = self._service()
        full = (
            service.users()
            .messages()
            .get(userId="me", id=message.message_id, format="full")
            .execute()
        )
        payload = full.get("payload") or {}
        attachments: list[AttachmentRef] = []
        for part in _collect_pdf_parts(payload):
            filename = str(part.get("filename") or "").strip()
            mime_type = str(part.get("mimeType") or "").strip()
            body = part.get("body") or {}
            process_hint, process_score = guess_process_from_filename(filename, self._template_names)
            classification = classify_document(
                filename=filename,
                subject=message.subject,
                process_hint=process_hint,
                process_score=process_score,
            )
            attachments.append(
                AttachmentRef(
                    attachment_id=str(body.get("attachmentId") or "").strip(),
                    filename=filename,
                    mime_type=mime_type,
                    size_bytes=int(body.get("size") or 0),
                    process_hint=process_hint,
                    process_score=process_score,
                    document_kind=classification.document_kind,
                    document_label=classification.document_label,
                    is_ods_candidate=classification.is_ods_candidate,
                    classification_score=classification.classification_score,
                    classification_reason=classification.classification_reason,
                )
            )
        return attachments

    def download_attachment_bytes(self, message: GmailMessageRef, attachment: AttachmentRef) -> bytes:
        service = self._service()
        response = (
            service.users()
            .messages()
            .attachments()
            .get(
                userId="me",
                messageId=message.message_id,
                id=attachment.attachment_id,
            )
            .execute()
        )
        encoded = str(response.get("data") or "").strip()
        if not encoded:
            return b""
        return base64.urlsafe_b64decode(encoded.encode("ascii"))
