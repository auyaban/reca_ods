from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.automation.models import AttachmentRef, GmailMessageRef
from app.automation.orchestrator import get_automation_attachment_analysis


class AutomationAttachmentAnalysisTests(unittest.TestCase):
    @patch("app.automation.orchestrator.parse_acta_pdf")
    @patch("app.automation.orchestrator._professional_email_map")
    @patch("app.automation.orchestrator._gmail_gateway")
    def test_returns_parsed_attachment_analysis(
        self,
        mock_gateway_factory,
        mock_professional_email_map,
        mock_parse_acta_pdf,
    ) -> None:
        message = GmailMessageRef(
            message_id="msg-1",
            thread_id="thread-1",
            subject="Asunto",
            sender="Ana Perez <ana@recacolombia.org>",
            sender_email="ana@recacolombia.org",
            to_address="gestiondocumental@recacolombia.org",
            received_at="Fri, 13 Mar 2026 10:00:00 -0500",
        )
        attachment = AttachmentRef(
            attachment_id="att-1",
            filename="presentacion.pdf",
            mime_type="application/pdf",
            size_bytes=1234,
            process_hint="presentación_programa",
            process_score=0.55,
        )
        gateway = MagicMock()
        gateway.get_message_ref.return_value = message
        gateway.list_pdf_attachments.return_value = [attachment]
        gateway.download_attachment_bytes.return_value = b"%PDF-1.4"
        mock_gateway_factory.return_value = gateway
        mock_professional_email_map.return_value = {"ana@recacolombia.org": "Ana Perez"}
        mock_parse_acta_pdf.return_value = {
            "nombre_empresa": "Empresa Demo",
            "nit_empresa": "900123456",
            "fecha_servicio": "2026-03-13",
            "nombre_profesional": "Ana Perez",
            "modalidad_servicio": "Virtual",
            "participantes": [],
            "warnings": [],
        }

        result = get_automation_attachment_analysis(
            {
                "message_id": "msg-1",
                "attachment_id": "att-1",
                "filename": "presentacion.pdf",
            }
        )

        data = result["data"]
        self.assertEqual(data["attachment"]["attachment_id"], "att-1")
        self.assertEqual(data["analysis"]["matched_professional_sender"], "Ana Perez")
        self.assertEqual(data["analysis"]["process_hint"], "presentación_programa")


if __name__ == "__main__":
    unittest.main()
