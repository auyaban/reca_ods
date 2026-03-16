from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from app.automation.models import AttachmentRef, GmailMessageRef
from app.automation.orchestrator import (
    process_automation_email_preview,
    publish_automation_email_preview,
)
from app.google_sheet_layouts import ODS_INPUT_HEADERS


class _SuggestionStub:
    def __init__(self, payload: dict) -> None:
        self._payload = dict(payload)

    def to_dict(self) -> dict:
        return dict(self._payload)


class AutomationEmailPreviewTests(unittest.TestCase):
    @patch("app.automation.orchestrator._download_and_parse_attachment")
    @patch("app.automation.orchestrator._company_by_name_strong")
    @patch("app.automation.orchestrator._company_by_nit_details")
    @patch("app.automation.orchestrator.build_import_result_from_parsed")
    @patch("app.automation.orchestrator.suggest_service_from_analysis")
    @patch("app.automation.orchestrator._gmail_gateway")
    def test_process_email_preview_builds_rows_and_skips_supports(
        self,
        mock_gateway_factory,
        mock_suggest_service,
        mock_build_import_result,
        mock_company_by_nit,
        mock_company_by_name,
        mock_download_and_parse,
    ) -> None:
        message = GmailMessageRef(
            message_id="msg-1",
            thread_id="thread-1",
            subject="Acta inclusion laboral",
            sender="Ana Perez <ana@recacolombia.org>",
            sender_email="ana@recacolombia.org",
            to_address="gestiondocumental@recacolombia.org",
            received_at="Fri, 13 Mar 2026 10:00:00 -0500",
        )
        attachments = [
            AttachmentRef(
                attachment_id="att-1",
                filename="reactivacion.pdf",
                mime_type="application/pdf",
                size_bytes=1234,
                document_kind="program_reactivation",
                document_label="Reactivacion",
                is_ods_candidate=True,
                classification_reason="Acta ODS candidata.",
            ),
            AttachmentRef(
                attachment_id="att-2",
                filename="control_asistencia.pdf",
                mime_type="application/pdf",
                size_bytes=400,
                document_kind="attendance_support",
                document_label="Control de asistencia",
                is_ods_candidate=False,
                classification_reason="Documento clasificado como soporte.",
            ),
        ]
        gateway = MagicMock()
        gateway.get_message_ref.return_value = message
        gateway.list_pdf_attachments.return_value = attachments
        gateway.download_attachment_bytes.return_value = b"%PDF-1.4"
        mock_gateway_factory.return_value = gateway
        mock_download_and_parse.return_value = {"file_path": "reactivacion.pdf"}
        mock_build_import_result.return_value = {
            "analysis": {
                "nombre_empresa": "Empresa Demo",
                "nit_empresa": "900123456-1",
                "fecha_servicio": "2026-03-13",
                "nombre_profesional": "Ana Perez",
                "modalidad_servicio": "Virtual",
                "document_kind": "program_reactivation",
                "participantes": [
                    {
                        "nombre_usuario": "Juan Villa",
                        "cedula_usuario": "1073520676",
                        "discapacidad_usuario": "Auditiva",
                        "genero_usuario": "Hombre",
                    }
                ],
                "warnings": [],
            }
        }
        mock_suggest_service.return_value = _SuggestionStub(
            {
                "codigo_servicio": "37",
                "referencia_servicio": "IL2.VMR.V.R",
                "descripcion_servicio": "Reactivacion virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
                "confidence": "medium",
                "observaciones": "OK",
                "observacion_agencia": "",
                "seguimiento_servicio": "",
                "rationale": ["Regla de reactivacion."],
            }
        )
        mock_company_by_nit.return_value = {
            "nit_empresa": "900123456-1",
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "asesor": "Asesor Demo",
            "sede_empresa": "Bogota",
        }
        mock_company_by_name.return_value = None

        result = process_automation_email_preview({"message_id": "msg-1"})

        data = result["data"]
        self.assertTrue(data["ready_to_upload"])
        self.assertEqual(len(data["upload_rows"]), 1)
        self.assertEqual(len(data["preview_rows"]), 1)
        self.assertEqual(data["skipped_count"], 1)
        self.assertEqual(data["upload_rows"][0]["codigo_servicio"], "37")
        self.assertEqual(data["preview_rows"][0]["documento"], "reactivacion.pdf")

    @patch("app.automation.orchestrator._company_by_name_strong")
    @patch("app.automation.orchestrator._company_by_nit_details")
    @patch("app.automation.orchestrator._download_and_parse_attachment")
    @patch("app.automation.orchestrator.build_import_result_from_parsed")
    @patch("app.automation.orchestrator.suggest_service_from_analysis")
    @patch("app.automation.orchestrator._gmail_gateway")
    def test_process_email_preview_resolves_interpreter_context_from_same_email(
        self,
        mock_gateway_factory,
        mock_suggest_service,
        mock_build_import_result,
        mock_download_and_parse,
        mock_company_by_nit,
        mock_company_by_name,
    ) -> None:
        message = GmailMessageRef(
            message_id="msg-2",
            thread_id="thread-2",
            subject="Acta principal + interprete",
            sender="Ana Perez <ana@recacolombia.org>",
            sender_email="ana@recacolombia.org",
            to_address="gestiondocumental@recacolombia.org",
            received_at="Fri, 13 Mar 2026 11:00:00 -0500",
        )
        attachments = [
            AttachmentRef(
                attachment_id="att-main",
                filename="seleccion.pdf",
                mime_type="application/pdf",
                size_bytes=1200,
                document_kind="inclusive_selection",
                document_label="Seleccion incluyente",
                is_ods_candidate=True,
                classification_reason="Acta ODS candidata.",
            ),
            AttachmentRef(
                attachment_id="att-int",
                filename="interprete.pdf",
                mime_type="application/pdf",
                size_bytes=800,
                document_kind="interpreter_service",
                document_label="Interprete LSC",
                is_ods_candidate=True,
                classification_reason="Acta interprete.",
            ),
        ]
        gateway = MagicMock()
        gateway.get_message_ref.return_value = message
        gateway.list_pdf_attachments.return_value = attachments
        gateway.download_attachment_bytes.return_value = b"%PDF-1.4"
        mock_gateway_factory.return_value = gateway
        mock_download_and_parse.side_effect = [
            {"file_path": "seleccion.pdf"},
            {"file_path": "interprete.pdf"},
        ]
        mock_build_import_result.side_effect = [
            {
                "analysis": {
                    "nombre_empresa": "SOLLA S.A",
                    "nit_empresa": "860001000-1",
                    "fecha_servicio": "2026-03-04",
                    "nombre_profesional": "Sara Nidia Sanchez Morales",
                    "interpretes": ["Sara Nidia Sanchez Morales", "Laura Alejandra Perez Bustacara"],
                    "modalidad_servicio": "Presencial",
                    "document_kind": "inclusive_selection",
                    "participantes": [
                        {
                            "nombre_usuario": "Juan Villa",
                            "cedula_usuario": "1073520676",
                            "discapacidad_usuario": "Auditiva",
                            "genero_usuario": "Hombre",
                        }
                    ],
                    "warnings": [],
                },
                "service_suggestion": {
                    "codigo_servicio": "56",
                    "referencia_servicio": "IL5.SI",
                    "descripcion_servicio": "Seleccion incluyente",
                    "modalidad_servicio": "Presencial",
                    "valor_base": 1000,
                    "confidence": "medium",
                    "observaciones": "Seleccion",
                    "observacion_agencia": "",
                    "seguimiento_servicio": "",
                    "rationale": ["Documento principal."],
                },
            },
            {
                "analysis": {
                    "nombre_empresa": "SOLLA S.A",
                    "nit_empresa": "",
                    "fecha_servicio": "2026-03-04",
                    "nombre_profesional": "Sara Nidia Sanchez Morales",
                    "interpretes": ["Sara Nidia Sanchez Morales", "Laura Alejandra Perez Bustacara"],
                    "modalidad_servicio": "Presencial",
                    "document_kind": "interpreter_service",
                    "participantes": [
                        {
                            "nombre_usuario": "Juan Villa",
                            "cedula_usuario": "1073520676",
                            "discapacidad_usuario": "",
                            "genero_usuario": "",
                        }
                    ],
                    "sumatoria_horas_interpretes": 2.0,
                    "total_horas_interprete": 2.0,
                    "warnings": [],
                },
                "service_suggestion": {
                    "codigo_servicio": "88",
                    "referencia_servicio": "LSC1.2H",
                    "descripcion_servicio": "Interprete 2 horas",
                    "modalidad_servicio": "Presencial",
                    "valor_base": 2000,
                    "confidence": "medium",
                    "observaciones": "Interprete",
                    "observacion_agencia": "",
                    "seguimiento_servicio": "",
                    "rationale": ["Horas detectadas en el acta."],
                },
            },
        ]

        def _suggestion_for_document(*, analysis: dict, message: dict) -> _SuggestionStub:
            if analysis.get("document_kind") == "interpreter_service":
                return _SuggestionStub(
                    {
                        "codigo_servicio": "88",
                        "referencia_servicio": "LSC1.2H",
                        "descripcion_servicio": "Interprete 2 horas",
                        "modalidad_servicio": "Presencial",
                        "valor_base": 2000,
                        "confidence": "medium",
                        "observaciones": "Interprete",
                        "observacion_agencia": "",
                        "seguimiento_servicio": "",
                        "rationale": ["Horas detectadas en el acta."],
                    }
                )
            return _SuggestionStub(
                {
                    "codigo_servicio": "56",
                    "referencia_servicio": "IL5.SI",
                    "descripcion_servicio": "Seleccion incluyente",
                    "modalidad_servicio": "Presencial",
                    "valor_base": 1000,
                    "confidence": "medium",
                    "observaciones": "Seleccion",
                    "observacion_agencia": "",
                    "seguimiento_servicio": "",
                    "rationale": ["Documento principal."],
                }
            )

        mock_suggest_service.side_effect = _suggestion_for_document
        mock_company_by_nit.side_effect = lambda nit: {
            "nit_empresa": "860001000-1",
            "nombre_empresa": "SOLLA S.A",
            "caja_compensacion": "Compensar",
            "asesor": "Asesor Demo",
            "sede_empresa": "Bogota",
        } if str(nit or "").strip() == "860001000-1" else None
        mock_company_by_name.return_value = None

        result = process_automation_email_preview({"message_id": "msg-2"})

        upload_rows = result["data"]["upload_rows"]
        interpreter_rows = [row for row in upload_rows if row["_document_kind"] == "interpreter_service"]
        self.assertEqual(len(interpreter_rows), 2)
        self.assertEqual({row["nombre_profesional"] for row in interpreter_rows}, {"Sara Nidia Sanchez Morales", "Laura Alejandra Perez Bustacara"})
        self.assertEqual({row["nit_empresa"] for row in interpreter_rows}, {"860001000-1"})
        self.assertEqual({row["nombre_empresa"] for row in interpreter_rows}, {"SOLLA S.A"})
        self.assertEqual({row["horas_interprete"] for row in interpreter_rows}, {2.0})

    @patch("app.automation.orchestrator._append_decision_log")
    @patch("app.automation.orchestrator.write_sheet_values")
    @patch("app.automation.orchestrator.read_sheet_values")
    @patch("app.automation.orchestrator.get_settings")
    def test_publish_email_preview_appends_only_new_rows(
        self,
        mock_get_settings,
        mock_read_sheet_values,
        mock_write_sheet_values,
        mock_append_decision_log,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            google_sheets_automation_test_spreadsheet_id="spreadsheet-123",
            google_sheets_automation_test_sheet_name="ODS_INPUT",
        )
        mock_read_sheet_values.side_effect = [
            [["existing-id"]],
            [ODS_INPUT_HEADERS, ["existing-id"]],
        ]
        mock_append_decision_log.return_value = Path("C:/Users/aaron/Desktop/decisiones.txt")

        base_row = {
            "nombre_profesional": "Ana Perez",
            "codigo_servicio": "37",
            "nombre_empresa": "Empresa Demo",
            "nit_empresa": "900123456-1",
            "caja_compensacion": "Compensar",
            "fecha_servicio": "2026-03-13",
            "nombre_usuario": "Juan Villa",
            "cedula_usuario": "1073520676",
            "discapacidad_usuario": "Auditiva",
            "fecha_ingreso": "",
            "observaciones": "OK",
            "modalidad_servicio": "Virtual",
            "orden_clausulada": "no",
            "genero_usuario": "Hombre",
            "tipo_contrato": "",
            "asesor_empresa": "Asesor Demo",
            "sede_empresa": "Bogota",
            "observacion_agencia": "",
            "seguimiento_servicio": "",
            "cargo_servicio": "",
            "total_personas": 1,
            "horas_interprete": "",
            "mes_servicio": 3,
            "ano_servicio": 2026,
        }

        result = publish_automation_email_preview(
            {
                "message": {"message_id": "msg-1", "subject": "Acta demo", "sender_email": "ana@recacolombia.org"},
                "upload_rows": [
                    {"id": "existing-id", **base_row},
                    {"id": "new-id", **base_row},
                ],
                "decision_log_entries": ["entrada-base"],
            }
        )

        self.assertEqual(result["data"]["written_count"], 1)
        self.assertEqual(result["data"]["skipped_existing_count"], 1)
        mock_write_sheet_values.assert_called_once()
        args = mock_write_sheet_values.call_args[0]
        self.assertEqual(args[0], "spreadsheet-123")
        self.assertEqual(args[1], "'ODS_INPUT'!A3:Y3")
        self.assertEqual(len(args[2]), 1)


if __name__ == "__main__":
    unittest.main()
