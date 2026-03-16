from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.automation.staging import AutomationStagingRepository


class AutomationStagingRepositoryTests(unittest.TestCase):
    def test_save_and_list_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = AutomationStagingRepository(Path(tmpdir) / "automation_staging.json")

            case = repo.save_case(
                message={
                    "message_id": "msg-1",
                    "received_at": "2026-03-13T10:00:00+00:00",
                    "subject": "Asunto",
                },
                attachment={
                    "filename": "reactivacion.pdf",
                    "document_label": "Reactivacion del programa",
                },
                analysis={
                    "nombre_empresa": "Empresa Demo",
                },
                suggestion={
                    "codigo_servicio": "37",
                    "confidence": "medium",
                },
            )

            cases = repo.list_cases()

            self.assertEqual(len(cases), 1)
            self.assertEqual(cases[0].case_id, case.case_id)
            self.assertEqual(cases[0].analysis["nombre_empresa"], "Empresa Demo")

    def test_update_case_changes_suggestion_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = AutomationStagingRepository(Path(tmpdir) / "automation_staging.json")
            case = repo.save_case(
                message={
                    "message_id": "msg-1",
                    "received_at": "2026-03-13T10:00:00+00:00",
                    "subject": "Asunto",
                },
                attachment={"filename": "reactivacion.pdf"},
                analysis={"nombre_empresa": "Empresa Demo"},
                suggestion={"codigo_servicio": "37", "confidence": "medium"},
            )

            updated = repo.update_case(
                case_id=case.case_id,
                suggestion_updates={"codigo_servicio": "40", "observaciones": "Ajustado manualmente"},
                status="approved_for_publish",
            )

            self.assertEqual(updated.status, "approved_for_publish")
            self.assertEqual(updated.suggestion["codigo_servicio"], "40")
            self.assertEqual(updated.suggestion["observaciones"], "Ajustado manualmente")


if __name__ == "__main__":
    unittest.main()
