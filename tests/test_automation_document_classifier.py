from __future__ import annotations

import unittest

from app.automation.document_classifier import classify_document


class AutomationDocumentClassifierTests(unittest.TestCase):
    def test_marks_attendance_control_as_non_ods_candidate(self) -> None:
        result = classify_document(
            filename="CONTROL DE ASISTENCIA INCLUSION LABORAL.pdf",
            subject="Empresa Demo",
        )

        self.assertEqual(result.document_kind, "attendance_support")
        self.assertFalse(result.is_ods_candidate)

    def test_marks_program_presentation_as_ods_candidate(self) -> None:
        result = classify_document(
            filename="PRESENTACION DEL PROGRAMA DE INCLUSION LABORAL.pdf",
            subject="Empresa Demo",
        )

        self.assertEqual(result.document_kind, "program_presentation")
        self.assertTrue(result.is_ods_candidate)


if __name__ == "__main__":
    unittest.main()
