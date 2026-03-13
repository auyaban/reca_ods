from __future__ import annotations

import unittest

from app.automation.process_catalog import guess_process_from_filename


class AutomationProcessCatalogTests(unittest.TestCase):
    def test_guess_process_from_filename_matches_similar_template_name(self) -> None:
        process, score = guess_process_from_filename(
            "ACTA Sensibilizacion empresa marzo 2026.pdf",
            ["sensibilizacion", "seguimientos", "seleccion_incluyente"],
        )

        self.assertEqual(process, "sensibilizacion")
        self.assertGreaterEqual(score, 0.9)

    def test_guess_process_from_filename_returns_blank_for_unrelated_name(self) -> None:
        process, score = guess_process_from_filename(
            "archivo_generico.pdf",
            ["sensibilizacion", "seguimientos", "seleccion_incluyente"],
        )

        self.assertEqual(process, "")
        self.assertEqual(score, 0.0)


if __name__ == "__main__":
    unittest.main()
