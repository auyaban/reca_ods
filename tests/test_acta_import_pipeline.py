from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.acta_import_pipeline import (
    _professional_name_matches,
    build_import_result_from_parsed,
)


class ActaImportPipelineTests(unittest.TestCase):
    def test_professional_name_matching_accepts_coherent_subset(self) -> None:
        score = _professional_name_matches(
            "Leidy Novoa",
            "Leidy Johana Novoa Casasbuenas",
        )
        self.assertGreaterEqual(score, 0.96)

    @patch("app.services.acta_import_pipeline._companies")
    @patch("app.services.acta_import_pipeline._users_by_cedula")
    @patch("app.services.acta_import_pipeline._professionals")
    def test_build_import_result_resolves_partial_professional_name(
        self,
        mock_professionals,
        mock_users,
        mock_companies,
    ) -> None:
        mock_professionals.return_value = (
            {
                "nombre_profesional": "Leidy Johana Novoa Casasbuenas",
                "correo_profesional": "leidy@recacolombia.org",
                "programa": "Inclusion",
            },
        )
        mock_users.return_value = {}
        mock_companies.return_value = (
            {
                "nit_empresa": "890900291-8",
                "nombre_empresa": "SOLLA S.A",
                "caja_compensacion": "Compensar",
                "asesor": "Asesor Demo",
                "zona_empresa": "Mosquera",
                "sede_empresa": "Mosquera",
                "ciudad_empresa": "Mosquera",
            },
        )

        result = build_import_result_from_parsed(
            {
                "nombre_profesional": "Leidy Novoa",
                "candidatos_profesional": ["Leidy Novoa"],
                "asistentes": ["Leidy Novoa", "Maria Yomaira Garnica"],
                "nombre_empresa": "SOLLA S.A",
                "nit_empresa": "890900291-8",
                "fecha_servicio": "2026-03-04",
                "modalidad_servicio": "Virtual",
                "participantes": [],
                "warnings": [],
            },
            source_label="seleccion.pdf",
            attachment={
                "filename": "seleccion.pdf",
                "document_kind": "inclusive_selection",
                "document_label": "Seleccion incluyente",
                "is_ods_candidate": True,
                "classification_score": 0.9,
                "classification_reason": "Acta ODS",
                "process_hint": "",
                "process_score": 0.0,
            },
            create_missing_interpreter=True,
        )

        self.assertEqual(
            result["professional_resolved"],
            "Leidy Johana Novoa Casasbuenas",
        )
        self.assertEqual(
            result["analysis"]["nombre_profesional"],
            "Leidy Johana Novoa Casasbuenas",
        )


if __name__ == "__main__":
    unittest.main()
