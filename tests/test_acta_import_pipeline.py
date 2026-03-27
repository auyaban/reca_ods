from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.services.acta_import_pipeline import (
    _professional_name_matches,
    build_import_result_from_completion_payload,
    build_import_result_from_finalized_record,
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
    @patch("app.services.acta_import_pipeline.suggest_service_from_analysis")
    @patch("app.services.acta_import_pipeline._company_by_nit")
    def test_build_import_result_resolves_company_by_direct_nit_lookup_when_company_cache_is_incomplete(
        self,
        mock_company_by_nit,
        mock_suggest_service,
        mock_professionals,
        mock_users,
        mock_companies,
    ) -> None:
        mock_professionals.return_value = ()
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
        mock_company_by_nit.return_value = {
            "nit_empresa": "830095213-0",
            "nombre_empresa": "TERPEL SA",
            "caja_compensacion": "Compensar",
            "asesor": "Angie Stephanie Ramos Machuca",
            "zona_empresa": "Chapinero",
            "sede_empresa": "Principal",
            "ciudad_empresa": "Bogotá",
        }
        mock_suggest_service.return_value = MagicMock(
            to_dict=lambda: {
                "codigo_servicio": "47",
                "referencia_servicio": "IL4.RV.V",
                "descripcion_servicio": "Revisión de las Condiciones de la Vacante Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 121274.0,
                "confidence": "high",
                "observaciones": "Analista planeación financiera (1)",
                "observacion_agencia": "",
                "seguimiento_servicio": "",
                "rationale": [],
            }
        )

        result = build_import_result_from_parsed(
            {
                "nombre_profesional": "Sara Zambrano",
                "candidatos_profesional": ["Sara Zambrano"],
                "asistentes": ["Sara Zambrano"],
                "nombre_empresa": "TERPEL SA",
                "nit_empresa": "830095213-0",
                "fecha_servicio": "2026-03-26",
                "modalidad_servicio": "Virtual",
                "cargo_objetivo": "Analista planeación financiera",
                "total_vacantes": 1,
                "participantes": [],
                "warnings": [],
            },
            source_label="TERPEL vacante.pdf",
            attachment={
                "filename": "TERPEL vacante.pdf",
                "document_kind": "vacancy_review",
                "document_label": "Revision de condicion o vacante",
                "is_ods_candidate": True,
                "classification_score": 0.92,
                "classification_reason": "Acta ODS",
                "process_hint": "",
                "process_score": 0.0,
            },
            create_missing_interpreter=True,
        )

        self.assertEqual(result["analysis"]["nit_empresa"], "830095213-0")
        self.assertEqual(result["analysis"]["nombre_empresa"], "TERPEL SA")
        self.assertEqual(result["empresa_resolved"]["sede_empresa"], "Principal")
        self.assertEqual(result["blocking_errors"], [])
        self.assertNotIn("El NIT 830095213-0 no existe en Supabase.", result["warnings"])

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

    @patch("app.services.acta_import_pipeline._companies")
    @patch("app.services.acta_import_pipeline._users_by_cedula")
    @patch("app.services.acta_import_pipeline._professionals")
    def test_build_import_result_from_completion_payload(
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
        mock_users.return_value = {
            "123": {
                "cedula_usuario": "123",
                "nombre_usuario": "Ana Perez",
                "discapacidad_usuario": "Auditiva",
                "genero_usuario": "Mujer",
                "cargo_oferente": "Auxiliar",
                "fecha_firma_contrato": "",
                "tipo_contrato": "",
            }
        }
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

        result = build_import_result_from_completion_payload(
            {
                "schema_version": 1,
                "attachment": {
                    "filename": "seleccion.xlsx",
                    "document_kind": "inclusive_selection",
                    "document_label": "Seleccion incluyente",
                    "is_ods_candidate": True,
                    "classification_score": 1.0,
                    "classification_reason": "Generado por IL",
                    "process_hint": "",
                    "process_score": 1.0,
                },
                "parsed_raw": {
                    "nombre_profesional": "Leidy Novoa",
                    "candidatos_profesional": ["Leidy Novoa"],
                    "asistentes": ["Leidy Novoa"],
                    "nombre_empresa": "SOLLA S.A",
                    "nit_empresa": "890900291-8",
                    "fecha_servicio": "2026-03-04",
                    "modalidad_servicio": "Virtual",
                    "participantes": [
                        {
                            "nombre_usuario": "Ana Perez",
                            "cedula_usuario": "123",
                            "discapacidad_usuario": "Auditiva",
                            "genero_usuario": "Femenino",
                        }
                    ],
                    "warnings": [],
                },
            },
            source_label="Seleccion incluyente",
            row_context={"payload_schema_version": 1},
            create_missing_interpreter=True,
        )

        self.assertEqual(result["attachment"]["document_kind"], "inclusive_selection")
        self.assertEqual(result["analysis"]["nombre_profesional"], "Leidy Johana Novoa Casasbuenas")
        self.assertEqual(result["analysis"]["participantes"][0]["cedula_usuario"], "123")

    @patch("app.services.acta_import_pipeline.build_import_result_from_source")
    @patch("app.services.acta_import_pipeline.build_import_result_from_completion_payload")
    def test_build_import_result_from_finalized_record_prefers_payload(
        self,
        mock_from_payload,
        mock_from_source,
    ) -> None:
        mock_from_payload.return_value = {"source_label": "payload"}

        result = build_import_result_from_finalized_record(
            {
                "registro_id": "row-1",
                "nombre_formato": "Condiciones de Vacante",
                "path_formato": r"C:\tmp\vacante.xlsx",
                "payload_schema_version": 1,
                "payload_normalized": {
                    "schema_version": 1,
                    "attachment": {"filename": "vacante.xlsx"},
                    "parsed_raw": {"nombre_empresa": "Empresa Demo"},
                },
            }
        )

        self.assertEqual(result["source_label"], "payload")
        mock_from_payload.assert_called_once()
        mock_from_source.assert_not_called()

    @patch("app.services.acta_import_pipeline.build_import_result_from_source")
    @patch("app.services.acta_import_pipeline.build_import_result_from_completion_payload")
    def test_build_import_result_from_finalized_record_falls_back_to_source_when_payload_fails(
        self,
        mock_from_payload,
        mock_from_source,
    ) -> None:
        mock_from_payload.side_effect = ValueError("payload invalido")
        mock_from_source.return_value = {"source_label": "fallback"}

        result = build_import_result_from_finalized_record(
            {
                "registro_id": "row-2",
                "nombre_formato": "Seguimiento",
                "path_formato": r"C:\tmp\seguimiento.xlsx",
                "payload_schema_version": 2,
                "payload_normalized": {"schema_version": 2},
            }
        )

        self.assertEqual(result["source_label"], "fallback")
        mock_from_source.assert_called_once()

    def test_build_import_result_from_finalized_record_raises_when_payload_fails_without_source(self) -> None:
        with self.assertRaises(RuntimeError):
            build_import_result_from_finalized_record(
                {
                    "registro_id": "row-3",
                    "nombre_formato": "Seguimiento",
                    "payload_schema_version": 2,
                    "payload_normalized": {"schema_version": 2},
                }
            )


if __name__ == "__main__":
    unittest.main()
