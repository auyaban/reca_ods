from __future__ import annotations

import unittest
from unittest.mock import patch

from app.automation.rules_engine import suggest_service_from_analysis


class AutomationRulesEngineTests(unittest.TestCase):
    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_suggests_review_vacancy_code(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Scotiabank",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogotá",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "47",
                "referencia_servicio": "IL4.RV.V",
                "descripcion_servicio": "Revisión de las Condiciones de la Vacante Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 121274,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "vacancy_review",
                "participantes": [],
            },
            message={"subject": "Revision de condiciones de la vacante virtual"},
        )

        self.assertEqual(result.codigo_servicio, "47")
        self.assertEqual(result.confidence, "high")

    @patch("app.automation.rules_engine._company_by_nit")
    def test_marks_attendance_support_as_non_publishable(self, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = None

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "attendance_support",
                "participantes": [],
            },
            message={"subject": "Control de asistencia"},
        )

        self.assertEqual(result.codigo_servicio, "")
        self.assertEqual(result.confidence, "low")
        self.assertEqual(result.observaciones, "")

    @patch("app.automation.rules_engine._company_by_nit")
    def test_interpreter_service_requires_duration(self, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = None

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "interpreter_service",
                "participantes": [],
            },
            message={"subject": "Servicio interprete LSC"},
        )

        self.assertEqual(result.codigo_servicio, "")
        self.assertEqual(result.observaciones, "")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_suggests_inclusive_hiring_code_from_participant_bucket(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogotá",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "65",
                "referencia_servicio": "IL6.PCG2-4.V",
                "descripcion_servicio": "Proceso de Contratación Incluyente Grupal De 2 a 4 Oferentes Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 349524,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "inclusive_hiring",
                "participantes": [
                    {"cedula_usuario": "1"},
                    {"cedula_usuario": "2"},
                    {"cedula_usuario": "3"},
                ],
            },
            message={"subject": "Proceso de contratación incluyente virtual"},
        )

        self.assertEqual(result.codigo_servicio, "65")
        self.assertEqual(result.referencia_servicio, "IL6.PCG2-4.V")
        self.assertEqual(result.confidence, "medium")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_suggests_follow_up_standard_code(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogotá",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "83",
                "referencia_servicio": "IL10.SA.V",
                "descripcion_servicio": "Seguimiento y Acompañamiento al Proceso de Inclusión Laboral Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 178335,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "follow_up",
                "participantes": [],
            },
            message={"subject": "Seguimiento virtual inclusion laboral"},
        )

        self.assertEqual(result.codigo_servicio, "83")
        self.assertEqual(result.confidence, "medium")
        self.assertEqual(result.observaciones, "")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_suggests_interpreter_code_when_duration_is_detected(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = None
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "89",
                "referencia_servicio": "LSC1.TM",
                "descripcion_servicio": "Valor Interprete LSC * por 45 min de servicio",
                "modalidad_servicio": "Todas la modalidades",
                "valor_base": 62283,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "interpreter_service",
                "participantes": [],
                "file_path": "SERVICIO INTERPRETE LSC 45 MIN.pdf",
            },
            message={"subject": "Servicio interprete LSC"},
        )

        self.assertEqual(result.codigo_servicio, "89")
        self.assertEqual(result.confidence, "medium")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_defaults_reactivation_management_to_reca_when_template_is_missing(
        self,
        mock_tarifas,
        mock_company_by_nit,
    ) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "37",
                "referencia_servicio": "IL2.VMR.V.R",
                "descripcion_servicio": "Visita de Mantenimiento o Reactivacion -Gestion empresarial RECA -VIRTUAL",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "program_reactivation",
                "modalidad_servicio": "Virtual",
                "participantes": [],
            },
            message={"subject": "Reactivacion del programa"},
        )

        self.assertEqual(result.codigo_servicio, "37")
        self.assertEqual(result.confidence, "low")
        self.assertEqual(result.observaciones, "")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_defaults_accessibility_company_size_to_under_50_when_missing(
        self,
        mock_tarifas,
        mock_company_by_nit,
    ) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Sin dato",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "43",
                "referencia_servicio": "IL3.EA.ME.B.50",
                "descripcion_servicio": "Evaluacion de Accesibilidad Microempresa (hasta 50 trabajadores) BOGOTA",
                "modalidad_servicio": "Bogota",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "accessibility_assessment",
                "modalidad_servicio": "Bogota",
                "participantes": [],
            },
            message={"subject": "Evaluacion de accesibilidad"},
        )

        self.assertEqual(result.codigo_servicio, "43")
        self.assertEqual(result.confidence, "low")
        self.assertIn("hasta 50", " ".join(result.rationale).lower())

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_defaults_presentation_to_reca_and_one_company_when_missing(
        self,
        mock_tarifas,
        mock_company_by_nit,
    ) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "1",
                "referencia_servicio": "IL1.PP.B.R.V",
                "descripcion_servicio": "Promocion del Programa de Inclusion Laboral Individual -Gestion empresarial RECA -Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "program_presentation",
                "modalidad_servicio": "Virtual",
                "participantes": [],
            },
            message={"subject": "Presentacion del programa"},
        )

        self.assertEqual(result.codigo_servicio, "1")
        self.assertEqual(result.confidence, "low")
        self.assertEqual(result.observaciones, "")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_uses_multiple_detected_nits_to_pick_group_presentation_bucket(
        self,
        mock_tarifas,
        mock_company_by_nit,
    ) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Sin dato",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "7",
                "referencia_servicio": "IL1.PP2-3.B.R",
                "descripcion_servicio": "Promocion del Programa de Inclusion Laboral 2-3 Empresas - Gestion empresarial RECA- Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "900123456-1, 800123456-2",
                "document_kind": "program_presentation",
                "modalidad_servicio": "Virtual",
                "participantes": [],
            },
            message={"subject": "Presentacion del programa"},
        )

        self.assertEqual(result.codigo_servicio, "7")
        self.assertIn("2 nit", " ".join(result.rationale).lower())

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_formats_vacancy_review_observations_with_cargo_and_vacancies(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "47",
                "referencia_servicio": "IL4.RV.V",
                "descripcion_servicio": "Revision de las Condiciones de la Vacante Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "vacancy_review",
                "modalidad_servicio": "Virtual",
                "cargo_objetivo": "Auxiliar Administrativo",
                "total_vacantes": 3,
                "participantes": [],
            },
            message={"subject": "Revision de condiciones de la vacante"},
        )

        self.assertEqual(result.codigo_servicio, "47")
        self.assertEqual(result.observaciones, "Auxiliar Administrativo (3)")

    @patch("app.automation.rules_engine._company_by_nit")
    @patch("app.automation.rules_engine._tarifas")
    def test_formats_follow_up_observations_with_follow_up_number(self, mock_tarifas, mock_company_by_nit) -> None:
        mock_company_by_nit.return_value = {
            "nombre_empresa": "Empresa Demo",
            "caja_compensacion": "Compensar",
            "ciudad_empresa": "Bogota",
        }
        mock_tarifas.return_value = (
            {
                "codigo_servicio": "83",
                "referencia_servicio": "IL10.SA.V",
                "descripcion_servicio": "Seguimiento y Acompanamiento al Proceso de Inclusion Laboral Virtual",
                "modalidad_servicio": "Virtual",
                "valor_base": 1000,
            },
        )

        result = suggest_service_from_analysis(
            analysis={
                "nit_empresa": "123",
                "document_kind": "follow_up",
                "modalidad_servicio": "Virtual",
                "numero_seguimiento": "3",
                "participantes": [],
            },
            message={"subject": "Seguimiento virtual inclusion laboral"},
        )

        self.assertEqual(result.codigo_servicio, "83")
        self.assertEqual(result.observaciones, "")
        self.assertEqual(result.seguimiento_servicio, "3")


if __name__ == "__main__":
    unittest.main()

