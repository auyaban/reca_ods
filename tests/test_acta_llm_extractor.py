from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.acta_llm_extractor import (
    _prepare_llm_source_text,
    extract_structured_acta_pdf,
    get_acta_llm_schema,
)


class ActaLlmExtractorTests(unittest.TestCase):
    def test_schema_exposes_expected_required_fields(self) -> None:
        schema = get_acta_llm_schema()
        required = set(schema["schema"]["required"])
        self.assertIn("nit_empresa", required)
        self.assertIn("participantes", required)
        self.assertIn("warnings", required)

    def test_truncates_long_text_sections_to_300_characters(self) -> None:
        source_text = """1. DATOS DE LA EMPRESA
Nombre de la empresa
Observaciones
""" + ("A" * 450) + """
2. DATOS DEL OFERENTE
Nombre completo
"""

        prepared = _prepare_llm_source_text([source_text])

        self.assertIn("Observaciones", prepared)
        self.assertIn("2. DATOS DEL OFERENTE", prepared)
        self.assertNotIn("A" * 350, prepared)
        self.assertIn(("A" * 300) + "...", prepared)

    def test_filters_sections_using_process_profile(self) -> None:
        source_text = """1. DATOS GENERALES
Fecha de la Visita: 13/03/2026
2. CARACTERISTICAS DE LA VACANTE
Nombre de la vacante: Profesional Administrativo
6. LA VACANTE ES ACCESIBLE Y COMPATIBLE PARA PERSONAS CON...
Texto muy largo que no deberia enviarse
8.ASISTENTES
Nombre completo: Adriana Gonzalez Moreno
"""

        prepared = _prepare_llm_source_text([source_text], document_kind="vacancy_review")

        self.assertIn("1. DATOS GENERALES", prepared)
        self.assertIn("2. CARACTERISTICAS DE LA VACANTE", prepared)
        self.assertIn("8.ASISTENTES", prepared)
        self.assertNotIn("Texto muy largo que no deberia enviarse", prepared)

    @patch("app.services.acta_llm_extractor._extract_pdf_text_pages")
    @patch("app.services.acta_llm_extractor._save_llm_json")
    @patch("app.services.acta_llm_extractor._invoke_edge_function_http")
    @patch("app.services.acta_llm_extractor.parse_acta_pdf")
    @patch("app.services.acta_llm_extractor.get_settings")
    def test_uses_llm_edge_response_when_enabled(
        self,
        mock_get_settings,
        mock_parse_acta_pdf,
        mock_invoke_edge,
        mock_save_llm_json,
        mock_extract_pdf_text_pages,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            automation_llm_extraction_enabled=True,
            supabase_edge_acta_extraction_function="extract-acta-ods",
        )
        mock_parse_acta_pdf.return_value = {"warnings": []}
        mock_extract_pdf_text_pages.return_value = ["Texto extraido del PDF"]
        mock_save_llm_json.return_value = "C:/Users/aaron/Desktop/JSONs/demo.json"
        mock_invoke_edge.return_value = {
            "data": {
                "schema_version": "v1",
                "extraction_status": "ok",
                "document_type_hint": "inclusive_selection",
                "process_name_hint": "seleccion_incluyente",
                "nit_empresa": "901024978-1",
                "nits_empresas": ["901024978-1"],
                "nombre_empresa": "GALLAGHER CONSULTING LTDA",
                "fecha_servicio": "2026-03-02",
                "nombre_profesional": "Leidy Novoa",
                "modalidad_servicio": "Virtual",
                "gestion_empresarial": "",
                "tamano_empresa": "",
                "total_empresas": 1,
                "is_fallido": False,
                "total_horas_interprete": 0,
                "sumatoria_horas_interpretes": 0,
                "participantes": [
                    {
                        "nombre_usuario": "Angie Lorena Avellaneda Chaparro",
                        "cedula_usuario": "1034657640",
                        "discapacidad_usuario": "visual baja vision",
                        "genero_usuario": "",
                    }
                ],
                "warnings": [],
            }
        }

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = extract_structured_acta_pdf(str(temp_path), filename="demo.pdf", subject="Correo demo")
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertTrue(result["llm_extraction_used"])
        self.assertEqual(result["llm_extraction_status"], "ok")
        self.assertEqual(result["nombre_profesional"], "Leidy Novoa")
        self.assertEqual(result["participantes"][0]["cedula_usuario"], "1034657640")

    @patch("app.services.acta_llm_extractor._extract_pdf_text_pages")
    @patch("app.services.acta_llm_extractor._save_llm_json")
    @patch("app.services.acta_llm_extractor._invoke_edge_function_http")
    @patch("app.services.acta_llm_extractor.parse_acta_pdf")
    @patch("app.services.acta_llm_extractor.get_settings")
    def test_sends_pdf_and_instruction_context_to_edge_function(
        self,
        mock_get_settings,
        mock_parse_acta_pdf,
        mock_invoke_edge,
        mock_save_llm_json,
        mock_extract_pdf_text_pages,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            automation_llm_extraction_enabled=True,
            supabase_edge_acta_extraction_function="extract-acta-ods",
        )
        mock_parse_acta_pdf.return_value = {"warnings": []}
        mock_extract_pdf_text_pages.return_value = [
            "Observaciones\n" + ("B" * 500) + "\n2. DATOS DEL OFERENTE\nNombre completo"
        ]
        mock_save_llm_json.return_value = "C:/Users/aaron/Desktop/JSONs/demo.json"
        mock_invoke_edge.return_value = {
            "data": {
                "schema_version": "v1",
                "extraction_status": "ok",
                "document_type_hint": "inclusive_selection",
                "process_name_hint": "seleccion_incluyente",
                "nit_empresa": "",
                "nits_empresas": [],
                "nombre_empresa": "",
                "fecha_servicio": "",
                "nombre_profesional": "",
                "interpretes": [],
                "asistentes": [],
                "modalidad_servicio": "",
                "gestion_empresarial": "",
                "tamano_empresa": "",
                "cargo_objetivo": "",
                "total_vacantes": 0,
                "numero_seguimiento": "",
                "total_empresas": 0,
                "is_fallido": False,
                "total_horas_interprete": 0,
                "sumatoria_horas_interpretes": 0,
                "participantes": [],
                "warnings": [],
            }
        }

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            extract_structured_acta_pdf(
                str(temp_path),
                filename="PROCESO DE SELECCIÓN INCLUYENTE demo.pdf",
                subject="Proceso de selección incluyente demo",
            )
        finally:
            if temp_path.exists():
                temp_path.unlink()

        payload = mock_invoke_edge.call_args.args[1]
        sent_text = payload["text"]
        self.assertIn("document_kind_hint:", sent_text)
        self.assertIn("perfil_documento:", sent_text)
        self.assertIn("guia_extraccion_especifica:", sent_text)
        self.assertNotIn(("B" * 300) + "...", sent_text)
        self.assertIn("pdf_base64", payload)

    @patch("app.services.acta_llm_extractor._extract_pdf_text_pages")
    @patch("app.services.acta_llm_extractor._save_llm_json")
    @patch("app.services.acta_llm_extractor._invoke_edge_function_http")
    @patch("app.services.acta_llm_extractor.parse_acta_pdf")
    @patch("app.services.acta_llm_extractor.get_settings")
    def test_omits_forbidden_fields_from_profile_after_llm_response(
        self,
        mock_get_settings,
        mock_parse_acta_pdf,
        mock_invoke_edge,
        mock_save_llm_json,
        mock_extract_pdf_text_pages,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            automation_llm_extraction_enabled=True,
            supabase_edge_acta_extraction_function="extract-acta-ods",
        )
        mock_parse_acta_pdf.return_value = {"warnings": []}
        mock_extract_pdf_text_pages.return_value = [
            "REVISION DE LAS CONDICIONES DE LA VACANTE\n1. DATOS GENERALES\nFecha de la Visita: 13/03/2026"
        ]
        mock_save_llm_json.return_value = "C:/Users/aaron/Desktop/JSONs/demo.json"
        mock_invoke_edge.return_value = {
            "data": {
                "schema_version": "v1",
                "extraction_status": "ok",
                "document_type_hint": "vacancy_review",
                "process_name_hint": "revision_condiciones_vacante",
                "nit_empresa": "860002184",
                "nits_empresas": ["860002184"],
                "nombre_empresa": "AXA COLPATRIA SEGUROS S.A.",
                "fecha_servicio": "2026-03-13",
                "nombre_profesional": "Adriana Gonzalez Moreno",
                "interpretes": [],
                "asistentes": ["Adriana Gonzalez Moreno"],
                "modalidad_servicio": "Hibrida",
                "gestion_empresarial": "",
                "tamano_empresa": "",
                "cargo_objetivo": "Profesional Administrativo",
                "total_vacantes": 2,
                "numero_seguimiento": "2025-0485-CON-23069",
                "total_empresas": 0,
                "is_fallido": False,
                "total_horas_interprete": 0,
                "sumatoria_horas_interpretes": 0,
                "participantes": [],
                "warnings": [],
            }
        }

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = extract_structured_acta_pdf(str(temp_path), filename="revision_vacante.pdf", subject="Revision vacante")
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["numero_seguimiento"], "")
        self.assertIn("Campo omitido por perfil del documento", " ".join(result["warnings"]))
        self.assertTrue(result["llm_review_required"])

    @patch("app.services.acta_llm_extractor._extract_pdf_text_pages")
    @patch("app.services.acta_llm_extractor._save_llm_json")
    @patch("app.services.acta_llm_extractor._invoke_edge_function_http")
    @patch("app.services.acta_llm_extractor.parse_acta_pdf")
    @patch("app.services.acta_llm_extractor.get_settings")
    def test_falls_back_to_local_parser_when_edge_call_fails(
        self,
        mock_get_settings,
        mock_parse_acta_pdf,
        mock_invoke_edge,
        mock_save_llm_json,
        mock_extract_pdf_text_pages,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            automation_llm_extraction_enabled=True,
            supabase_edge_acta_extraction_function="extract-acta-ods",
        )
        mock_parse_acta_pdf.return_value = {
            "nombre_empresa": "Empresa Demo",
            "warnings": [],
        }
        mock_extract_pdf_text_pages.return_value = ["Texto extraido del PDF"]
        mock_save_llm_json.return_value = "C:/Users/aaron/Desktop/JSONs/demo.json"
        mock_invoke_edge.side_effect = RuntimeError("Function not found")

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = extract_structured_acta_pdf(str(temp_path), filename="demo.pdf", subject="Correo demo")
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertFalse(result["llm_extraction_used"])
        self.assertEqual(result["llm_extraction_status"], "fallback_local")
        self.assertIn("No se pudo usar extraccion LLM", " ".join(result["warnings"]))


if __name__ == "__main__":
    unittest.main()

