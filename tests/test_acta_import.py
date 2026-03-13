from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.excel_acta_import import (
    _extract_pdf_asistentes_candidates,
    _extract_pdf_participants,
    parse_acta_pdf,
    parse_acta_source,
)


class ActaImportTests(unittest.TestCase):
    def test_parse_acta_source_rejects_empty_source(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Debe indicar la ruta o URL del acta"):
            parse_acta_source("")

        with self.assertRaisesRegex(RuntimeError, "Debe indicar la ruta o URL del acta"):
            parse_acta_source(None)

    @patch("app.services.excel_acta_import.parse_acta_excel")
    def test_parse_acta_source_uses_local_file(self, mock_parse_excel) -> None:
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_source(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900123456")
        mock_parse_excel.assert_called_once_with(str(temp_path))

    @patch("app.services.excel_acta_import.parse_acta_pdf")
    def test_parse_acta_source_uses_local_pdf(self, mock_parse_pdf) -> None:
        mock_parse_pdf.return_value = {"nit_empresa": "900123456"}

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_source(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900123456")
        mock_parse_pdf.assert_called_once_with(str(temp_path))

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_google_sheets_url(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "sheet-123",
            "name": "Acta marzo",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")

        self.assertEqual(result["source_type"], "google_sheets")
        self.assertEqual(result["file_path"], "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")
        mock_export_spreadsheet.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_google_spreadsheets_url_backed_by_excel_file(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_download,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "sheet-123",
            "name": "Acta marzo.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://docs.google.com/spreadsheets/d/sheet-123/edit?usp=drivesdk")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_export_spreadsheet.assert_not_called()
        mock_download.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_google_sheet_link(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_sheets")
        mock_export_spreadsheet.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_excel_link(
        self,
        mock_get_metadata,
        mock_download,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_download.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_pdf")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_pdf_link(
        self,
        mock_get_metadata,
        mock_download,
        mock_parse_pdf,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.pdf",
            "mimeType": "application/pdf",
        }
        mock_parse_pdf.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_download.assert_called_once()
        mock_parse_pdf.assert_called_once()

    def test_parse_acta_source_rejects_unknown_url(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "No se pudo resolver el acta"):
            parse_acta_source("https://someotherdomain.com/file")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_unsupported_drive_file_type(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.docx",
            "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet, un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_legacy_xls_drive_file(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.xls",
            "mimeType": "application/vnd.ms-excel",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet, un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")

    def test_extract_pdf_participants_reads_joined_rows(self) -> None:
        text = (
            "1 Leydi Marcela Ávila Ardila107292221470.00%Discapacidad física316 6253584Pendiente "
            "Agente de CatálogoMartha Aurora Ardila RiosMadre 3223997748 15/06/1995 30 años.\n"
            "2 Edward Mauricio Riaño Zamora8075054626.30%Discapacidad física310 2691234Pendiente "
            "Agente de CatálogoLeonardo ZamoraHermano 320 4167513 28/07/1985 40 años."
        )

        participants = _extract_pdf_participants(text)

        self.assertEqual(
            participants,
            [
                {
                    "nombre_usuario": "Leydi Marcela Ávila Ardila",
                    "cedula_usuario": "1072922214",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
                {
                    "nombre_usuario": "Edward Mauricio Riaño Zamora",
                    "cedula_usuario": "80750546",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
            ],
        )


    def test_extract_pdf_asistentes_candidates_prefers_nombre_completo_order(self) -> None:
        text = (
            "3. Asistentes\n"
            "Nombre completo: Gabriela Rubiano Isaza Cargo: Profesional de inclusion laboral\n"
            "Nombre completo: Lissette Lorena Castaneda Cargo: Psicologa\n"
            "La presente acta deja constancia del proceso.\n"
        )

        candidates = _extract_pdf_asistentes_candidates(text)

        self.assertEqual(
            candidates,
            [
                "Gabriela Rubiano Isaza",
                "Lissette Lorena Castaneda",
            ],
        )

    def test_extract_pdf_asistentes_candidates_reads_multiline_asistentes_block(self) -> None:
        text = (
            "8.ASISTENTES\n"
            "Nombre completo: Sandra Milena Pachon Rojas\n"
            "Nombre completo: Ana Maria Malagon\n"
            "Coordinacion de inclusion laboral\n"
            "Lider Desarrollo Talento\n"
            "Nombre completo: Francia Palacios\n"
        )

        candidates = _extract_pdf_asistentes_candidates(text)

        self.assertEqual(
            candidates,
            [
                "Sandra Milena Pachon Rojas",
                "Ana Maria Malagon",
                "Francia Palacios",
            ],
        )

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_layout_with_values_before_labels(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "1.DATOS GENERALES",
                    "Número de NIT: 900439301-2",
                    "05/03/2026 Modalidad: Virtual",
                    "INVERSIONES INT COLOMBIA SAS Ciudad/Municipio: Bogotá",
                    "Cra 22 # 83 - 31",
                    "Fecha de la Visita:",
                    "Nombre de la Empresa:",
                    "Dirección de la Empresa:",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900439301-2")
        self.assertEqual(result["fecha_servicio"], "2026-03-05")
        self.assertEqual(result["modalidad_servicio"], "Virtual")
        self.assertEqual(result["nombre_empresa"], "INVERSIONES INT COLOMBIA SAS")
        self.assertNotIn("No se detecto nombre de empresa en el PDF.", result["warnings"])
        self.assertNotIn("No se detecto fecha de servicio en formato valido.", result["warnings"])


if __name__ == "__main__":
    unittest.main()
