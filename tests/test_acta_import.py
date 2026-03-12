from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.excel_acta_import import parse_acta_source


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

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    def test_parse_acta_source_supports_google_sheets_url(
        self,
        mock_export_spreadsheet,
        mock_parse_excel,
    ) -> None:
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")

        self.assertEqual(result["source_type"], "google_sheets")
        self.assertEqual(result["file_path"], "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")
        mock_export_spreadsheet.assert_called_once()
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

    def test_parse_acta_source_rejects_unknown_url(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "No se pudo resolver el acta"):
            parse_acta_source("https://someotherdomain.com/file")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_unsupported_drive_file_type(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.pdf",
            "mimeType": "application/pdf",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet ni un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_legacy_xls_drive_file(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.xls",
            "mimeType": "application/vnd.ms-excel",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet ni un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")


if __name__ == "__main__":
    unittest.main()
