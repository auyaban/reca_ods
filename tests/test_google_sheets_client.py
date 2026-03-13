from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.google_sheets_client import _credentials_path, extract_drive_file_id, normalize_google_file_open_url


class GoogleSheetsClientTests(unittest.TestCase):
    def test_credentials_path_expands_windows_env_vars(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            secrets_dir = Path(tmpdir) / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            credential_file = secrets_dir / "google-service-account.json"
            credential_file.write_text("{}", encoding="utf-8")

            with patch.dict(os.environ, {"APPDATA": tmpdir}, clear=False):
                with patch(
                    "app.google_sheets_client.get_settings",
                    return_value=SimpleNamespace(
                        google_service_account_file=r"%APPDATA%\secrets\google-service-account.json"
                    ),
                ):
                    path = _credentials_path()

        self.assertEqual(path, credential_file)

    def test_extract_drive_file_id_supports_google_docs_urls(self) -> None:
        file_id = extract_drive_file_id("https://docs.google.com/spreadsheets/d/sheet-123/edit?usp=drivesdk")

        self.assertEqual(file_id, "sheet-123")

    @patch("app.google_sheets_client.get_drive_file_metadata")
    def test_normalize_google_file_open_url_uses_sheet_editor_for_native_sheets(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {"mimeType": "application/vnd.google-apps.spreadsheet"}

        url = normalize_google_file_open_url("https://docs.google.com/spreadsheets/d/sheet-123/edit?usp=drivesdk")

        self.assertEqual(url, "https://docs.google.com/spreadsheets/d/sheet-123/edit")

    @patch("app.google_sheets_client.get_drive_file_metadata")
    def test_normalize_google_file_open_url_uses_drive_preview_for_excel_files(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {"mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}

        url = normalize_google_file_open_url("https://docs.google.com/spreadsheets/d/sheet-123/edit?usp=drivesdk")

        self.assertEqual(url, "https://drive.google.com/file/d/sheet-123/view")


if __name__ == "__main__":
    unittest.main()
