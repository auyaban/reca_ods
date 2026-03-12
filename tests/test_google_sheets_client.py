from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.google_sheets_client import _credentials_path


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


if __name__ == "__main__":
    unittest.main()
