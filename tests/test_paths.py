from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app import paths


class PathsTests(unittest.TestCase):
    def test_merge_env_file_normalizes_legacy_google_service_account_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "legacy.env"
            destination = Path(tmpdir) / "canonical.env"
            source.write_text(
                "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gesti\u00f3n ODS RECA\\secrets\\google-service-account.json\n",
                encoding="utf-8",
            )

            paths._merge_env_file(source, destination)

            self.assertEqual(
                destination.read_text(encoding="utf-8"),
                "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gestion ODS RECA\\secrets\\google-service-account.json\n",
            )

    def test_merge_env_file_preserves_existing_canonical_value_over_legacy_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "legacy.env"
            destination = Path(tmpdir) / "canonical.env"
            source.write_text(
                (
                    "SUPABASE_URL=https://legacy.supabase.co\n"
                    "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gesti\u00f3n ODS RECA\\secrets\\google-service-account.json\n"
                ),
                encoding="utf-8",
            )
            destination.write_text(
                (
                    "SUPABASE_URL=https://canonical.supabase.co\n"
                    "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gestion ODS RECA\\secrets\\google-service-account.json\n"
                ),
                encoding="utf-8",
            )

            paths._merge_env_file(source, destination)

            self.assertEqual(
                destination.read_text(encoding="utf-8"),
                (
                    "SUPABASE_URL=https://canonical.supabase.co\n"
                    "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gestion ODS RECA\\secrets\\google-service-account.json\n"
                ),
            )


if __name__ == "__main__":
    unittest.main()
