from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import config


class ConfigEnvEncodingTests(unittest.TestCase):
    def test_get_settings_loads_cp1252_env_normalizes_paths_and_rewrites_utf8(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_text = (
                "SUPABASE_URL=https://example.supabase.co\n"
                "SUPABASE_ANON_KEY=test-key\n"
                "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gesti\u00f3n ODS RECA\\secrets\\google-service-account.json\n"
            )
            env_path.write_bytes(env_text.encode("cp1252"))

            keys = [
                "SUPABASE_URL",
                "SUPABASE_ANON_KEY",
                "GOOGLE_SERVICE_ACCOUNT_FILE",
            ]
            original = {key: os.environ.get(key) for key in keys}
            for key in keys:
                os.environ.pop(key, None)

            try:
                with patch.object(config, "_ENV_PATH", env_path):
                    config.clear_settings_cache(reload_env=True)
                    settings = config.get_settings()
            finally:
                for key, value in original.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
                config.clear_settings_cache(reload_env=False)

            self.assertEqual(settings.supabase_url, "https://example.supabase.co")
            self.assertEqual(settings.supabase_anon_key, "test-key")
            self.assertEqual(
                settings.google_service_account_file,
                "%APPDATA%\\Sistema de Gestion ODS RECA\\secrets\\google-service-account.json",
            )
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                (
                    "SUPABASE_URL=https://example.supabase.co\n"
                    "SUPABASE_ANON_KEY=test-key\n"
                    "GOOGLE_SERVICE_ACCOUNT_FILE=%APPDATA%\\Sistema de Gestion ODS RECA\\secrets\\google-service-account.json\n"
                ),
            )


if __name__ == "__main__":
    unittest.main()
