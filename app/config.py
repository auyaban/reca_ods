from functools import lru_cache
import os

from pathlib import Path

from app.paths import app_data_dir
from app.utils.cache import ttl_bucket

_ENV_PATH = app_data_dir() / ".env"
_SETTINGS_CACHE_TTL_SECONDS = 300
_DEFAULT_SUPABASE_AUTH_EMAIL = ""
_DEFAULT_SUPABASE_AUTH_PASSWORD = ""
_DEFAULT_AUTOMATION_TEST_SPREADSHEET_ID = ""
_DEFAULT_AUTOMATION_TEST_SHEET_NAME = "ODS_INPUT"
_LEGACY_SUPABASE_AUTH_PASSWORDS: tuple[str, ...] = ()
_ENV_FALLBACK_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def _read_env_text(path: Path) -> tuple[str, str] | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None

    for encoding in _ENV_FALLBACK_ENCODINGS:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace"), "latin-1"


def _rewrite_env_utf8(path: Path, text: str, source_encoding: str) -> None:
    if source_encoding in {"utf-8", "utf-8-sig"}:
        return
    try:
        path.write_text(text, encoding="utf-8")
    except OSError:
        return


def _load_env_file(path: Path, *, override: bool) -> None:
    if not path.exists():
        return

    loaded = _read_env_text(path)
    if loaded is None:
        return
    raw_text, source_encoding = loaded
    _rewrite_env_utf8(path, raw_text, source_encoding)
    raw_lines = raw_text.splitlines()

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip()
        if not key or value == "":
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = value


def _load_env() -> None:
    _load_env_file(_ENV_PATH, override=True)
    fallback = Path(__file__).resolve().parents[1] / ".env"
    _load_env_file(fallback, override=False)


_load_env()


def _clean_env(value: str) -> str:
    clean = value.strip()
    if (clean.startswith('"') and clean.endswith('"')) or (
        clean.startswith("'") and clean.endswith("'")
    ):
        clean = clean[1:-1].strip()
    return clean


def _env_or_default(key: str, default: str) -> str:
    raw = os.getenv(key)
    if raw is None:
        return default
    clean = _clean_env(raw)
    if clean == "":
        return default
    if key == "SUPABASE_AUTH_PASSWORD" and clean in _LEGACY_SUPABASE_AUTH_PASSWORDS:
        return _DEFAULT_SUPABASE_AUTH_PASSWORD
    return clean


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    clean = _clean_env(raw).lower()
    if clean == "":
        return default
    return clean in {"1", "true", "t", "yes", "y", "si", "sí", "on"}


def _env_csv(key: str) -> tuple[str, ...]:
    raw = os.getenv(key)
    if raw is None:
        return ()
    clean = _clean_env(raw)
    if not clean:
        return ()
    values = [item.strip() for item in clean.split(",")]
    return _unique_ordered([item for item in values if item])


def _unique_ordered(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


class Settings:
    def __init__(self) -> None:
        self.supabase_url = _clean_env(os.getenv("SUPABASE_URL", ""))
        self.supabase_anon_key = _clean_env(os.getenv("SUPABASE_ANON_KEY", ""))
        self.supabase_auth_email = _env_or_default(
            "SUPABASE_AUTH_EMAIL", _DEFAULT_SUPABASE_AUTH_EMAIL
        )
        self.supabase_auth_password = _env_or_default(
            "SUPABASE_AUTH_PASSWORD", _DEFAULT_SUPABASE_AUTH_PASSWORD
        )
        self.supabase_auth_password_candidates = _unique_ordered(
            [
                self.supabase_auth_password,
                _DEFAULT_SUPABASE_AUTH_PASSWORD,
                *_LEGACY_SUPABASE_AUTH_PASSWORDS,
            ]
        )
        self.supabase_rpc_terminar_servicio = _clean_env(
            os.getenv("SUPABASE_RPC_TERMINAR_SERVICIO", "")
        )
        self.google_service_account_file = _clean_env(
            os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
        )
        self.google_sheets_default_spreadsheet_id = _clean_env(
            os.getenv("GOOGLE_SHEETS_DEFAULT_SPREADSHEET_ID", "")
        )
        self.google_drive_shared_folder_id = _clean_env(
            os.getenv("GOOGLE_DRIVE_SHARED_FOLDER_ID", "")
        )
        self.google_drive_template_spreadsheet_name = _clean_env(
            os.getenv("GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME", "")
        )
        self.google_gmail_delegated_user = _clean_env(
            os.getenv("GOOGLE_GMAIL_DELEGATED_USER", "")
        )
        self.google_gmail_to_filter = _clean_env(
            os.getenv("GOOGLE_GMAIL_TO_FILTER", "")
        )
        try:
            self.google_gmail_fetch_limit = int(_clean_env(os.getenv("GOOGLE_GMAIL_FETCH_LIMIT", "20")) or "20")
        except ValueError:
            self.google_gmail_fetch_limit = 20
        self.google_sheets_automation_test_spreadsheet_id = _env_or_default(
            "GOOGLE_SHEETS_AUTOMATION_TEST_SPREADSHEET_ID",
            _DEFAULT_AUTOMATION_TEST_SPREADSHEET_ID,
        )
        self.google_sheets_automation_test_sheet_name = _env_or_default(
            "GOOGLE_SHEETS_AUTOMATION_TEST_SHEET_NAME",
            _DEFAULT_AUTOMATION_TEST_SHEET_NAME,
        )
        self.automation_decisions_log_path = _clean_env(
            os.getenv("AUTOMATION_DECISIONS_LOG_PATH", "")
        )
        self.automation_llm_extraction_enabled = _env_bool(
            "AUTOMATION_LLM_EXTRACTION_ENABLED", False
        )
        self.supabase_edge_acta_extraction_function = _env_or_default(
            "SUPABASE_EDGE_ACTA_EXTRACTION_FUNCTION",
            "extract-acta-ods",
        )
        self.supabase_edge_acta_extraction_secret = _clean_env(
            os.getenv("SUPABASE_EDGE_ACTA_EXTRACTION_SECRET", "")
        )
        self.ods_automation_test_enabled = _env_bool("ODS_AUTOMATION_TEST_ENABLED", False)
        self.ods_automation_test_users = _env_csv("ODS_AUTOMATION_TEST_USERS")
        self.automation_process_templates_dir = _clean_env(
            os.getenv("AUTOMATION_PROCESS_TEMPLATES_DIR", "")
        )


@lru_cache
def _get_settings_cached(_ttl_bucket: int) -> Settings:
    _load_env()
    return Settings()


def get_settings() -> Settings:
    return _get_settings_cached(ttl_bucket(_SETTINGS_CACHE_TTL_SECONDS))


def clear_settings_cache(reload_env: bool = True) -> None:
    _get_settings_cached.cache_clear()
    if reload_env:
        _load_env()


def persist_supabase_auth_credentials(email: str, password: str) -> None:
    lines: list[str] = []
    existing: list[str] = []
    if _ENV_PATH.exists():
        loaded = _read_env_text(_ENV_PATH)
        if loaded is not None:
            existing = loaded[0].splitlines()

    updated_email = False
    updated_password = False
    for line in existing:
        if line.startswith("SUPABASE_AUTH_EMAIL="):
            lines.append(f"SUPABASE_AUTH_EMAIL={email}")
            updated_email = True
            continue
        if line.startswith("SUPABASE_AUTH_PASSWORD="):
            lines.append(f"SUPABASE_AUTH_PASSWORD={password}")
            updated_password = True
            continue
        lines.append(line)

    if not updated_email:
        lines.append(f"SUPABASE_AUTH_EMAIL={email}")
    if not updated_password:
        lines.append(f"SUPABASE_AUTH_PASSWORD={password}")

    _ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
    _ENV_PATH.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    clear_settings_cache(reload_env=True)
