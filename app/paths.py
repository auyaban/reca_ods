import os
import shutil
import sys
from pathlib import Path

APP_NAME = "Sistema de Gestion ODS RECA"
_LEGACY_APP_NAMES = (
    "Sistema de Gesti\u00f3n ODS RECA",
    "Sistema de GestiÃ³n ODS RECA",
    "Sistema de GestiÃƒÂ³n ODS RECA",
)
_TEXT_FALLBACK_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def _appdata_base_dir() -> Path:
    base = os.getenv("APPDATA")
    if base:
        return Path(base)
    return Path.home() / "AppData" / "Roaming"


def _read_text(path: Path) -> str | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None

    for encoding in _TEXT_FALLBACK_ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def _merge_env_file(source: Path, destination: Path) -> None:
    source_text = _read_text(source)
    if source_text is None:
        return

    destination_text = _read_text(destination) if destination.exists() else ""
    merged: dict[str, str] = {}
    ordered_keys: list[str] = []

    def consume(text: str, *, prefer_existing: bool) -> None:
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip()
            if not key:
                continue
            if key not in merged:
                ordered_keys.append(key)
            if prefer_existing and key in merged and merged[key]:
                continue
            if value or key not in merged:
                merged[key] = value

    consume(source_text, prefer_existing=False)
    consume(destination_text or "", prefer_existing=True)

    if not ordered_keys:
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        "\n".join(f"{key}={merged.get(key, '')}" for key in ordered_keys) + "\n",
        encoding="utf-8",
    )


def _merge_legacy_tree(source: Path, destination: Path) -> None:
    if not source.exists() or source == destination:
        return
    for item in source.iterdir():
        dest = destination / item.name
        if item.is_dir():
            dest.mkdir(parents=True, exist_ok=True)
            _merge_legacy_tree(item, dest)
            continue
        if item.name.lower() == ".env":
            _merge_env_file(item, dest)
            continue
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(item, dest)
        except OSError:
            continue


def app_data_dir() -> Path:
    base_dir = _appdata_base_dir()
    canonical = base_dir / APP_NAME
    canonical.mkdir(parents=True, exist_ok=True)
    for legacy_name in _LEGACY_APP_NAMES:
        legacy = base_dir / legacy_name
        if legacy.exists():
            _merge_legacy_tree(legacy, canonical)
    return canonical


def resource_path(relative: str) -> Path:
    if getattr(sys, "_MEIPASS", None):
        return Path(sys._MEIPASS) / relative
    root = Path(__file__).resolve().parents[1]
    return root / relative
