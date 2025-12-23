import os
import sys
from pathlib import Path

APP_NAME = "Sistema de Gestión ODS RECA"


def app_data_dir() -> Path:
    base = os.getenv("APPDATA")
    if base:
        return Path(base) / APP_NAME
    return Path.home() / "AppData" / "Roaming" / APP_NAME


def resource_path(relative: str) -> Path:
    if getattr(sys, "_MEIPASS", None):
        return Path(sys._MEIPASS) / relative
    root = Path(__file__).resolve().parents[1]
    return root / relative
