import hashlib
import os
import subprocess
import tempfile
from pathlib import Path

import requests

REPO_OWNER = "auyaban"
REPO_NAME = "reca_ods"
INSTALLER_ASSET = "RECA_ODS_Setup.exe"
HASH_ASSET = "RECA_ODS_Setup.exe.sha256"
APPDATA_DIRNAME = "Sistema de Gestión ODS RECA"


def _update_log_path() -> Path:
    base = os.getenv("APPDATA")
    if base:
        log_dir = Path(base) / APPDATA_DIRNAME / "logs"
    else:
        log_dir = Path.home() / "AppData" / "Roaming" / APPDATA_DIRNAME / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "updater.log"


def _log_update(message: str) -> None:
    try:
        with _update_log_path().open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip() + "\n")
    except Exception:
        pass


def _get_latest_release() -> tuple[str | None, dict]:
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
    response = requests.get(api_url, timeout=15)
    if response.status_code >= 400:
        _log_update(f"ERROR obtener release: status={response.status_code}")
        return None, {}
    data = response.json()
    remote_version = str(data.get("tag_name", "")).lstrip("v")
    assets = {asset["name"]: asset["browser_download_url"] for asset in data.get("assets", [])}
    return remote_version or None, assets


def get_latest_version() -> str | None:
    remote_version, _ = _get_latest_release()
    return remote_version


def _parse_version(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    cleaned = value.strip().lstrip("v")
    parts = []
    for chunk in cleaned.split("."):
        try:
            parts.append(int(chunk))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def is_update_available(local_version: str | None, remote_version: str | None) -> bool:
    if not local_version or not remote_version:
        return False
    return _parse_version(remote_version) > _parse_version(local_version)


def get_latest_release_assets() -> tuple[str | None, dict]:
    return _get_latest_release()


def _download_file(url: str, destination: Path, progress_callback=None) -> None:
    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length") or 0)
        downloaded = 0
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                handle.write(chunk)
                downloaded += len(chunk)
                if progress_callback and total:
                    percent = int((downloaded / total) * 100)
                    progress_callback("Descargando instalador...", percent)


def _verify_hash(installer_path: Path, assets: dict) -> None:
    url = assets.get(HASH_ASSET)
    if not url:
        return
    hash_path = installer_path.with_suffix(".sha256")
    _download_file(url, hash_path)
    expected = hash_path.read_text(encoding="utf-8").strip().split()[0]
    digest = hashlib.sha256(installer_path.read_bytes()).hexdigest()
    if expected and digest.lower() != expected.lower():
        raise RuntimeError("Hash del instalador no coincide.")


def download_installer(assets: dict, progress_callback=None) -> Path:
    url = assets.get(INSTALLER_ASSET)
    if not url:
        raise RuntimeError("No se encontro el instalador en el release.")
    installer_path = Path(tempfile.gettempdir()) / INSTALLER_ASSET
    _download_file(url, installer_path, progress_callback)
    _verify_hash(installer_path, assets)
    return installer_path


def run_installer(installer_path: Path, wait: bool = True) -> None:
    args = [
        str(installer_path),
        "/VERYSILENT",
        "/CURRENTUSER",
        "/SUPPRESSMSGBOXES",
        "/NORESTART",
    ]
    if wait:
        subprocess.run(args, check=False)
    else:
        subprocess.Popen(args, close_fds=True)
