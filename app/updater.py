import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

import requests

from app.version import get_version

REPO_OWNER = "auyaban"
REPO_NAME = "reca_ods"
INSTALLER_ASSET = "RECA_ODS_Setup.exe"
HASH_ASSET = "RECA_ODS_Setup.exe.sha256"


def _parse_version(value: str) -> tuple[int, int, int]:
    clean = value.strip().lower().lstrip("v")
    parts = clean.split(".")
    nums = [int(p) if p.isdigit() else 0 for p in parts[:3]]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)


def _is_newer(remote: str, local: str) -> bool:
    return _parse_version(remote) > _parse_version(local)


def _download(url: str, target: Path, progress_callback=None) -> None:
    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()
        total = response.headers.get("Content-Length")
        total_size = int(total) if total and total.isdigit() else 0
        downloaded = 0
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    handle.write(chunk)
                    if total_size and progress_callback:
                        downloaded += len(chunk)
                        percent = int((downloaded / total_size) * 100)
                        progress_callback(min(percent, 100))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _get_latest_release() -> tuple[str | None, dict]:
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
    response = requests.get(api_url, timeout=15)
    if response.status_code >= 400:
        return None, {}
    data = response.json()
    remote_version = str(data.get("tag_name", "")).lstrip("v")
    assets = {asset["name"]: asset["browser_download_url"] for asset in data.get("assets", [])}
    return remote_version or None, assets


def get_latest_version() -> str | None:
    remote_version, _ = _get_latest_release()
    return remote_version


def check_and_update(status_callback=None, progress_callback=None, version_callback=None) -> bool:
    local_version = get_version()
    remote_version, assets = _get_latest_release()
    if version_callback:
        version_callback(local_version, remote_version)
    if not remote_version or not _is_newer(remote_version, local_version):
        return False

    installer_url = assets.get(INSTALLER_ASSET)
    hash_url = assets.get(HASH_ASSET)
    if not installer_url or not hash_url:
        return False

    if status_callback:
        status_callback("Descargando actualizacion...")
    if progress_callback:
        progress_callback(0)

    temp_dir = Path(tempfile.mkdtemp(prefix="reca_ods_update_"))
    installer_path = temp_dir / INSTALLER_ASSET
    hash_path = temp_dir / HASH_ASSET

    _download(installer_url, installer_path, progress_callback=progress_callback)
    if progress_callback:
        progress_callback(70)
    _download(hash_url, hash_path, progress_callback=None)

    expected = hash_path.read_text(encoding="utf-8").strip().split()[0]
    actual = _sha256(installer_path)
    if expected.lower() != actual.lower():
        return False

    if status_callback:
        status_callback("Iniciando instalador...")
    if progress_callback:
        progress_callback(None)

    args = [
        str(installer_path),
        "/VERYSILENT",
        "/SUPPRESSMSGBOXES",
        "/NORESTART",
        "/SP-",
    ]
    result = subprocess.run(args, cwd=str(temp_dir), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode != 0:
        return False

    if status_callback:
        status_callback("Reiniciando aplicacion...")
    if progress_callback:
        progress_callback(100)
    return True
