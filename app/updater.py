import hashlib
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import requests

from app.logging_utils import LOGGER_UPDATER, get_file_logger
from app.paths import app_data_dir

REPO_OWNER = "auyaban"
REPO_NAME = "reca_ods"
INSTALLER_ASSET = "RECA_ODS_Setup.exe"
HASH_ASSET = "RECA_ODS_Setup.exe.sha256"

_LOGGER = get_file_logger(LOGGER_UPDATER, app_data_dir() / "logs" / "updater.log")


def _new_op_id() -> str:
    return uuid4().hex[:8]


def _log_update(message: str, *, context: str = "updater", op_id: str | None = None, level: str = "info") -> None:
    op_id = op_id or _new_op_id()
    log_fn = getattr(_LOGGER, level, _LOGGER.info)
    log_fn(f"[ctx={context} op={op_id}] {message.rstrip()}")


def _get_latest_release() -> tuple[str | None, dict]:
    op_id = _new_op_id()
    context = "release.latest"
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
    response = requests.get(api_url, timeout=15)
    if response.status_code >= 400:
        _log_update(
            f"ERROR obtener release: status={response.status_code}",
            context=context,
            op_id=op_id,
            level="error",
        )
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
    op_id = _new_op_id()
    context = "asset.download"
    _log_update(f"Iniciando descarga url={url} destino={destination}", context=context, op_id=op_id)
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
    _log_update("Descarga completada", context=context, op_id=op_id)


def _verify_hash(installer_path: Path, assets: dict) -> None:
    op_id = _new_op_id()
    context = "asset.verify_hash"
    url = assets.get(HASH_ASSET)
    if not url:
        _log_update("No hay hash publicado; se omite verificacion.", context=context, op_id=op_id, level="warning")
        return
    hash_path = installer_path.with_suffix(".sha256")
    _download_file(url, hash_path)
    expected = hash_path.read_text(encoding="utf-8").strip().split()[0]
    digest = hashlib.sha256(installer_path.read_bytes()).hexdigest()
    if expected and digest.lower() != expected.lower():
        _log_update("Hash del instalador no coincide.", context=context, op_id=op_id, level="error")
        raise RuntimeError("Hash del instalador no coincide.")
    _log_update("Hash verificado correctamente.", context=context, op_id=op_id)


def download_installer(assets: dict, progress_callback=None) -> Path:
    op_id = _new_op_id()
    context = "installer.download"
    url = assets.get(INSTALLER_ASSET)
    if not url:
        _log_update("No se encontro el instalador en el release.", context=context, op_id=op_id, level="error")
        raise RuntimeError("No se encontro el instalador en el release.")
    installer_path = Path(tempfile.gettempdir()) / INSTALLER_ASSET
    _log_update(f"Destino instalador temporal: {installer_path}", context=context, op_id=op_id)
    _download_file(url, installer_path, progress_callback)
    _verify_hash(installer_path, assets)
    _log_update("Instalador descargado y validado.", context=context, op_id=op_id)
    return installer_path


def run_installer(installer_path: Path, wait: bool = True) -> None:
    op_id = _new_op_id()
    context = "installer.run"
    args = [
        str(installer_path),
        "/VERYSILENT",
        "/CURRENTUSER",
        "/SUPPRESSMSGBOXES",
        "/NORESTART",
    ]
    if wait:
        _log_update(f"Ejecutando instalador (wait=True): {' '.join(args)}", context=context, op_id=op_id)
        completed = subprocess.run(args, check=False)
        if completed.returncode != 0:
            _log_update(
                f"ERROR instalador returncode={completed.returncode}",
                context=context,
                op_id=op_id,
                level="error",
            )
            raise RuntimeError(f"La instalacion finalizo con codigo {completed.returncode}.")
        _log_update("Instalador finalizado correctamente.", context=context, op_id=op_id)
    else:
        _log_update(f"Ejecutando instalador (wait=False): {' '.join(args)}", context=context, op_id=op_id)
        process = subprocess.Popen(args, close_fds=True)
        if process.pid is None:
            _log_update("ERROR instalador no devolvio PID.", context=context, op_id=op_id, level="error")
            raise RuntimeError("No se pudo iniciar el instalador.")
        _log_update(f"Instalador iniciado PID={process.pid}.", context=context, op_id=op_id)
