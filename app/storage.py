import os
from pathlib import Path
import logging
import shutil

from app.paths import app_data_dir, resource_path

_LOG_FILE = app_data_dir() / "logs" / "excel.log"
_logger = logging.getLogger("reca_ods_excel")
if not _logger.handlers:
    _LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)


def _desktop_excel_dir() -> Path:
    one_drive = os.getenv("OneDrive")
    if one_drive:
        for folder in ("Desktop", "Escritorio"):
            candidate = Path(one_drive) / folder
            if candidate.exists():
                return candidate / "Excel ODS"
    return Path.home() / "Desktop" / "Excel ODS"


def ensure_appdata_files() -> None:
    data_root = app_data_dir()
    excel_dir = _desktop_excel_dir()
    facturas_dir = data_root / "facturas"

    if not excel_dir.exists():
        excel_dir.mkdir(parents=True, exist_ok=True)
        _logger.info("Carpeta Excel creada: %s", excel_dir)
    else:
        _logger.info("Carpeta Excel ya existe: %s", excel_dir)

    if not facturas_dir.exists():
        facturas_dir.mkdir(parents=True, exist_ok=True)
        _logger.info("Carpeta facturas creada: %s", facturas_dir)
    else:
        _logger.info("Carpeta facturas ya existe: %s", facturas_dir)
    _logger.info("Verificando rutas. Excel=%s Facturas=%s", excel_dir, facturas_dir)

    ods_source = resource_path("Excel/ods_2026.xlsx")
    ods_target = excel_dir / "ODS 2026.xlsx"
    if ods_source.exists() and not ods_target.exists():
        shutil.copy2(ods_source, ods_target)
        _logger.info("Excel base copiado. Origen=%s Destino=%s", ods_source, ods_target)
    elif not ods_source.exists():
        _logger.warning("Plantilla ODS no encontrada. Ruta=%s", ods_source)

    clausulada_src = resource_path("facturas/clausulada.xlsx")
    clausulada_dst = facturas_dir / "clausulada.xlsx"
    if clausulada_src.exists() and not clausulada_dst.exists():
        shutil.copy2(clausulada_src, clausulada_dst)
        _logger.info("Factura clausulada copiada. Origen=%s Destino=%s", clausulada_src, clausulada_dst)
    elif not clausulada_src.exists():
        _logger.warning("Plantilla clausulada no encontrada. Ruta=%s", clausulada_src)

    no_clausulada_src = resource_path("facturas/no_clausulada.xlsx")
    no_clausulada_dst = facturas_dir / "no_clausulada.xlsx"
    if no_clausulada_src.exists() and not no_clausulada_dst.exists():
        shutil.copy2(no_clausulada_src, no_clausulada_dst)
        _logger.info("Factura no clausulada copiada. Origen=%s Destino=%s", no_clausulada_src, no_clausulada_dst)
    elif not no_clausulada_src.exists():
        _logger.warning("Plantilla no clausulada no encontrada. Ruta=%s", no_clausulada_src)
