from pathlib import Path
import shutil

from app.paths import app_data_dir, resource_path


def ensure_appdata_files() -> None:
    data_root = app_data_dir()
    excel_dir = data_root / "Excel"
    facturas_dir = data_root / "facturas"

    excel_dir.mkdir(parents=True, exist_ok=True)
    facturas_dir.mkdir(parents=True, exist_ok=True)

    ods_source = resource_path("Excel/ods_2026.xlsx")
    ods_target = excel_dir / "ods_2026.xlsx"
    if ods_source.exists() and not ods_target.exists():
        shutil.copy2(ods_source, ods_target)

    clausulada_src = resource_path("facturas/clausulada.xlsx")
    clausulada_dst = facturas_dir / "clausulada.xlsx"
    if clausulada_src.exists() and not clausulada_dst.exists():
        shutil.copy2(clausulada_src, clausulada_dst)

    no_clausulada_src = resource_path("facturas/no_clausulada.xlsx")
    no_clausulada_dst = facturas_dir / "no_clausulada.xlsx"
    if no_clausulada_src.exists() and not no_clausulada_dst.exists():
        shutil.copy2(no_clausulada_src, no_clausulada_dst)
