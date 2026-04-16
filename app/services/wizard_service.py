from app.automation import (
    get_automation_attachment_analysis as _get_automation_attachment_analysis,
    get_automation_gmail_preview as _get_automation_gmail_preview,
    process_automation_email_preview as _process_automation_email_preview,
    publish_automation_email_preview as _publish_automation_email_preview,
    get_automation_staging_case as _get_automation_staging_case,
    get_automation_staging_cases as _get_automation_staging_cases,
    get_automation_test_status as _get_automation_test_status,
    save_automation_staging_case as _save_automation_staging_case,
    update_automation_staging_case as _update_automation_staging_case,
    run_batch_eod_scan as _run_batch_eod_scan,
    confirm_batch_eod_upload as _confirm_batch_eod_upload,
)
from app.catalog_index import (
    clear_runtime_caches as clear_catalog_index_runtime_caches,
    sync_local_catalog_indexes as _sync_local_catalog_indexes,
)
from app.services.sections import (
    actas_finalizadas,
    google_drive,
    google_sheet_supabase_sync,
    resumen_final,
    seccion1,
    seccion2,
    seccion3,
    seccion4,
    seccion5,
    terminar,
)
from app.services.background import InlineBackgroundTasks
from app.config import clear_settings_cache
from app.google_sheets_client import clear_google_sheets_service_cache
from app.supabase_client import clear_supabase_client_cache


def get_orden_clausulada_opciones() -> dict:
    return seccion1.get_orden_clausulada_opciones()


def get_profesionales(programa: str | None = None) -> dict:
    return seccion1.get_profesionales(programa=programa)


def crear_profesional(payload: dict) -> dict:
    req = seccion1.CrearProfesionalRequest(**payload)
    return seccion1.crear_profesional(req)


def confirmar_seccion_1(payload: dict) -> dict:
    req = seccion1.Seccion1ConfirmarRequest(**payload)
    return seccion1.confirmar_seccion_1(req)


def get_empresas() -> dict:
    return seccion2.get_empresas()


def get_empresa_por_nit(nit: str) -> dict:
    return seccion2.get_empresa_por_nit(nit)


def confirmar_seccion_2(payload: dict) -> dict:
    req = seccion2.Seccion2ConfirmarRequest(**payload)
    return seccion2.confirmar_seccion_2(req)


def get_codigos_servicio() -> dict:
    return seccion3.get_codigos_servicio()


def get_tarifa_por_codigo(codigo: str) -> dict:
    return seccion3.get_tarifa_por_codigo(codigo)


def confirmar_seccion_3(payload: dict) -> dict:
    req = seccion3.Seccion3ConfirmarRequest(**payload)
    return seccion3.confirmar_seccion_3(req)


def get_usuarios_reca() -> dict:
    return seccion4.get_usuarios_reca()


def get_usuario_por_cedula(cedula: str) -> dict:
    return seccion4.get_usuario_por_cedula(cedula)


def verificar_usuario_existe(cedula: str) -> dict:
    return seccion4.verificar_usuario_existe(cedula)


def get_discapacidades() -> dict:
    return seccion4.get_discapacidades()


def get_generos() -> dict:
    return seccion4.get_generos()


def get_tipos_contrato() -> dict:
    return seccion4.get_tipos_contrato()


def crear_usuario(payload: dict) -> dict:
    req = seccion4.CrearUsuarioRequest(**payload)
    return seccion4.crear_usuario(req)


def confirmar_seccion_4(payload: dict) -> dict:
    req = seccion4.Seccion4ConfirmarRequest(**payload)
    return seccion4.confirmar_seccion_4(req)


def confirmar_seccion_5(payload: dict) -> dict:
    req = seccion5.Seccion5ConfirmarRequest(**payload)
    return seccion5.confirmar_seccion_5(req)


def resumen_final_servicio(payload: dict) -> dict:
    req = resumen_final.ResumenFinalRequest(**payload)
    return resumen_final.resumen_final(req)


def terminar_servicio(payload: dict) -> dict:
    req = terminar.TerminarServicioRequest(**payload)
    tasks = InlineBackgroundTasks()
    response = terminar.terminar_servicio(req, tasks)
    tasks.run()
    return response


def google_drive_flush() -> dict:
    return google_drive.google_drive_flush()


def google_drive_status() -> dict:
    return google_drive.google_drive_status()


def preview_google_sheet_supabase_sync(payload: dict) -> dict:
    req = google_sheet_supabase_sync.GoogleSheetSupabaseSyncPreviewRequest(**payload)
    return google_sheet_supabase_sync.preview_google_sheet_supabase_sync(req)


def apply_google_sheet_supabase_sync(payload: dict) -> dict:
    req = google_sheet_supabase_sync.GoogleSheetSupabaseSyncApplyRequest(**payload)
    return google_sheet_supabase_sync.apply_google_sheet_supabase_sync(req)


def listar_actas_finalizadas(params: dict | None = None) -> dict:
    params = params or {}
    try:
        limit = int(params.get("limit", 500))
    except (TypeError, ValueError):
        limit = 500
    return actas_finalizadas.listar_actas_finalizadas(limit=limit)


def estado_actas_finalizadas() -> dict:
    return actas_finalizadas.estado_actas_finalizadas()


def actualizar_acta_revisado(payload: dict) -> dict:
    req = actas_finalizadas.ActaRevisadoRequest(**payload)
    return actas_finalizadas.actualizar_revisado(req)


def get_automation_test_status() -> dict:
    return _get_automation_test_status()


def get_automation_gmail_preview(limit: int | None = None) -> dict:
    return _get_automation_gmail_preview(limit=limit)


def get_automation_attachment_analysis(payload: dict) -> dict:
    return _get_automation_attachment_analysis(payload)


def process_automation_email_preview(payload: dict) -> dict:
    return _process_automation_email_preview(payload)


def publish_automation_email_preview(payload: dict) -> dict:
    return _publish_automation_email_preview(payload)


def get_automation_staging_cases() -> dict:
    return _get_automation_staging_cases()


def save_automation_staging_case(payload: dict) -> dict:
    return _save_automation_staging_case(payload)


def get_automation_staging_case(case_id: str) -> dict:
    return _get_automation_staging_case(case_id)


def update_automation_staging_case(payload: dict) -> dict:
    return _update_automation_staging_case(payload)


def run_batch_eod_scan(limit: int | None = None) -> dict:
    return _run_batch_eod_scan(limit=limit)


def confirm_batch_eod_upload(payload: dict) -> dict:
    return _confirm_batch_eod_upload(payload)


def sync_local_catalog_indexes(
    *,
    force_full: bool = False,
    catalogs: tuple[str, ...] | None = None,
    status_callback=None,
    allow_stale: bool = False,
) -> dict:
    return _sync_local_catalog_indexes(
        force_full=force_full,
        catalogs=catalogs,
        status_callback=status_callback,
        allow_stale=allow_stale,
    )


def reset_runtime_caches() -> None:
    clear_settings_cache(reload_env=True)
    clear_supabase_client_cache()
    clear_google_sheets_service_cache()
    clear_catalog_index_runtime_caches()
    terminar.clear_schema_cache()
    from app.services.acta_import_pipeline import clear_import_pipeline_caches
    clear_import_pipeline_caches()
    from app.automation.rules_engine import _get_company_by_nit_cached, _get_tarifas_cached
    _get_company_by_nit_cached.cache_clear()
    _get_tarifas_cached.cache_clear()
