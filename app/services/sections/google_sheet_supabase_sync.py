from __future__ import annotations

from pydantic import BaseModel, Field

from app.google_sheet_supabase_sync import (
    apply_google_sheet_supabase_sync as apply_sync_core,
    preview_google_sheet_supabase_sync as preview_sync_core,
)
from app.services.errors import ServiceError


class GoogleSheetSupabaseSyncPreviewRequest(BaseModel):
    mes: int = Field(ge=1, le=12)
    ano: int = Field(ge=2000)


class GoogleSheetSupabaseSyncApplyRequest(BaseModel):
    mes: int = Field(ge=1, le=12)
    ano: int = Field(ge=2000)
    selected_ids: list[str] | None = None


def preview_google_sheet_supabase_sync(payload: GoogleSheetSupabaseSyncPreviewRequest) -> dict:
    try:
        result = preview_sync_core(payload.mes, payload.ano)
    except Exception as exc:
        raise ServiceError(f"No se pudo generar el reporte de sincronizacion: {exc}", status_code=500) from exc
    return {"data": result}


def apply_google_sheet_supabase_sync(payload: GoogleSheetSupabaseSyncApplyRequest) -> dict:
    try:
        result = apply_sync_core(payload.mes, payload.ano, selected_ids=payload.selected_ids)
    except Exception as exc:
        raise ServiceError(f"No se pudo actualizar Supabase desde Google Sheets: {exc}", status_code=500) from exc
    return {"data": result}
