from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import facturas as service

router = APIRouter(prefix="/wizard/facturas", tags=["wizard"])


@router.post("/crear")
def crear_factura(payload: service.CrearFacturaRequest) -> dict:
    try:
        return service.crear_factura(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/preview")
def preview_factura(payload: service.PreviewFacturaRequest) -> dict:
    try:
        return service.preview_factura(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/generar")
def generar_factura(payload: service.GenerarFacturaRequest) -> dict:
    try:
        return service.generar_factura(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/debug/actualizar")
def debug_actualizar_factura(mes: int, ano: int, tipo: str) -> dict:
    try:
        return service.debug_actualizar_factura(mes, ano, tipo)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
