from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import seccion5 as service

router = APIRouter(prefix="/wizard/seccion-5", tags=["wizard"])


@router.post("/confirmar")
def confirmar_seccion_5(payload: service.Seccion5ConfirmarRequest) -> dict:
    try:
        return service.confirmar_seccion_5(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/resumen")
def resumen_servicio(payload: service.ResumenServicioRequest) -> dict:
    try:
        return service.resumen_servicio(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
