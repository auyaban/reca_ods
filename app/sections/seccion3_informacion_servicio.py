from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import seccion3 as service

router = APIRouter(prefix="/wizard/seccion-3", tags=["wizard"])


@router.get("/tarifas")
def get_codigos_servicio() -> dict:
    try:
        return service.get_codigos_servicio()
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/tarifa")
def get_tarifa_por_codigo(codigo: str) -> dict:
    try:
        return service.get_tarifa_por_codigo(codigo)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/confirmar")
def confirmar_seccion_3(payload: service.Seccion3ConfirmarRequest) -> dict:
    try:
        return service.confirmar_seccion_3(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
