from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import seccion1 as service

router = APIRouter(prefix="/wizard/seccion-1", tags=["wizard"])


@router.get("/orden-clausulada/opciones")
def get_orden_clausulada_opciones() -> dict:
    return service.get_orden_clausulada_opciones()


@router.get("/profesionales")
def get_profesionales(programa: str | None = None) -> dict:
    try:
        return service.get_profesionales(programa=programa)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/confirmar")
def confirmar_seccion_1(payload: service.Seccion1ConfirmarRequest) -> dict:
    try:
        return service.confirmar_seccion_1(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/profesionales")
def crear_profesional(payload: service.CrearProfesionalRequest) -> dict:
    try:
        return service.crear_profesional(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
