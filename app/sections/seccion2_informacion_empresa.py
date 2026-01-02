from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import seccion2 as service

router = APIRouter(prefix="/wizard/seccion-2", tags=["wizard"])


@router.get("/empresas")
def get_empresas() -> dict:
    try:
        return service.get_empresas()
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/empresa")
def get_empresa_por_nit(nit: str) -> dict:
    try:
        return service.get_empresa_por_nit(nit)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/confirmar")
def confirmar_seccion_2(payload: service.Seccion2ConfirmarRequest) -> dict:
    return service.confirmar_seccion_2(payload)
