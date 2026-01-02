from fastapi import APIRouter, HTTPException

from app.services.errors import ServiceError
from app.services.sections import seccion4 as service

router = APIRouter(prefix="/wizard/seccion-4", tags=["wizard"])


@router.get("/usuarios")
def get_usuarios_reca() -> dict:
    try:
        return service.get_usuarios_reca()
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/usuario")
def get_usuario_por_cedula(cedula: str) -> dict:
    try:
        return service.get_usuario_por_cedula(cedula)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/usuarios/existe")
def verificar_usuario_existe(cedula: str) -> dict:
    try:
        return service.verificar_usuario_existe(cedula)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/discapacidades")
def get_discapacidades() -> dict:
    return service.get_discapacidades()


@router.get("/generos")
def get_generos() -> dict:
    return service.get_generos()


@router.get("/contratos")
def get_tipos_contrato() -> dict:
    return service.get_tipos_contrato()


@router.post("/usuarios")
def crear_usuario(payload: service.CrearUsuarioRequest) -> dict:
    try:
        return service.crear_usuario(payload)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/confirmar")
def confirmar_seccion_4(payload: service.Seccion4ConfirmarRequest) -> dict:
    return service.confirmar_seccion_4(payload)
