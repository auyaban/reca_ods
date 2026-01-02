from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.services.errors import ServiceError
from app.services.sections import editar as service

router = APIRouter(prefix="/wizard/editar", tags=["wizard"])


@router.get("/buscar")
def buscar_entradas(
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    try:
        return service.buscar_entradas(
            nombre_profesional=nombre_profesional,
            nit_empresa=nit_empresa,
            fecha_servicio=fecha_servicio,
            codigo_servicio=codigo_servicio,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/entrada")
def obtener_entrada(
    id: str | None = None,
    nombre_profesional: str | None = None,
    nit_empresa: str | None = None,
    fecha_servicio: str | None = None,
    codigo_servicio: str | None = None,
):
    try:
        return service.obtener_entrada(
            id=id,
            nombre_profesional=nombre_profesional,
            nit_empresa=nit_empresa,
            fecha_servicio=fecha_servicio,
            codigo_servicio=codigo_servicio,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/actualizar")
def actualizar_entrada(payload: service.OdsActualizarRequest, background_tasks: BackgroundTasks):
    try:
        return service.actualizar_entrada(payload, background_tasks)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/eliminar")
def eliminar_entrada(payload: service.OdsEliminarRequest, background_tasks: BackgroundTasks):
    try:
        return service.eliminar_entrada(payload, background_tasks)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/excel/flush")
def flush_excel_queue() -> dict:
    try:
        return service.flush_excel_queue()
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/excel/status")
def excel_status() -> dict:
    return service.excel_status()


@router.post("/excel/rebuild")
def rebuild_excel() -> dict:
    try:
        return service.rebuild_excel()
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
