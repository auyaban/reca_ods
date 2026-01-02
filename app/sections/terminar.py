from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.models.payloads import TerminarServicioRequest
from app.services.errors import ServiceError
from app.services.sections import terminar as service

router = APIRouter(prefix="/wizard", tags=["wizard"])


@router.post("/terminar-servicio")
def terminar_servicio(payload: TerminarServicioRequest, background_tasks: BackgroundTasks) -> dict:
    try:
        return service.terminar_servicio(payload, background_tasks)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
