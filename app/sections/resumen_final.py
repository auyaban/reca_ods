from fastapi import APIRouter

from app.models.payloads import ResumenFinalRequest
from app.services.sections import resumen_final as service

router = APIRouter(prefix="/wizard", tags=["wizard"])


@router.post("/resumen-final")
def resumen_final(payload: ResumenFinalRequest) -> dict:
    return service.resumen_final(payload)
