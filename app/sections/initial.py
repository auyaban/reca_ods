from fastapi import APIRouter

from app.services.sections import initial as service

router = APIRouter(prefix="/wizard", tags=["wizard"])


@router.get("/opciones")
def get_opciones_iniciales() -> dict:
    return service.get_opciones_iniciales()
