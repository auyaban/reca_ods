from fastapi import APIRouter

router = APIRouter(prefix="/wizard", tags=["wizard"])


@router.get("/inicial/opciones")
def get_opciones_iniciales() -> dict:
    opciones = [
        {"id": "nueva", "label": "Crear nueva entrada"},
        {"id": "editar", "label": "Editar entrada existente"},
    ]
    return {"data": opciones}
