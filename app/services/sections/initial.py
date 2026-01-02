def get_opciones_iniciales() -> dict:
    opciones = [
        {"id": "nueva", "label": "Crear nueva entrada"},
        {"id": "editar", "label": "Editar entrada existente"},
    ]
    return {"data": opciones}
