from app.models.payloads import ResumenFinalRequest


def resumen_final(payload: ResumenFinalRequest) -> dict:
    ods = payload.ods
    data = {
        "fecha_servicio": ods.fecha_servicio.strip(),
        "nombre_profesional": ods.nombre_profesional.strip(),
        "nombre_empresa": ods.nombre_empresa.strip(),
        "codigo_servicio": ods.codigo_servicio.strip(),
        "valor_total": ods.valor_total,
    }
    return {"data": data}
