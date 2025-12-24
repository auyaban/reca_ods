from pydantic import BaseModel


class FacturaItem(BaseModel):
    codigo_servicio: str
    referencia_servicio: str
    descripcion_servicio: str
    valor_base: float
    cantidad: float
    total: float
