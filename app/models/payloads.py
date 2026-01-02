from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class UsuarioNuevo(BaseModel):
    nombre_usuario: str
    cedula_usuario: str
    discapacidad_usuario: str
    genero_usuario: str


class OdsPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    orden_clausulada: str
    nombre_profesional: str
    nit_empresa: str
    nombre_empresa: str
    caja_compensacion: str | None = None
    asesor_empresa: str | None = None
    sede_empresa: str | None = None
    fecha_servicio: str
    codigo_servicio: str
    referencia_servicio: str
    descripcion_servicio: str
    modalidad_servicio: str
    valor_virtual: float
    valor_bogota: float
    valor_otro: float
    todas_modalidades: float
    horas_interprete: float | None = None
    valor_interprete: float
    valor_total: float
    nombre_usuario: str | None = None
    cedula_usuario: str | None = None
    discapacidad_usuario: str | None = None
    genero_usuario: str | None = None
    fecha_ingreso: str | None = None
    tipo_contrato: str | None = None
    cargo_servicio: str | None = None
    total_personas: int = 0
    observaciones: str | None = None
    observacion_agencia: str | None = None
    seguimiento_servicio: str | None = None
    mes_servicio: int
    ano_servicio: int = Field(alias="año_servicio")

    @field_validator("fecha_servicio")
    @classmethod
    def _validar_fecha_servicio(cls, value: str) -> str:
        date.fromisoformat(value.strip())
        return value

    @field_validator("orden_clausulada")
    @classmethod
    def _validar_orden_clausulada(cls, value: str) -> str:
        clean = value.strip().lower()
        if clean not in {"si", "no"}:
            raise ValueError("orden_clausulada debe ser 'si' o 'no'")
        return clean

    @field_validator("mes_servicio")
    @classmethod
    def _validar_mes(cls, value: int) -> int:
        if value < 1 or value > 12:
            raise ValueError("mes_servicio invalido")
        return value

    @model_validator(mode="after")
    def _validar_valor_total(self) -> "OdsPayload":
        base_total = (
            self.valor_virtual
            + self.valor_bogota
            + self.valor_otro
            + self.todas_modalidades
        )
        esperado = self.valor_interprete if self.valor_interprete > 0 else base_total
        if abs(self.valor_total - esperado) > 0.01:
            raise ValueError("valor_total no coincide con la suma de los valores")
        return self

    @model_validator(mode="after")
    def _validar_total_personas(self) -> "OdsPayload":
        return self


class TerminarServicioRequest(BaseModel):
    ods: OdsPayload
    usuarios_nuevos: list[UsuarioNuevo] = []


class ResumenFinalRequest(BaseModel):
    ods: OdsPayload


def dump_ods_for_rpc(ods: OdsPayload) -> dict[str, Any]:
    return ods.model_dump(by_alias=True)
