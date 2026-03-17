from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from app.utils.text import normalize_spaces


class UsuarioNuevo(BaseModel):
    nombre_usuario: str
    cedula_usuario: str
    discapacidad_usuario: str
    genero_usuario: str

    @model_validator(mode="before")
    @classmethod
    def _normalizar_textos(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = {}
        for key, value in data.items():
            if isinstance(value, str):
                normalized[key] = normalize_spaces(value)
            else:
                normalized[key] = value
        return normalized


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
    ano_servicio: int
    session_id: str | None = None
    started_at: str | None = None
    submitted_at: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalizar_keys_textos(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = {}
        alt_ano_keys = {
            "ano_servicio",
            "año_servicio",
            "a\u00c3\u00b1o_servicio",
            "a?o_servicio",
            "a\u00ef\u00bf\u00bdo_servicio",
            "a\u00c3\u0192\u00c2\u00b1o_servicio",
        }
        for key, value in data.items():
            if key in alt_ano_keys:
                key = "ano_servicio"
            if isinstance(value, str):
                value = normalize_spaces(value)
            normalized[key] = value
        return normalized

    @field_validator("fecha_servicio")
    @classmethod
    def _validar_fecha_servicio(cls, value: str) -> str:
        date.fromisoformat(value.strip())
        return value

    @field_validator("session_id")
    @classmethod
    def _validar_session_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        clean = value.strip()
        if not clean:
            return None
        UUID(clean)
        return clean

    @field_validator("started_at", "submitted_at")
    @classmethod
    def _validar_datetime_iso(cls, value: str | None) -> str | None:
        if value is None:
            return None
        clean = value.strip()
        if not clean:
            return None
        datetime.fromisoformat(clean.replace("Z", "+00:00"))
        return clean

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
        if self.started_at and self.submitted_at:
            started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
            submitted = datetime.fromisoformat(self.submitted_at.replace("Z", "+00:00"))
            if submitted < started:
                raise ValueError("submitted_at no puede ser anterior a started_at")
        return self


class TerminarServicioRequest(BaseModel):
    ods: OdsPayload
    usuarios_nuevos: list[UsuarioNuevo] = []


class ResumenFinalRequest(BaseModel):
    ods: OdsPayload


def dump_ods_for_rpc(ods: OdsPayload) -> dict[str, Any]:
    return ods.model_dump()
