from datetime import date
import logging
from pathlib import Path
import unicodedata

from pydantic import BaseModel

from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client

_ROOT_DIR = Path(__file__).resolve().parents[3]
_LOG_DIR = _ROOT_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "backend.log"

_logger = logging.getLogger("reca_ods")
if not _logger.handlers:
    handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.info("Logger iniciado. Archivo=%s", _LOG_FILE)


def _normalize_text(value: str) -> str:
    clean = " ".join(value.strip().lower().split())
    normalized = unicodedata.normalize("NFD", clean)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def get_codigos_servicio() -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("tarifas")
            .select(
                "codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base"
            )
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


def get_tarifa_por_codigo(codigo: str) -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("tarifas")
            .select(
                "codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base"
            )
            .eq("codigo_servicio", codigo)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


class Seccion3ConfirmarRequest(BaseModel):
    fecha_servicio: str
    codigo_servicio: str
    referencia_servicio: str
    descripcion_servicio: str
    modalidad_servicio: str
    valor_base: float
    servicio_interpretacion: bool = False
    horas_interprete: int | None = None
    minutos_interprete: int | None = None


def confirmar_seccion_3(payload: Seccion3ConfirmarRequest) -> dict:
    try:
        date.fromisoformat(payload.fecha_servicio.strip())
    except ValueError as exc:
        raise ServiceError(
            "fecha_servicio debe tener formato YYYY-MM-DD", status_code=422
        ) from exc

    modalidad = _normalize_text(payload.modalidad_servicio)
    _logger.info(
        "Seccion3 modalidad raw=%s normalized=%s codigo=%s",
        payload.modalidad_servicio,
        modalidad,
        payload.codigo_servicio,
    )
    valores = {
        "valor_virtual": 0.0,
        "valor_bogota": 0.0,
        "valor_otro": 0.0,
        "todas_modalidades": 0.0,
    }

    if "toda" in modalidad and "modalidad" in modalidad:
        modalidad = "todas las modalidades"
    elif "fuera" in modalidad or "otro" in modalidad:
        modalidad = "fuera de bogota"
    elif "bogota" in modalidad:
        modalidad = "bogota"
    elif "virtual" in modalidad:
        modalidad = "virtual"

    if modalidad == "virtual":
        valores["valor_virtual"] = payload.valor_base
    elif modalidad == "bogota":
        valores["valor_bogota"] = payload.valor_base
    elif modalidad == "fuera de bogota":
        valores["valor_otro"] = payload.valor_base
    elif modalidad == "todas las modalidades":
        valores["todas_modalidades"] = payload.valor_base
    else:
        raise ServiceError("modalidad_servicio invalida", status_code=422)

    horas_decimales = None
    valor_interprete = 0.0
    if payload.servicio_interpretacion:
        if payload.horas_interprete is None or payload.minutos_interprete is None:
            raise ServiceError(
                "Debe indicar horas_interprete y minutos_interprete",
                status_code=422,
            )

        if payload.horas_interprete < 0 or payload.minutos_interprete < 0:
            raise ServiceError("Horas/minutos invalidos", status_code=422)

        horas_decimales = payload.horas_interprete + (payload.minutos_interprete / 60)
        valor_interprete = horas_decimales * payload.valor_base

    base_total = (
        valores["valor_virtual"]
        + valores["valor_bogota"]
        + valores["valor_otro"]
        + valores["todas_modalidades"]
    )
    if payload.servicio_interpretacion and horas_decimales is not None:
        valor_total = valor_interprete
    else:
        valor_total = base_total

    data = {
        "fecha_servicio": payload.fecha_servicio.strip(),
        "codigo_servicio": payload.codigo_servicio.strip(),
        "referencia_servicio": payload.referencia_servicio.strip(),
        "descripcion_servicio": payload.descripcion_servicio.strip(),
        "modalidad_servicio": payload.modalidad_servicio.strip(),
        "valor_virtual": valores["valor_virtual"],
        "valor_bogota": valores["valor_bogota"],
        "valor_otro": valores["valor_otro"],
        "todas_modalidades": valores["todas_modalidades"],
        "horas_interprete": horas_decimales,
        "valor_interprete": valor_interprete,
        "valor_total": valor_total,
    }
    return {"data": data}
