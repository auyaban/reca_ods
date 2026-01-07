from pydantic import BaseModel

from app.services.errors import ServiceError
from app.supabase_client import get_supabase_client

DISCAPACIDADES = {
    "intelectual": "Intelectual",
    "multiple": "Múltiple",
    "fisica": "Física",
    "visual": "Visual",
    "auditiva": "Auditiva",
    "psicosocial": "Psicosocial",
    "n/a": "N/A",
}


GENEROS = {
    "hombre": "Hombre",
    "mujer": "Mujer",
    "otro": "Otro",
}

TIPOS_CONTRATO = ["Laboral", "Contrato Aprendiz Especial"]


def _normalize_key(value: str) -> str:
    clean = " ".join(value.strip().lower().split())
    clean = clean.replace("múltiple", "multiple").replace("física", "fisica")
    import unicodedata
    clean = "".join(ch for ch in unicodedata.normalize("NFKD", clean) if not unicodedata.combining(ch))
    clean = "".join(ch for ch in clean if ch.isalnum() or ch == "/")
    return clean
    return {"data": response.data}


def get_usuario_por_cedula(cedula: str) -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("usuarios_reca")
            .select(
                "nombre_usuario,cedula_usuario,discapacidad_usuario,genero_usuario"
            )
            .eq("cedula_usuario", cedula)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    return {"data": response.data}


def verificar_usuario_existe(cedula: str) -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("usuarios_reca")
            .select("cedula_usuario")
            .eq("cedula_usuario", cedula)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ServiceError(f"Supabase error: {exc}", status_code=502) from exc

    existe = bool(response.data)
    return {"data": {"existe": existe}}


def get_discapacidades() -> dict:
    opciones = [{"id": key, "label": label} for key, label in DISCAPACIDADES.items()]
    return {"data": opciones}


def get_generos() -> dict:
    opciones = [{"id": key, "label": label} for key, label in GENEROS.items()]
    return {"data": opciones}


def get_tipos_contrato() -> dict:
    opciones = [{"id": item.lower(), "label": item} for item in TIPOS_CONTRATO]
    return {"data": opciones}


class CrearUsuarioRequest(BaseModel):
    nombre_usuario: str
    cedula_usuario: str
    discapacidad_usuario: str
    genero_usuario: str


def crear_usuario(payload: CrearUsuarioRequest) -> dict:
    discapacidad_key = _normalize_key(payload.discapacidad_usuario)
    genero_key = _normalize_key(payload.genero_usuario)

    if discapacidad_key not in DISCAPACIDADES:
        raise ServiceError("discapacidad_usuario invalida", status_code=422)
    if genero_key not in GENEROS:
        raise ServiceError("genero_usuario invalido", status_code=422)

    data = {
        "nombre_usuario": payload.nombre_usuario.strip(),
        "cedula_usuario": payload.cedula_usuario.strip(),
        "discapacidad_usuario": DISCAPACIDADES[discapacidad_key],
        "genero_usuario": GENEROS[genero_key],
    }
    return {"data": data, "persistir_al_final": True}


class PersonaOferente(BaseModel):
    nombre_usuario: str | None = None
    cedula_usuario: str | None = None
    discapacidad_usuario: str | None = None
    genero_usuario: str | None = None
    fecha_ingreso: str | None = None
    tipo_contrato: str | None = None
    cargo_servicio: str | None = None


class Seccion4ConfirmarRequest(BaseModel):
    personas: list[PersonaOferente] | None = None


def confirmar_seccion_4(payload: Seccion4ConfirmarRequest) -> dict:
    if not payload.personas:
        return {
            "data": {
                "nombre_usuario": None,
                "cedula_usuario": None,
                "discapacidad_usuario": None,
                "genero_usuario": None,
                "fecha_ingreso": None,
                "tipo_contrato": None,
                "cargo_servicio": None,
                "total_personas": 0,
            }
        }

    def join_field(values: list[str]) -> str:
        return ";".join(values)

    nombres: list[str] = []
    cedulas: list[str] = []
    discapacidades: list[str] = []
    generos: list[str] = []
    fechas: list[str] = []
    contratos: list[str] = []
    cargos: list[str] = []

    for persona in payload.personas:
        nombre = (persona.nombre_usuario or "").strip()
        cedula = (persona.cedula_usuario or "").strip()
        discapacidad = (persona.discapacidad_usuario or "").strip()
        genero = (persona.genero_usuario or "").strip()
        contrato = (persona.tipo_contrato or "").strip()
        cargo = (persona.cargo_servicio or "").strip()
        fecha_ingreso = (persona.fecha_ingreso or "").strip()

        if not any([nombre, cedula, discapacidad, genero, contrato, cargo, fecha_ingreso]):
            continue

        nombres.append(nombre)
        cedulas.append(cedula)
        discapacidades.append(discapacidad)
        generos.append(genero)
        fechas.append(fecha_ingreso)
        contratos.append(contrato)
        cargos.append(cargo)

    if not nombres:
        return {
            "data": {
                "nombre_usuario": None,
                "cedula_usuario": None,
                "discapacidad_usuario": None,
                "genero_usuario": None,
                "fecha_ingreso": None,
                "tipo_contrato": None,
                "cargo_servicio": None,
                "total_personas": 0,
            }
        }

    data = {
        "nombre_usuario": join_field(nombres),
        "cedula_usuario": join_field(cedulas),
        "discapacidad_usuario": join_field(discapacidades),
        "genero_usuario": join_field(generos),
        "fecha_ingreso": join_field(fechas),
        "tipo_contrato": join_field(contratos),
        "cargo_servicio": join_field(cargos),
        "total_personas": len(nombres),
    }

    return {"data": data}
