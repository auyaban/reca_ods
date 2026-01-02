from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.supabase_client import get_supabase_client

router = APIRouter(prefix="/wizard/seccion-4", tags=["wizard"])

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
    clean = value.strip().lower()
    return (
        clean.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )


@router.get("/usuarios")
def get_usuarios_reca() -> dict:
    client = get_supabase_client()
    try:
        response = (
            client.table("usuarios_reca")
            .select(
                "cedula_usuario,nombre_usuario,discapacidad_usuario,genero_usuario"
            )
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    return {"data": response.data}


@router.get("/usuario")
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
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    return {"data": response.data}


@router.get("/usuarios/existe")
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
        raise HTTPException(status_code=502, detail=f"Supabase error: {exc}") from exc

    existe = bool(response.data)
    return {"data": {"existe": existe}}


@router.get("/discapacidades")
def get_discapacidades() -> dict:
    opciones = [{"id": key, "label": label} for key, label in DISCAPACIDADES.items()]
    return {"data": opciones}


@router.get("/generos")
def get_generos() -> dict:
    opciones = [{"id": key, "label": label} for key, label in GENEROS.items()]
    return {"data": opciones}


@router.get("/contratos")
def get_tipos_contrato() -> dict:
    opciones = [{"id": item.lower(), "label": item} for item in TIPOS_CONTRATO]
    return {"data": opciones}


class CrearUsuarioRequest(BaseModel):
    nombre_usuario: str
    cedula_usuario: str
    discapacidad_usuario: str
    genero_usuario: str


@router.post("/usuarios")
def crear_usuario(payload: CrearUsuarioRequest) -> dict:
    discapacidad_key = _normalize_key(payload.discapacidad_usuario)
    genero_key = _normalize_key(payload.genero_usuario)

    if discapacidad_key not in DISCAPACIDADES:
        raise HTTPException(status_code=422, detail="discapacidad_usuario invalida")
    if genero_key not in GENEROS:
        raise HTTPException(status_code=422, detail="genero_usuario invalido")

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


@router.post("/confirmar")
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
        if not all([nombre, cedula, discapacidad, genero, contrato, cargo]):
            raise HTTPException(
                status_code=422,
                detail="Completa todos los campos del oferente o deja la fila vacia.",
            )

        discapacidad_key = _normalize_key(discapacidad)
        genero_key = _normalize_key(genero)
        if discapacidad_key not in DISCAPACIDADES:
            raise HTTPException(
                status_code=422, detail="discapacidad_usuario invalida"
            )
        if genero_key not in GENEROS:
            raise HTTPException(status_code=422, detail="genero_usuario invalido")

        if fecha_ingreso:
            try:
                date.fromisoformat(fecha_ingreso)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail="fecha_ingreso debe tener formato YYYY-MM-DD",
                ) from exc

        contrato_key = _normalize_key(contrato)
        contrato_label = None
        for option in TIPOS_CONTRATO:
            if _normalize_key(option) == contrato_key:
                contrato_label = option
                break
        if contrato_label is None:
            raise HTTPException(status_code=422, detail="tipo_contrato invalido")

        nombres.append(nombre)
        cedulas.append(cedula)
        discapacidades.append(DISCAPACIDADES[discapacidad_key])
        generos.append(GENEROS[genero_key])
        fechas.append(fecha_ingreso)
        contratos.append(contrato_label)
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
