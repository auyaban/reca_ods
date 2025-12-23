from fastapi import APIRouter

from app.sections.initial import router as initial_router
from app.sections.seccion1_informacion_basica import router as seccion1_router
from app.sections.seccion2_informacion_empresa import router as seccion2_router
from app.sections.seccion3_informacion_servicio import router as seccion3_router
from app.sections.seccion4_oferentes import router as seccion4_router
from app.sections.seccion5_observaciones import router as seccion5_router
from app.sections.editar_entrada import router as editar_router
from app.sections.resumen_final import router as resumen_router
from app.sections.terminar import router as terminar_router
from app.sections.facturas import router as facturas_router
from app.config import get_settings

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}

router.include_router(initial_router)
router.include_router(seccion1_router)
router.include_router(seccion2_router)
router.include_router(seccion3_router)
router.include_router(seccion4_router)
router.include_router(seccion5_router)
router.include_router(editar_router)
router.include_router(resumen_router)
router.include_router(terminar_router)
router.include_router(facturas_router)


@router.get("/debug/settings")
def debug_settings() -> dict:
    settings = get_settings()
    return {
        "data": {
            "supabase_url_ok": bool(settings.supabase_url),
            "supabase_anon_key_ok": bool(settings.supabase_anon_key),
            "supabase_rpc_terminar_servicio": settings.supabase_rpc_terminar_servicio,
        }
    }
