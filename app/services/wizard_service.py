from app.services.sections import (
    editar as editar_entrada,
    facturas,
    resumen_final,
    seccion1,
    seccion2,
    seccion3,
    seccion4,
    seccion5,
    terminar,
)
from app.services.background import InlineBackgroundTasks


def get_orden_clausulada_opciones() -> dict:
    return seccion1.get_orden_clausulada_opciones()


def get_profesionales(programa: str | None = None) -> dict:
    return seccion1.get_profesionales(programa=programa)


def crear_profesional(payload: dict) -> dict:
    req = seccion1.CrearProfesionalRequest(**payload)
    return seccion1.crear_profesional(req)


def confirmar_seccion_1(payload: dict) -> dict:
    req = seccion1.Seccion1ConfirmarRequest(**payload)
    return seccion1.confirmar_seccion_1(req)


def get_empresas() -> dict:
    return seccion2.get_empresas()


def get_empresa_por_nit(nit: str) -> dict:
    return seccion2.get_empresa_por_nit(nit)


def confirmar_seccion_2(payload: dict) -> dict:
    req = seccion2.Seccion2ConfirmarRequest(**payload)
    return seccion2.confirmar_seccion_2(req)


def get_codigos_servicio() -> dict:
    return seccion3.get_codigos_servicio()


def get_tarifa_por_codigo(codigo: str) -> dict:
    return seccion3.get_tarifa_por_codigo(codigo)


def confirmar_seccion_3(payload: dict) -> dict:
    req = seccion3.Seccion3ConfirmarRequest(**payload)
    return seccion3.confirmar_seccion_3(req)


def get_usuarios_reca() -> dict:
    return seccion4.get_usuarios_reca()


def get_usuario_por_cedula(cedula: str) -> dict:
    return seccion4.get_usuario_por_cedula(cedula)


def verificar_usuario_existe(cedula: str) -> dict:
    return seccion4.verificar_usuario_existe(cedula)


def get_discapacidades() -> dict:
    return seccion4.get_discapacidades()


def get_generos() -> dict:
    return seccion4.get_generos()


def get_tipos_contrato() -> dict:
    return seccion4.get_tipos_contrato()


def crear_usuario(payload: dict) -> dict:
    req = seccion4.CrearUsuarioRequest(**payload)
    return seccion4.crear_usuario(req)


def confirmar_seccion_4(payload: dict) -> dict:
    req = seccion4.Seccion4ConfirmarRequest(**payload)
    return seccion4.confirmar_seccion_4(req)


def confirmar_seccion_5(payload: dict) -> dict:
    req = seccion5.Seccion5ConfirmarRequest(**payload)
    return seccion5.confirmar_seccion_5(req)


def resumen_final_servicio(payload: dict) -> dict:
    req = resumen_final.ResumenFinalRequest(**payload)
    return resumen_final.resumen_final(req)


def terminar_servicio(payload: dict) -> dict:
    req = terminar.TerminarServicioRequest(**payload)
    tasks = InlineBackgroundTasks()
    response = terminar.terminar_servicio(req, tasks)
    tasks.run()
    return response


def buscar_entradas(params: dict) -> dict:
    return editar_entrada.buscar_entradas(**params)


def obtener_entrada(params: dict) -> dict:
    return editar_entrada.obtener_entrada(**params)


def actualizar_entrada(payload: dict) -> dict:
    req = editar_entrada.OdsActualizarRequest(**payload)
    tasks = InlineBackgroundTasks()
    response = editar_entrada.actualizar_entrada(req, tasks)
    tasks.run()
    return response


def eliminar_entrada(payload: dict) -> dict:
    req = editar_entrada.OdsEliminarRequest(**payload)
    tasks = InlineBackgroundTasks()
    response = editar_entrada.eliminar_entrada(req, tasks)
    tasks.run()
    return response


def excel_flush() -> dict:
    return editar_entrada.flush_excel_queue()


def excel_status() -> dict:
    return editar_entrada.excel_status()


def excel_rebuild() -> dict:
    return editar_entrada.rebuild_excel()


def crear_factura(payload: dict) -> dict:
    req = facturas.CrearFacturaRequest(**payload)
    return facturas.crear_factura(req)


 
