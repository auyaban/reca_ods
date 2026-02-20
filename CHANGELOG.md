# Changelog

## 2.0.6
- Busqueda por nombre de empresa muestra todos los nombres aunque compartan NIT.

## 2.0.5
- Boton de crear factura con flujo manual (mes/ano/tipo) y eliminacion de triggers automaticos.
- Actualizacion manual desde el GUI con instalador silencioso y reinicio automatico.
- Ajustes de estabilidad en cola Excel y validaciones del GUI.

## 2.0.3
- Actualizacion previa al GUI con barra propia y reinicio automatico.
- Verificacion de version en GUI sin iniciar update en segundo plano.

## 2.0.2
- Hotfix: restaura `get_usuarios_reca` y corrige caracteres corruptos en Seccion 4.

## 2.0.1
- Correcciones de caracteres/acentos en textos de opciones (Sí, Inclusión, Múltiple, Física).
- Normalización adicional de claves corruptas para `año_servicio`.

## 2.0.0
- Nuevo flujo sin backend embebido: la app usa servicios locales para Supabase, Excel y validaciones.
- Refactor por secciones a `app/services/*` y simplificacion de rutas.
- Excel: cola y reconstruccion mas estable; backups solo en reconstruccion total.
- Profesionales e interpretes separados en tablas y lista unificada en el GUI.
- Mejoras en edicion: cierre automatico y regreso al menu principal.

## 1.1.17
- Oferentes sin validacion obligatoria: se permiten vacios y se envian como NULL.

## 1.1.0
- Splash de arranque estable con wrapper `start_gui.py` y lanzador `run_gui.cmd`.
- Guardado en Excel/factura en segundo plano y optimizado (una sola apertura).
- Cola de Excel con estado visible, reintento y bloqueo cuando hay pendientes o Excel abierto.
- ID UUID oculto en Excel para actualizaciones/eliminaciones exactas.
- Reconstruccion de Excel desde Supabase con backup automatico.
- Correcciones de actualizacion/edicion (fechas duplicadas, errores de requests).
- Limpieza del frontend (flujo de factura removido).

## 1.1.1
- Hotfix: desactivar logging de uvicorn en modo embebido para evitar error en ejecutable.

## 1.1.2
- Hotfix: configuracion minima de logging de uvicorn en modo embebido.

## 1.1.3
- Hotfix: logging de uvicorn con formatter/handler por defecto en modo embebido.

## 1.1.4
- Hotfix: logging de uvicorn con handler/formatter de access en modo embebido.

## 1.1.5
- Hotfix: fallback de backend embebido a subproceso y modo `--backend` para ejecutable.

## 1.1.6
- Hotfix: log de arranque del backend en `%TEMP%` para diagnostico en instaladores.

## 1.1.7
- Hotfix: forzar `localhost` a `127.0.0.1` y log adicional de subproceso backend.

## 1.1.8
- Hotfix: log del subproceso backend con salida no bufferizada y logging default.

## 1.1.9
- Hotfix: backend embebido/subproceso usa app construida en runtime (sin importar `main`).

## 1.1.10
- Mejora: el instalador usa `start_gui.py` como entrypoint para asegurar pantalla de carga visible.

## 1.1.11
- Mejora: layout de una sola columna y `fecha_ingreso` opcional.

## 1.1.12
- Mejora: autoupdater con instalador visible y reinicio automatico; version local/remota en el GUI.

## 1.1.13
- Mejora: logs explicitos al crear/verificar carpeta Excel y facturas.









