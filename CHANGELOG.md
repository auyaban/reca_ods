# Changelog

## 2.0.20
- Se agrega `Orientacion Laboral` a la lista desplegable de `Tipo de contrato`.

## 2.0.19
- Autenticacion Supabase mas robusta: si la credencial tecnica configurada falla, la app reintenta con la vigente y passwords legacy conocidos.
- Autocorreccion del `.env` local en AppData cuando se recupera una credencial tecnica valida.
- Flujo de agregar interpretes endurecido ante problemas de permisos en tabla `interpretes`.

## 2.0.18
- Rotacion de credencial tecnica Supabase para el usuario `test@reca.local`.
- Actualizacion del password tecnico por defecto en configuracion local.
- Compatibilidad retroactiva: si se detecta el password tecnico anterior en `SUPABASE_AUTH_PASSWORD`, se reemplaza automaticamente por el nuevo.

## 2.0.17
- Seccion 3: cambio de fecha de servicio a tres campos independientes (dia editable, mes y ano desplegables).
- Fecha vacia por defecto al crear nueva entrada.
- Construccion y validacion de fecha en formato `YYYY-MM-DD` antes de confirmar/guardar.
- Compatibilidad actualizada con importacion de actas y resumen para el nuevo esquema de fecha.

## 2.0.16
- Nueva instrumentacion de trazas del flujo de "Crear nueva entrada" en `Desktop\\log ods.log` (inicio, validaciones por seccion, resumen, guardado, timeout/errores).
- Guardado final de `terminar_servicio` movido a background task para evitar congelamiento de UI en la ventana de procesamiento.
- Manejo de errores robusto en tareas en segundo plano y en cliente API para excepciones de servicio (`ServiceError`).
- Consulta de estado de "Actas Terminadas" en carga asíncrona al abrir la pantalla inicial, evitando bloqueos en el arranque.

## 2.0.15
- Importador de actas: mejora en deteccion de profesional desde la seccion de asistentes con multiples candidatos y matching por similitud contra profesionales RECA.
- Importador de actas: mejor deteccion de NIT en celdas vecinas y filas cercanas, incluyendo valores con separadores.
- Importador de actas: ajuste en extraccion de participantes (incluye cabecera de nombre oferente) y evita capturar textos largos no utiles.

## 2.0.14
- Autenticacion Supabase automatica al iniciar (usuario tecnico) para mantener compatibilidad con politicas `authenticated`.
- Configuracion de credenciales tecnicas por defecto y soporte por variables `SUPABASE_AUTH_EMAIL` / `SUPABASE_AUTH_PASSWORD`.
- Mejora de usabilidad en selectores de fecha (campos mas grandes y con mejor area de clic).
- Correccion de textos con mojibake pendientes en confirmaciones de GUI.

## 2.0.13
- Hotfix GUI: correccion de textos con mojibake (GESTION/Si/Ano/Bogota/Inclusion).
- Popup de agregar profesional mejorado con validaciones, carga visible y mensaje de exito.
- Guardado de profesional mas robusto ante valores de programa con codificacion inconsistente.

## 2.0.12
- Remediacion integral frontend: monitor en tiempo real thread-safe (estado tipado, limpieza de traces y callbacks).
- Carga no bloqueante al iniciar y al abrir "Crear nueva entrada", con timeout y opcion de reintento.
- Paridad de calculos GUI/backend en Seccion 3 usando modulo compartido de dominio.
- Endurecimiento de errores UI (mensaje sanitizado con codigo) y apertura segura de rutas en Actas Terminadas.
- Control anti doble accion en menu principal durante operaciones criticas.

## 2.0.11
- Importador de actas Excel en "Crear nueva entrada" con vista previa antes de aplicar.
- Mapeo base: NIT, fecha de visita y profesional desde seccion de asistentes (con match parcial/similar contra BD).
- Validacion de NIT contra BD antes de aplicar importacion.
- Participantes por cedula unicamente; solo se cargan cedulas existentes en BD.
- Mejoras de layout inicial (centrado y ajuste para pantallas pequenas) y boton de actualizacion junto a notificaciones.

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











