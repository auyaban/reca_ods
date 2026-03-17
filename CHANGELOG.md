# Changelog

## 2.2.10
- Instrumentacion de cadencia en `ods`: nuevas columnas `session_id`, `started_at` y `submitted_at` para medir tiempo de captura por entrada.
- La app registra automaticamente el inicio y envio de cada nueva entrada ODS y persiste esos metadatos junto al guardado.
- Nueva vista `public.ods_daily_cadence_report` en Supabase para analizar entradas por dia y gaps entre `created_at` consecutivos.

## 2.2.9
- `Actas Terminadas` ahora prioriza `payload_normalized` proveniente de Inclusion Laboral y solo cae al parser del archivo si ese payload falta o es invalido.
- El pipeline de importacion soporta payloads estructurados versionados para formularios finalizados y seguimientos.
- Cobertura de pruebas ampliada para importacion payload-first y fallback sobre filas finalizadas.

## 2.2.8
- Importador PDF ajustado para evaluaciones de accesibilidad con valores antes de sus etiquetas, evitando bloqueos por mismatch de empresa al crear nueva entrada.
- Motor de reglas: `observaciones` vuelve a limitarse a cargo y vacantes cuando aplica; el número de seguimiento se conserva en `seguimiento_servicio`.
- Cobertura de pruebas ampliada para parser PDF y reglas de automatización asociadas a observaciones.

## 2.2.3
- Se agrega `Aaron TEST`, un acceso controlado por `username` y flags para probar automatizacion sin exponer funciones nuevas al resto de usuarios.
- Nuevo flujo de lectura `read-only` de Gmail con service account delegada: lista correos candidatos, PDFs y cruza remitentes contra `profesionales`.
- Clasificacion preliminar de documentos y motor de reglas para sugerir `codigo_servicio`, observaciones y confianza en procesos de inclusion laboral.
- Correccion del importador PDF productivo para actas donde `fecha`, `modalidad` y `empresa` aparecen antes de sus etiquetas, evitando fallos al importar ciertos formatos de reactivacion.
- Cobertura de pruebas ampliada para Gmail, catalogo de procesos, clasificacion, reglas de negocio, Google file URLs e importacion PDF.

## 2.2.1
- Hotfix de arranque: la app ahora acepta archivos `.env` en ANSI/Windows-1252 y los normaliza a UTF-8 automaticamente.
- Se elimina la dependencia de `python-dotenv` en el arranque para evitar fallos de decodificacion al importar configuracion.

## 2.2.0
- Importacion de actas ampliada a PDF con texto seleccionable, incluyendo archivos locales y enlaces de Google Drive.
- Validacion mas estricta al importar: NIT y nombre deben coincidir con `empresas`, y las cedulas se verifican contra `usuarios_reca`.
- Si una cedula no existe pero el acta trae datos minimos, el usuario se prepara para creacion automatica al guardar el servicio.
- Resolucion del profesional mejorada: se toma desde asistentes/participantes del acta y se cruza por similitud con la tabla de profesionales.
- Vista previa de importacion redisenada para mostrar resumen claro, estado de usuarios y avisos; ademas se abre con mejor alto para dejar visibles los botones.
- Instalador corregido para preservar configuracion `GOOGLE_*` y copiar la service account al perfil del usuario final.

## 2.1.0
- Migracion operativa de ODS a Google Drive / Google Sheets con sincronizacion mensual por Shared Drive.
- Nuevo flujo `Actualizar Supabase` desde `ODS_CALCULADA` con preview, comparacion por `id` y aplicacion parcial robusta.
- Remocion completa de factura, reconstruccion desde Supabase y monitor en tiempo real, junto con sus dependencias de Excel local.
- Limpieza de almacenamiento y build: sin plantillas Excel/factura locales ni runtime ligado a `app/excel_sync.py`.
- `Actas Terminadas` ahora soporta enlaces de Google Drive/Sheets y puede abrir/preparar una nueva entrada al marcar un acta como revisada.
- Importacion de actas extendida a Google Sheets/Drive, con validacion fuera del hilo principal y cobertura de pruebas ampliada.

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











