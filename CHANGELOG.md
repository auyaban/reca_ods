# Changelog

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

