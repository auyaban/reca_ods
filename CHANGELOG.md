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
