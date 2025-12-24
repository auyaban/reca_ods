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
