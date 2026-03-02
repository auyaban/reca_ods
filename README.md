# Sistema de Gestion ODS RECA

Aplicacion de escritorio (Tkinter + Python) para gestionar servicios ODS, sincronizar datos con Supabase, escribir en Excel y operar flujos de factura y revision de actas finalizadas.

## Resumen rapido

- UI desktop en `main_gui.py`.
- Servicios locales en `app/services/*` (sin backend HTTP externo).
- Fuente de verdad: tabla `ods` en Supabase.
- Salida operativa: `ODS 2026.xlsx` en escritorio y plantillas de factura en AppData.
- Update de app manual desde el boton **Actualizar Version de la Aplicacion**.

## Funcionalidades principales

- Crear nueva entrada (flujo por secciones 1 a 5).
- Importar acta Excel y precargar campos (con validaciones contra BD).
- Guardado en Supabase + escritura en Excel (en background y con cola cuando Excel esta bloqueado).
- Monitor en tiempo real para revisar/editar registros de `ods`.
- Actas terminadas con badge de pendientes y cambio de estado `revisado`.
- Crear factura manual por mes/ano/tipo.
- Reconstruir Excel completo desde Supabase.
- Refrescar cache local de catalogos desde Supabase (timeout 60s).
- Actualizacion de version desde GitHub Releases.

## Arquitectura

### Capas

1. GUI
- `main_gui.py`: pantallas, validaciones de UX y orquestacion.
- `start_gui.py`: splash inicial y bootstrap.

2. Servicios de negocio
- `app/services/wizard_service.py`: facade de operaciones.
- `app/services/sections/*.py`: logica por seccion (seccion1..5, editar, terminar, facturas, actas_finalizadas).

3. Integraciones
- Supabase: `app/supabase_client.py` + `app/config.py`.
- Excel: `app/excel_sync.py`.
- Update: `app/updater.py`.

## Estructura de carpetas

- `main_gui.py`: aplicacion principal.
- `start_gui.py`: launcher con splash.
- `app/`: servicios, modelos, config, utilidades.
- `Excel/`: plantilla base (`ods_2026.xlsx`).
- `facturas/`: plantillas de factura.
- `installer.iss`, `build.ps1`, `release.ps1`, `publish.ps1`: empaquetado y release.
- `CHANGELOG.md`: historial de versiones.
- `README_INSTALL.md`: guia corta de build/release.

## Requisitos

- Windows 10/11
- Python 3.10+
- Dependencias de `requirements.txt`
- Credenciales Supabase validas

Para build/release:
- Inno Setup 6
- GitHub CLI (`gh`) autenticado

## Configuracion

1) Copiar `.env.example` a `.env`.
2) Definir:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

Nota: en runtime instalado, la app prioriza `.env` en:
`%APPDATA%\Sistema de Gestion ODS RECA\.env`

## Ejecucion local

### Opcion 1 (recomendada)

```powershell
python start_gui.py
```

### Opcion 2

```powershell
python main_gui.py
```

## Flujo operativo (alto nivel)

### 1) Inicio

- Crea/verifica estructura local (`app/storage.py`).
- Precarga catalogos desde Supabase:
  - orden clausulada
  - profesionales
  - empresas
  - tarifas
  - usuarios
  - discapacidades
  - generos
  - contratos
- Muestra version local y consulta version remota en segundo plano.
- Actualiza badge de Actas Terminadas.

### 2) Crear nueva entrada

- Valida secciones 1..5.
- Genera resumen final.
- Inserta registro en `ods`.
- Actualiza Excel en background:
  - si Excel esta libre: escribe directo.
  - si Excel esta abierto/error: encola en `ODS 2026 pendiente.jsonl`.

### 3) Monitor en tiempo real

- Lee `ods` ordenado por `created_at` ascendente.
- Permite edicion controlada y guardado de cambios a Supabase.

### 4) Actas terminadas

- Lee `formatos_finalizados_il`.
- Calcula pendientes considerando `revisado` falso o nulo.
- Permite toggle de `revisado` por doble click.

### 5) Factura

- Boton manual: solicita mes, ano y tipo (`clausulada` / `no clausulada`).
- Construye factura con datos en Excel.

## Tablas Supabase usadas

- `ods`
- `empresas`
- `tarifas`
- `usuarios_reca`
- `profesionales`
- `interpretes`
- `formatos_finalizados_il`

## Archivos y rutas locales

### Excel de trabajo

- Carpeta: `%USERPROFILE%\Desktop\Excel ODS`
- Archivo principal: `ODS 2026.xlsx`
- Cola: `ODS 2026 pendiente.jsonl`

### AppData

- `%APPDATA%\Sistema de Gestion ODS RECA\facturas\`
- `%APPDATA%\Sistema de Gestion ODS RECA\logs\`

Logs frecuentes:
- `excel.log`
- `updater.log`
- `logs/backend.log` (repo local en desarrollo)

## Convenciones de logging

Los nombres de logger estan estandarizados por dominio:

| Logger | Dominio | Uso principal | Archivo de log |
|---|---|---|---|
| `reca.gui` | GUI | Eventos de interfaz y errores de UI | Configuracion global del proceso |
| `reca.excel` | Excel/Storage | Sincronizacion Excel, cola, plantillas y rutas locales | `%APPDATA%\Sistema de Gestion ODS RECA\logs\excel.log` |
| `reca.backend` | Servicios backend locales | Validaciones y operaciones de servicios | `logs/backend.log` (desarrollo) |
| `reca.backend.insert` | Insercion/terminar servicio | Coercion de schema ODS y escritura en BD | Configuracion global del proceso |
| `reca.updater` | Actualizador | Descarga, hash e instalacion de updates | `%APPDATA%\Sistema de Gestion ODS RECA\logs\updater.log` |

Implementacion base en `app/logging_utils.py`.

Formato recomendado para eventos criticos:
- `[ctx=<contexto> op=<operation_id>] mensaje...`
- `ctx`: identifica el bloque funcional (`excel.save`, `excel.flush`, `installer.run`, etc).
- `op`: correlacion corta para rastrear toda la operacion en el log.

## Build y release

### Build local

```powershell
powershell -ExecutionPolicy Bypass -File build.ps1
```

Salida esperada:
- `dist\RECA_ODS\RECA_ODS.exe`
- `installer\RECA_ODS_Setup.exe`

### Release (tag + assets)

```powershell
powershell -ExecutionPolicy Bypass -File release.ps1 vX.Y.Z
```

Publica en GitHub Release:
- `RECA_ODS_Setup.exe`
- `RECA_ODS_Setup.exe.sha256`

## Actualizacion de app (usuario final)

Desde el menu principal:
- Boton **Actualizar Version de la Aplicacion**.
- Compara version local vs release mas reciente.
- Si hay update:
  - descarga instalador
  - valida hash
  - instala en modo silencioso
  - reinicia app con cuenta regresiva

## Troubleshooting rapido

### "No se pudieron cargar datos iniciales"
- Verificar `SUPABASE_URL` y `SUPABASE_ANON_KEY`.
- Verificar conectividad de red/firewall.

### "Excel en uso" o cola pendiente
- Cerrar `ODS 2026.xlsx`.
- Reintentar cola desde flujo de edicion/estado.

### Actas pendientes en 0 pero hay registros
- Revisar campo `revisado` en `formatos_finalizados_il`.
- El sistema trata `null` como pendiente.

### El update no instala
- Verificar acceso a GitHub Releases.
- Revisar `%APPDATA%\Sistema de Gestion ODS RECA\logs\updater.log`.

## Notas de mantenimiento

- Mantener `VERSION` sincronizado con release tag.
- Documentar cambios en `CHANGELOG.md`.
- Evitar cambios manuales en plantillas sin probar reconstruccion de Excel y factura.

---

Si quieres, en un siguiente paso puedo crear tambien:
- `docs/ARCHITECTURE.md` (diagramas y contratos por modulo)
- `docs/OPERACION.md` (manual para usuarios)
- `docs/RUNBOOK.md` (soporte y diagnostico)
