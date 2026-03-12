# Sistema de Gestion ODS RECA

Aplicacion de escritorio (Tkinter + Python) para gestionar servicios ODS, persistir en Supabase y sincronizar la operacion mensual con Google Drive / Google Sheets.

## Resumen rapido

- UI desktop en `main_gui.py`.
- Servicios locales en `app/services/*` sin backend HTTP externo.
- Fuente de verdad: tabla `ods` en Supabase.
- Sincronizacion operativa en Google Drive / Google Sheets por mes y año.
- Update de app manual desde el boton **Actualizar Version de la Aplicacion**.

## Funcionalidades principales

- Crear nueva entrada por secciones 1 a 5.
- Importar acta Excel y precargar campos con validaciones contra BD.
- Guardado en Supabase y sincronizacion posterior a Google Drive.
- Actualizar cache local de catalogos desde Supabase.
- Actualizar Supabase desde `ODS_CALCULADA` en Google Sheets con preview y confirmacion.
- Gestion de actas terminadas con badge de pendientes y cambio de estado `revisado`.
- Actualizacion de version desde GitHub Releases.

## Arquitectura

### Capas

1. GUI
- `main_gui.py`: pantallas, validaciones de UX y orquestacion.
- `start_gui.py`: splash inicial y bootstrap.

2. Servicios de negocio
- `app/services/wizard_service.py`: facade de operaciones.
- `app/services/sections/*.py`: logica por seccion y flujos operativos.

3. Integraciones
- Supabase: `app/supabase_client.py` + `app/config.py`.
- Google Drive / Sheets: `app/google_sheets_client.py`, `app/google_drive_sync.py`, `app/google_sheet_supabase_sync.py`.
- Update: `app/updater.py`.

## Estructura de carpetas

- `main_gui.py`: aplicacion principal.
- `start_gui.py`: launcher con splash.
- `app/`: servicios, modelos, config, utilidades.
- `tools/`: utilidades operativas y de soporte.
- `installer.iss`, `build.ps1`, `release.ps1`, `publish.ps1`: empaquetado y release.
- `CHANGELOG.md`: historial de versiones.
- `README_INSTALL.md`: guia corta de build/release.

## Requisitos

- Windows 10/11
- Python 3.10+
- Dependencias de `requirements.txt`
- Credenciales Supabase validas
- Service account de Google con acceso al Shared Drive operativo

Para build/release:
- Inno Setup 6
- GitHub CLI (`gh`) autenticado

## Configuracion

1. Copiar `.env.example` a `.env`.
2. Definir:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `GOOGLE_SERVICE_ACCOUNT_FILE`
- `GOOGLE_DRIVE_SHARED_FOLDER_ID`
- `GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME`

Nota: en runtime instalado, la app prioriza `.env` en:
`%APPDATA%\Sistema de Gestion ODS RECA\.env`

Para despliegue en equipos finales, usa una ruta portable para `GOOGLE_SERVICE_ACCOUNT_FILE`, por ejemplo:
`%APPDATA%\Sistema de Gestion ODS RECA\secrets\google-service-account.json`

## Ejecucion local

### Opcion 1

```powershell
python start_gui.py
```

### Opcion 2

```powershell
python main_gui.py
```

## Flujo operativo

### 1) Inicio

- Verifica estructura local minima en AppData.
- Precarga catalogos desde Supabase.
- Muestra version local y consulta version remota en segundo plano.
- Actualiza badge de Actas Terminadas.

### 2) Crear nueva entrada

- Valida secciones 1..5.
- Genera resumen final.
- Inserta registro en `ods`.
- Sincroniza el registro al Google Sheet mensual del Shared Drive.
- Si Google falla de forma transitoria, la sincronizacion queda en cola para reintento.

### 3) Actualizar Supabase

- Ubica el spreadsheet mensual `ODS_{MON}_{YYYY}` en el Shared Drive.
- Lee `ODS_CALCULADA`.
- Compara por `id` contra Supabase.
- Muestra reporte en la app.
- Aplica solo los campos distintos tras confirmacion.

### 4) Actas terminadas

- Lee `formatos_finalizados_il`.
- Calcula pendientes considerando `revisado` falso o nulo.
- Permite toggle de `revisado` por doble click.

## Tablas Supabase usadas

- `ods`
- `empresas`
- `tarifas`
- `usuarios_reca`
- `profesionales`
- `interpretes`
- `formatos_finalizados_il`

## AppData

La app asegura estas rutas locales:

- `%APPDATA%\Sistema de Gestion ODS RECA\logs\`
- `%APPDATA%\Sistema de Gestion ODS RECA\queues\`
- `%APPDATA%\Sistema de Gestion ODS RECA\secrets\`

Logs frecuentes:

- `google_drive.log`
- `updater.log`
- `logs/backend.log` en desarrollo

## Convenciones de logging

| Logger | Dominio | Uso principal |
|---|---|---|
| `reca.gui` | GUI | Eventos de interfaz y errores de UI |
| `reca.google_drive` | Google Drive / Sheets | Sincronizacion mensual y cola Drive |
| `reca.backend` | Servicios backend locales | Validaciones y operaciones de servicios |
| `reca.backend.insert` | Insercion / terminar servicio | Coercion de schema ODS y escritura en BD |
| `reca.updater` | Actualizador | Descarga, hash e instalacion de updates |

## Build y release

### Build local

```powershell
powershell -ExecutionPolicy Bypass -File build.ps1
```

Salida esperada:

- `dist\RECA_ODS\RECA_ODS.exe`
- `installer\RECA_ODS_Setup.exe`

### Release

```powershell
powershell -ExecutionPolicy Bypass -File release.ps1 vX.Y.Z
```

Publica en GitHub Release:

- `RECA_ODS_Setup.exe`
- `RECA_ODS_Setup.exe.sha256`

## Troubleshooting rapido

### "No se pudieron cargar datos iniciales"

- Verificar `SUPABASE_URL` y `SUPABASE_ANON_KEY`.
- Verificar conectividad de red/firewall.

### "La sincronizacion a Google Drive quedo pendiente"

- Verificar credenciales del service account.
- Verificar acceso al Shared Drive y a la plantilla mensual.
- Reintentar desde el boton de sincronizacion Drive.
