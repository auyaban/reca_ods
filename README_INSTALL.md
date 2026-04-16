Instalador y releases (Windows)

Requisitos en la maquina de build
- Python 3.10 a 3.14
- Inno Setup 6
- GitHub CLI (gh) autenticado

Recomendado para build/release
- Python 3.13.x

Variables importantes
- `GOOGLE_SERVICE_ACCOUNT_FILE`: ruta portable que usara la app instalada, idealmente `%APPDATA%\Sistema de Gestion ODS RECA\secrets\google-service-account.json`
- `GOOGLE_SERVICE_ACCOUNT_BUILD_SOURCE_FILE`: JSON real disponible en la maquina de build para empaquetarlo dentro del installer

Build local
1) powershell -ExecutionPolicy Bypass -File build.ps1
2) Ejecutable generado en: dist\RECA_ODS\RECA_ODS.exe
3) El build ahora falla si Google Drive/Sheets esta configurado y no existe una credencial empaquetable
4) El build ahora falla si la `.venv` usa Python fuera del rango soportado para PyInstaller
5) Si la `.venv` usa una version distinta de `3.13.x`, el build avisa pero no bloquea mientras siga en rango soportado

Installer
1) Abrir installer.iss con Inno Setup y compilar
2) Instalador generado en: installer\RECA_ODS_Setup.exe
3) El archivo local `installer_config.iss` debe definir como minimo:
   - `SupabaseUrl`
   - `SupabaseKey`
   - `SupabaseAuthEmail`
   - `SupabaseAuthPassword`
   - `GoogleDriveSharedFolderId`
   - `GoogleDriveTemplateSpreadsheetName`
   - opcional: `SupabaseEdgeActaExtractionSecret`
4) El installer escribe `GOOGLE_SERVICE_ACCOUNT_FILE` hacia la ruta portable de AppData y copia el JSON si el payload existe

Release automatizado
1) Ejecutar: powershell -ExecutionPolicy Bypass -File release.ps1 vX.Y.Z
2) El release ahora falla si:
   - `installer_config.iss` queda con credenciales requeridas vacias
   - `dist\RECA_ODS\RECA_ODS.exe --smoke-test` no arranca limpio
   - el instalador no instala bien en una carpeta temporal
   - el ejecutable instalado no pasa `--smoke-test`
3) Crea release en GitHub con assets:
   - RECA_ODS_Setup.exe
   - RECA_ODS_Setup.exe.sha256

Auto‑update
- La app revisa GitHub Releases en cada inicio.
- Si hay nueva version, descarga el instalador, valida SHA256 y actualiza en silencio.
