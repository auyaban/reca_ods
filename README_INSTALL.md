Instalador y releases (Windows)

Requisitos en la maquina de build
- Python 3.10+
- Inno Setup 6
- GitHub CLI (gh) autenticado

Build local
1) powershell -ExecutionPolicy Bypass -File build.ps1
2) Ejecutable generado en: dist\RECA_ODS\RECA_ODS.exe

Installer
1) Abrir installer.iss con Inno Setup y compilar
2) Instalador generado en: installer\RECA_ODS_Setup.exe

Release automatizado
1) Ejecutar: powershell -ExecutionPolicy Bypass -File release.ps1 vX.Y.Z
2) Crea release en GitHub con assets:
   - RECA_ODS_Setup.exe
   - RECA_ODS_Setup.exe.sha256

Auto‑update
- La app revisa GitHub Releases en cada inicio.
- Si hay nueva version, descarga el instalador, valida SHA256 y actualiza en silencio.
