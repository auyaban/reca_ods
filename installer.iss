#define MyAppName "Sistema de Gestión ODS RECA"
#ifndef MyAppVersion
  #define MyAppVersion "1.0.0"
#endif
#define MyAppPublisher "RECA"
#define MyAppExeName "RECA_ODS.exe"

#include "installer_config.iss"

[Setup]
AppId={{3A7B7F07-7F0E-4B0B-BE7B-5F2F71A19E7D}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={localappdata}\Programs\{#MyAppName}
DefaultGroupName={#MyAppName}
OutputDir=installer
OutputBaseFilename=RECA_ODS_Setup
Compression=lzma
SolidCompression=yes
PrivilegesRequired=lowest
ArchitecturesInstallIn64BitMode=x64
WizardStyle=modern
SetupIconFile=logo\logo_reca.ico

[Files]
Source: "dist\\RECA_ODS\\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\\{#MyAppName}"; Filename: "{app}\\{#MyAppExeName}"
Name: "{userdesktop}\\{#MyAppName}"; Filename: "{app}\\{#MyAppExeName}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; GroupDescription: "Accesos directos:"

[Run]
Filename: "{app}\\{#MyAppExeName}"; Description: "Abrir {#MyAppName}"; Flags: nowait postinstall skipifsilent

[Code]
procedure CurStepChanged(CurStep: TSetupStep);
var
  EnvPath: string;
  EnvContent: string;
begin
  if CurStep = ssInstall then
  begin
    ForceDirectories(ExpandConstant('{userappdata}\\{#MyAppName}'));
    EnvPath := ExpandConstant('{userappdata}\\{#MyAppName}\\.env');
    EnvContent := 'SUPABASE_URL={#SupabaseUrl}' + #13#10 +
                  'SUPABASE_ANON_KEY={#SupabaseKey}' + #13#10 +
                  'BACKEND_URL={#BackendUrl}' + #13#10;
    SaveStringToFile(EnvPath, EnvContent, False);
  end;
end;
