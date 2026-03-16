#define MyAppName "Sistema de Gestión ODS RECA"
#ifndef MyAppVersion
  #define MyAppVersion "1.0.0"
#endif
#define MyAppPublisher "RECA"
#define MyAppExeName "RECA_ODS.exe"

#ifndef SupabaseAuthEmail
  #define SupabaseAuthEmail ""
#endif
#ifndef SupabaseAuthPassword
  #define SupabaseAuthPassword ""
#endif
#ifndef SupabaseEdgeActaExtractionSecret
  #define SupabaseEdgeActaExtractionSecret ""
#endif

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
Source: "dist\\installer_payload\\google-service-account.json"; DestDir: "{userappdata}\\{#MyAppName}\\secrets"; DestName: "google-service-account.json"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
Name: "{group}\\{#MyAppName}"; Filename: "{app}\\{#MyAppExeName}"
Name: "{userdesktop}\\{#MyAppName}"; Filename: "{app}\\{#MyAppExeName}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; GroupDescription: "Accesos directos:"

[Run]
Filename: "{app}\\{#MyAppExeName}"; Description: "Abrir {#MyAppName}"; Flags: nowait postinstall skipifsilent

[Code]
function LoadExistingEnvContent(const EnvPath: string): string;
begin
  if not LoadStringFromFile(EnvPath, Result) then
    Result := '';
end;

function ExtractEnvValue(const EnvContent, Key: string): string;
var
  SearchKey: string;
  StartPos: Integer;
  EndPos: Integer;
begin
  Result := '';
  SearchKey := Key + '=';
  StartPos := Pos(SearchKey, EnvContent);
  if StartPos = 0 then
    Exit;

  StartPos := StartPos + Length(SearchKey);
  EndPos := StartPos;
  while (EndPos <= Length(EnvContent)) and
        (EnvContent[EndPos] <> #13) and
        (EnvContent[EndPos] <> #10) do
    EndPos := EndPos + 1;

  Result := Copy(EnvContent, StartPos, EndPos - StartPos);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  EnvPath: string;
  EnvContent: string;
  ExistingEnvContent: string;
  ExistingGoogleServiceAccountPath: string;
  GoogleServiceAccountPath: string;
begin
  if CurStep = ssInstall then
  begin
    ForceDirectories(ExpandConstant('{userappdata}\\{#MyAppName}'));
    ForceDirectories(ExpandConstant('{userappdata}\\{#MyAppName}\\secrets'));
    EnvPath := ExpandConstant('{userappdata}\\{#MyAppName}\\.env');
    ExistingEnvContent := LoadExistingEnvContent(EnvPath);
    ExistingGoogleServiceAccountPath := ExtractEnvValue(ExistingEnvContent, 'GOOGLE_SERVICE_ACCOUNT_FILE');
    EnvContent := 'SUPABASE_URL={#SupabaseUrl}' + #13#10 +
                  'SUPABASE_ANON_KEY={#SupabaseKey}' + #13#10 +
                  'SUPABASE_AUTH_EMAIL={#SupabaseAuthEmail}' + #13#10 +
                  'SUPABASE_AUTH_PASSWORD={#SupabaseAuthPassword}' + #13#10 +
                  'BACKEND_URL={#BackendUrl}' + #13#10 +
                  'GOOGLE_DRIVE_SHARED_FOLDER_ID={#GoogleDriveSharedFolderId}' + #13#10 +
                  'GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME={#GoogleDriveTemplateSpreadsheetName}' + #13#10 +
                  'AUTOMATION_LLM_EXTRACTION_ENABLED=0' + #13#10 +
                  'SUPABASE_EDGE_ACTA_EXTRACTION_FUNCTION=extract-acta-ods' + #13#10 +
                  'SUPABASE_EDGE_ACTA_EXTRACTION_SECRET={#SupabaseEdgeActaExtractionSecret}' + #13#10 +
                  'ODS_AUTOMATION_TEST_ENABLED=0' + #13#10;
    if {#HasGoogleServiceAccount} = 1 then
      GoogleServiceAccountPath := '{#GoogleServiceAccountInstalledPath}'
    else if ExistingGoogleServiceAccountPath <> '' then
      GoogleServiceAccountPath := ExistingGoogleServiceAccountPath
    else if FileExists(ExpandConstant('{userappdata}\\{#MyAppName}\\secrets\\google-service-account.json')) then
      GoogleServiceAccountPath := '{#GoogleServiceAccountInstalledPath}'
    else
      GoogleServiceAccountPath := '';
    EnvContent := EnvContent + 'GOOGLE_SERVICE_ACCOUNT_FILE=' + GoogleServiceAccountPath + #13#10;
    SaveStringToFile(EnvPath, EnvContent, False);
  end;
end;
