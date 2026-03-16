# Automatizacion

Supabase Edge Functions para apoyar la automatizacion ODS.

## Funcion actual

- `extract-acta-ods`: recibe instrucciones estructuradas y opcionalmente `pdf_base64`, y devuelve JSON estructurado.
- Proveedor principal recomendado: OpenAI `gpt-5-mini`.
- Existe soporte opcional para Anthropic usando `provider_override=anthropic`, pero no es la ruta principal.

## Request esperado

```json
{
  "schema_name": "acta_ods_extraction",
  "schema": { "type": "object", "properties": {} },
  "source_label": "acta.pdf",
  "filename": "acta.pdf",
  "subject": "Asunto del correo",
  "text": "Instrucciones y contexto estructurado",
  "pdf_base64": "JVBERi0xLjQK...",
  "provider_override": "openai",
  "model_override": "gpt-5-mini"
}
```

Notas:
- `text` es opcional si se envia `pdf_base64`, pero normalmente se usa para pasar instrucciones guiadas por perfil.
- `provider_override` admite `openai` o `anthropic`.
- `model_override` permite forzar un modelo concreto por llamada.

## Secrets esperados en Supabase

- `OPENAI_API_KEY`
- `OPENAI_ACTA_EXTRACTION_MODEL` opcional, por defecto `gpt-5-mini`
- `CLAUDE_API_KEY` opcional, solo si se habilita Anthropic
- `CLAUDE_ACTA_EXTRACTION_MODEL` opcional, por defecto `claude-sonnet-4-6`
- `ACTA_EXTRACTION_SHARED_SECRET` recomendado para proteger la funcion cuando `verify_jwt = false`

## Seguridad

La funcion acepta un header compartido:

```http
x-acta-extraction-secret: <shared-secret>
```

Si `ACTA_EXTRACTION_SHARED_SECRET` existe en Supabase, ese header se vuelve obligatorio.

## Deploy

1. Instala Supabase CLI.
2. Inicia sesion: `supabase login`
3. Vincula el proyecto si hace falta: `supabase link --project-ref <project-ref>`
4. Define los secrets necesarios:

```powershell
supabase secrets set OPENAI_API_KEY=... ACTA_EXTRACTION_SHARED_SECRET=... --project-ref <project-ref>
```

5. Despliega la funcion:

```powershell
supabase functions deploy extract-acta-ods
```

## Uso desde RECA_ODS

Activar en el runtime de la app:

```env
AUTOMATION_LLM_EXTRACTION_ENABLED=1
SUPABASE_EDGE_ACTA_EXTRACTION_FUNCTION=extract-acta-ods
SUPABASE_EDGE_ACTA_EXTRACTION_SECRET=your-edge-shared-secret
```
