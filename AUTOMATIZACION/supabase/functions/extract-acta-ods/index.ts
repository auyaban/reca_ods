type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

type ExtractRequest = {
  schema_name?: string;
  schema?: Record<string, JsonValue>;
  source_label?: string;
  filename?: string;
  subject?: string;
  text?: string;
  provider_override?: string;
  model_override?: string;
  pdf_base64?: string;
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-acta-extraction-secret",
};

const OPENAI_MODEL = Deno.env.get("OPENAI_ACTA_EXTRACTION_MODEL") || "gpt-5-mini";
const CLAUDE_MODEL = Deno.env.get("CLAUDE_ACTA_EXTRACTION_MODEL") || "claude-sonnet-4-6";

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function extractResponseJson(payload: Record<string, unknown>): Record<string, unknown> {
  const outputParsed = payload.output_parsed;
  if (outputParsed && typeof outputParsed === "object" && !Array.isArray(outputParsed)) {
    return outputParsed as Record<string, unknown>;
  }

  const outputText = payload.output_text;
  if (typeof outputText === "string" && outputText.trim()) {
    return JSON.parse(outputText) as Record<string, unknown>;
  }

  const output = Array.isArray(payload.output) ? payload.output : [];
  for (const item of output) {
    if (!item || typeof item !== "object") continue;
    const content = Array.isArray((item as { content?: unknown[] }).content)
      ? ((item as { content: unknown[] }).content)
      : [];
    for (const part of content) {
      if (!part || typeof part !== "object") continue;
      const json = (part as { json?: unknown }).json;
      if (json && typeof json === "object" && !Array.isArray(json)) {
        return json as Record<string, unknown>;
      }
      const text = (part as { text?: unknown }).text;
      if (typeof text === "string" && text.trim()) {
        return JSON.parse(text) as Record<string, unknown>;
      }
    }
  }

  throw new Error("OpenAI no devolvio un JSON estructurado util.");
}

function extractAnthropicJson(payload: Record<string, unknown>): Record<string, unknown> {
  const content = Array.isArray(payload.content) ? payload.content : [];
  const textParts: string[] = [];
  for (const item of content) {
    if (!item || typeof item !== "object") continue;
    const type = String((item as { type?: unknown }).type || "");
    if (type !== "text") continue;
    const text = String((item as { text?: unknown }).text || "").trim();
    if (text) {
      textParts.push(text);
    }
  }
  const joined = textParts.join("\n").trim();
  if (!joined) {
    throw new Error("Anthropic no devolvio texto util.");
  }
  const direct = joined.replace(/^```json\s*/i, "").replace(/```$/i, "").trim();
  try {
    return JSON.parse(direct) as Record<string, unknown>;
  } catch {
    const start = direct.indexOf("{");
    const end = direct.lastIndexOf("}");
    if (start >= 0 && end > start) {
      return JSON.parse(direct.slice(start, end + 1)) as Record<string, unknown>;
    }
  }
  throw new Error("Anthropic no devolvio un JSON parseable.");
}

function inferProvider(model: string, providerOverride: string): "openai" | "anthropic" {
  const provider = providerOverride.trim().toLowerCase();
  if (provider === "anthropic" || provider === "claude") {
    return "anthropic";
  }
  if (provider === "openai") {
    return "openai";
  }
  return model.startsWith("claude-") ? "anthropic" : "openai";
}

function buildInstructions(schemaName: string): string {
  return [
    "Eres un extractor estructurado de actas ODS en espanol.",
    "Tu trabajo es leer texto de actas PDF o PDFs completos que pueden venir rotos, desordenados o con OCR imperfecto.",
    "Cuando recibas un PDF, el PDF es la fuente primaria de verdad para extraer los campos.",
    "Usa el contexto textual adicional solo como guia de secciones, etiquetas y reglas, no como fuente de datos del documento.",
    "Debes devolver exclusivamente un JSON valido que cumpla el schema recibido.",
    "No incluyas texto adicional, explicaciones ni markdown.",
    "Usa cadenas cortas y limpias; no inventes informacion.",
    "Si un dato no aparece con suficiente claridad, devuelve cadena vacia, 0, false o lista vacia segun el tipo.",
    "No devuelvas campos de texto largo.",
    "Si recibes un perfil_documento, debes seguirlo estrictamente: usa solo las secciones permitidas, prioriza las etiquetas indicadas e ignora el ruido de otras secciones.",
    "Si el perfil indica campos_que_deben_ir_vacios, dejalos vacios aunque aparezcan otros numeros o textos parecidos en el documento.",
    "cargo_objetivo solo es valido si proviene de una etiqueta explicita como 'Cargo', 'Nombre de la vacante' o 'Cargo que ocupa'.",
    "Nunca tomes cargo_objetivo desde la seccion ASISTENTES ni desde texto libre no rotulado.",
    "Si el documento esta incompleto, ambiguo o inconsistente, marca extraction_status como needs_review.",
    "Si el texto no parece un acta util o es ilegible, marca extraction_status como invalid.",
    "Para interprete LSC prioriza SUMATORIA HORAS INTERPRETES; si no existe usa Total Tiempo.",
    "Si aparece la palabra fallido en asunto o contenido, marca is_fallido en true.",
    "participantes debe incluir solo nombre, cedula, discapacidad y genero cuando existan.",
    `El nombre del schema es ${schemaName}.`,
  ].join(" ");
}

async function callOpenAI(params: {
  apiKey: string;
  model: string;
  schemaName: string;
  schema: Record<string, JsonValue>;
  sourceLabel: string;
  filename: string;
  subject: string;
  text: string;
  pdfBase64: string;
}): Promise<Record<string, unknown>> {
  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: params.model,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text: buildInstructions(params.schemaName),
            },
          ],
        },
        {
          role: "user",
          content: [
            ...(params.pdfBase64
              ? [
                  {
                    type: "input_file",
                    filename: params.filename || "document.pdf",
                    file_data: `data:application/pdf;base64,${params.pdfBase64}`,
                  },
                ]
              : []),
            {
              type: "input_text",
              text: [
                `source_label: ${params.sourceLabel || "-"}`,
                `filename: ${params.filename || "-"}`,
                `subject: ${params.subject || "-"}`,
                ...(params.text ? ["", "texto_extraido:", params.text] : []),
              ].join("\n"),
            },
          ],
        },
      ],
      max_output_tokens: 2200,
      text: {
        format: {
          type: "json_schema",
          name: params.schemaName,
          strict: true,
          schema: params.schema,
        },
      },
    }),
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(`OpenAI request failed: ${response.status} ${JSON.stringify(payload)}`);
  }

  return {
    data: extractResponseJson(payload as Record<string, unknown>),
    model: params.model,
    usage: (payload as { usage?: unknown }).usage || null,
    provider: "openai",
    raw: payload,
  };
}

async function callAnthropic(params: {
  apiKey: string;
  model: string;
  schemaName: string;
  schema: Record<string, JsonValue>;
  sourceLabel: string;
  filename: string;
  subject: string;
  text: string;
  pdfBase64: string;
}): Promise<Record<string, unknown>> {
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": params.apiKey,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: params.model,
      max_tokens: 2200,
      system: [
        buildInstructions(params.schemaName),
        "Debes responder solo JSON valido.",
        "Si el JSON no cumple el schema dado, corrige antes de responder.",
        `Schema requerido: ${JSON.stringify(params.schema)}`,
      ].join("\n\n"),
      messages: [
        {
          role: "user",
          content: [
            ...(params.pdfBase64
              ? [
                  {
                    type: "document",
                    source: {
                      type: "base64",
                      media_type: "application/pdf",
                      data: params.pdfBase64,
                    },
                  },
                ]
              : []),
            {
              type: "text",
              text: [
                `source_label: ${params.sourceLabel || "-"}`,
                `filename: ${params.filename || "-"}`,
                `subject: ${params.subject || "-"}`,
                ...(params.text ? ["", "texto_extraido:", params.text] : []),
              ].join("\n"),
            },
          ],
        },
      ],
    }),
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(`Anthropic request failed: ${response.status} ${JSON.stringify(payload)}`);
  }

  return {
    data: extractAnthropicJson(payload as Record<string, unknown>),
    model: params.model,
    usage: (payload as { usage?: unknown }).usage || null,
    provider: "anthropic",
    raw: payload,
  };
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const sharedSecret = String(Deno.env.get("ACTA_EXTRACTION_SHARED_SECRET") || "").trim();
    if (sharedSecret) {
      const providedSecret = String(request.headers.get("x-acta-extraction-secret") || "").trim();
      if (!providedSecret || providedSecret !== sharedSecret) {
        return jsonResponse({ error: "Unauthorized extract-acta-ods request." }, 401);
      }
    }

    const apiKey = Deno.env.get("OPENAI_API_KEY");
    const claudeApiKey = Deno.env.get("CLAUDE_API_KEY");

    const body = (await request.json()) as ExtractRequest;
    const schemaName = String(body.schema_name || "").trim() || "acta_ods_extraction";
    const schema = body.schema;
    const sourceLabel = String(body.source_label || "").trim();
    const filename = String(body.filename || "").trim();
    const subject = String(body.subject || "").trim();
    const text = String(body.text || "").trim();
    const pdfBase64 = String(body.pdf_base64 || "").trim();
    const providerOverride = String(body.provider_override || "").trim();
    const modelOverride = String(body.model_override || "").trim();
    const model = modelOverride || (providerOverride.toLowerCase() === "anthropic" ? CLAUDE_MODEL : OPENAI_MODEL);
    const provider = inferProvider(model, providerOverride);

    if (!schema || typeof schema !== "object" || Array.isArray(schema)) {
      return jsonResponse({ error: "schema is required and must be an object." }, 400);
    }
    if (!text && !pdfBase64) {
      return jsonResponse({ error: "text or pdf_base64 is required." }, 400);
    }
    if (provider === "anthropic" && !claudeApiKey) {
      return jsonResponse({ error: "Missing CLAUDE_API_KEY in Supabase secrets." }, 500);
    }
    if (provider === "openai" && !apiKey) {
      return jsonResponse({ error: "Missing OPENAI_API_KEY in Supabase secrets." }, 500);
    }
    const responsePayload = provider === "anthropic"
      ? await callAnthropic({
          apiKey: claudeApiKey || "",
          model,
          schemaName,
          schema,
          sourceLabel,
          filename,
          subject,
          text,
          pdfBase64,
        })
      : await callOpenAI({
          apiKey: apiKey || "",
          model,
          schemaName,
          schema,
          sourceLabel,
          filename,
          subject,
          text,
          pdfBase64,
        });

    return jsonResponse(responsePayload);
  } catch (error) {
    return jsonResponse(
      {
        error: "Unhandled extract-acta-ods error.",
        details: error instanceof Error ? error.message : String(error),
      },
      500,
    );
  }
});
