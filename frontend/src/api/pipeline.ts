import { apiFetch } from "./client";
import type {
  HealthResponse,
  ParsePreviewOut,
  ProcessResult,
  QueryRequest,
  QueryResponse,
} from "./types";

const pipelineBase = () =>
  import.meta.env.VITE_PIPELINE_URL?.replace(/\/$/, "") || "http://localhost:8080";

const queryBase = () =>
  import.meta.env.VITE_QUERY_URL?.replace(/\/$/, "") || "http://localhost:8081";

export async function getPipelineHealth(): Promise<HealthResponse> {
  const r = await apiFetch(`${pipelineBase()}/health`);
  if (!r.ok) throw new Error(`Pipeline health: ${r.status}`);
  return r.json() as Promise<HealthResponse>;
}

export async function getQueryHealth(): Promise<HealthResponse> {
  const r = await apiFetch(`${queryBase()}/health`);
  if (!r.ok) throw new Error(`Query health: ${r.status}`);
  return r.json() as Promise<HealthResponse>;
}

export async function parsePreview(
  text: string,
  formatHint?: string
): Promise<ParsePreviewOut> {
  const r = await apiFetch(`${pipelineBase()}/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      text,
      format: formatHint || null,
    }),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    const detail =
      typeof data === "object" && data && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : r.statusText;
    throw new Error(detail || `Preview failed: ${r.status}`);
  }
  return data as ParsePreviewOut;
}

export async function processLogFile(
  file: File,
  formatHint?: string
): Promise<ProcessResult> {
  const fd = new FormData();
  fd.append("file", file);
  const q = formatHint ? `?format=${encodeURIComponent(formatHint)}` : "";
  const r = await apiFetch(`${pipelineBase()}/process${q}`, {
    method: "POST",
    body: fd,
  });

  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    if (
      typeof data === "object" &&
      data &&
      "job_id" in data &&
      "status" in data &&
      "events_processed" in data
    ) {
      return data as ProcessResult;
    }
    const detail =
      typeof data === "object" && data && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : r.statusText;
    throw new Error(detail || `Process failed: ${r.status}`);
  }
  return data as ProcessResult;
}

export async function runNlQuery(body: QueryRequest): Promise<QueryResponse> {
  const r = await apiFetch(`${queryBase()}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: body.query,
      limit: body.limit ?? 100,
      time_range_hours: body.time_range_hours ?? 24,
    }),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    const detail =
      typeof data === "object" && data && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : r.statusText;
    throw new Error(detail || `Query failed: ${r.status}`);
  }
  return data as QueryResponse;
}
