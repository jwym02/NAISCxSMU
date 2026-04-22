const BASE = "/pipeline";

export interface Stats {
  events_processed: number;
  events_in_review: number;
  errors: number;
  jobs_today: number;
  severity_breakdown: Record<string, number>;
}

export interface TimeseriesPoint {
  hour: string;
  severity: string;
  count: number;
}

export interface TimeseriesResponse {
  hours: number;
  data: TimeseriesPoint[];
}

export interface UploadResponse {
  job_id: string;
  status: string;
  file_name: string;
  timestamp: string;
}

export async function fetchStats(): Promise<Stats> {
  const res = await fetch(`${BASE}/stats`);
  if (!res.ok) throw new Error("Failed to fetch stats");
  return res.json();
}

export async function fetchTimeseries(hours = 12): Promise<TimeseriesResponse> {
  const res = await fetch(`${BASE}/events/timeseries?hours=${hours}`);
  if (!res.ok) throw new Error("Failed to fetch timeseries");
  return res.json();
}

export async function uploadLog(
  file: File,
  format?: string
): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);
  if (format) form.append("file_format", format);
  const res = await fetch(`${BASE}/logs/upload`, { method: "POST", body: form });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: "Upload failed" }));
    throw new Error(err.detail || "Upload failed");
  }
  return res.json();
}
