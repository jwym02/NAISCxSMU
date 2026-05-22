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

// ── Review Queue ─────────────────────────────────────────────────────────────

export interface ReviewItem {
  job_id: string;
  source: string;
  event_type: string;
  message: string;
  severity: string;
  ai_category: string;
  ai_root_cause: string;
  ai_recommended_action: string;
  confidence_score: number;
  review_status: string;       // "pending" | "approved" | "rejected"
  timestamp: string;
  created_at?: string;
  review_reason?: string;
}

export interface ReviewQueueResponse {
  total_items: number;
  items: ReviewItem[];
}

export interface ReviewDecision {
  decision: "approved" | "rejected";
  notes?: string;
  category?: string;           // reviewer-corrected category (fed back to DynamoDB)
  severity?: string;           // reviewer-corrected severity (used for Kafka routing)
}

export interface ReviewDecisionResponse {
  job_id: string;
  decision: string;
  message: string;
  timestamp: string;
}

export async function fetchReviewQueue(): Promise<ReviewQueueResponse> {
  const res = await fetch(`${BASE}/queue`);
  if (!res.ok) throw new Error("Failed to fetch review queue");
  return res.json();
}

export async function fetchReviewQueueItem(jobId: string): Promise<ReviewItem & { raw_data?: string }> {
  const res = await fetch(`${BASE}/jobs/${jobId}`);
  if (!res.ok) throw new Error("Failed to fetch review item");
  return res.json();
}

export async function fetchRawLog(jobId: string): Promise<{ content: string; truncated: boolean }> {
  const res = await fetch(`${BASE}/logs/${jobId}/raw`);
  if (!res.ok) throw new Error("Raw file not available");
  const truncated = res.headers.get("X-Truncated") === "true";
  const content = await res.text();
  return { content, truncated };
}

export async function fetchCategories(): Promise<string[]> {
  const res = await fetch(`${BASE}/categories`);
  if (!res.ok) throw new Error("Failed to fetch categories");
  const data = await res.json();
  return data.categories as string[];
}

export async function addCategory(name: string): Promise<{ name: string; created: boolean }> {
  const res = await fetch(`${BASE}/categories`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: "Failed to add category" }));
    throw new Error(err.detail || "Failed to add category");
  }
  return res.json();
}

// ── Trend Alerts ─────────────────────────────────────────────────────────────

export interface TrendAlert {
  id: number;
  machine: string;
  pattern: string;
  predicted_severity: "warning" | "critical";
  estimated_time_to_critical: string;
  recommended_action: string;
  confidence: number;
  created_at: string;
}

export interface TrendAlertsResponse {
  alerts: TrendAlert[];
}

export async function fetchTrendAlerts(): Promise<TrendAlert[]> {
  const res = await fetch(`${BASE}/trend-alerts`);
  if (!res.ok) throw new Error("Failed to fetch trend alerts");
  const data: TrendAlertsResponse = await res.json();
  return data.alerts;
}

export async function submitReview(
  jobId: string,
  body: ReviewDecision
): Promise<ReviewDecisionResponse> {
  const res = await fetch(`${BASE}/queue/${jobId}/review`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: "Review failed" }));
    throw new Error(err.detail || "Review submission failed");
  }
  return res.json();
}
