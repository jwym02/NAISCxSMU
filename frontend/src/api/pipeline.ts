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
