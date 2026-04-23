const BASE = "/query-api";

export interface QueryResponse {
  original_query: string;
  generated_sql: string;
  rows: Record<string, unknown>[];
  row_count: number;
  execution_time_ms: number;
}

export async function runQuery(query: string): Promise<QueryResponse> {
  const res = await fetch(`${BASE}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: "Query failed" }));
    throw new Error(err.detail || "Query failed");
  }
  return res.json();
}
