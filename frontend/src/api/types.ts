export type ProcessResult = {
  job_id: string;
  file_name: string;
  file_format: string;
  status: "success" | "partial_success" | "failed";
  events_processed: number;
  events_routed: Record<string, number>;
  events_in_review: number;
  errors: string[];
  timestamp: string;
  records_preview?: Record<string, unknown>[] | null;
};

export type ParsePreviewOut = {
  detected_format: string;
  record_count: number;
  records: Record<string, unknown>[];
  parse_errors: unknown[];
  input_char_count: number;
  input_approx_tokens: number;
  tokenizer_version: string;
};

export type HealthResponse = {
  status: string;
  service: string;
  timestamp: string;
};

export type QueryRequest = {
  query: string;
  limit?: number;
  time_range_hours?: number;
};

export type QueryResponse = {
  original_query: string;
  generated_sql: string;
  rows: Record<string, unknown>[];
  row_count: number;
  execution_time_ms: number;
};
