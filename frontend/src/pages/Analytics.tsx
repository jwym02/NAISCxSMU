import { useState, useEffect, useCallback } from "react";
import { Header } from "../components/Header";
import { EventChart } from "../components/EventChart";
import { runQuery, QueryResponse } from "../api/query";
import { fetchTimeseries, TimeseriesResponse, TimeseriesPoint } from "../api/pipeline";

const QUERY_HISTORY_KEY = "analytics_query_history";
const MAX_HISTORY = 10;

function getQueryHistory(): string[] {
  try {
    const stored = localStorage.getItem(QUERY_HISTORY_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
}

function saveQueryHistory(query: string): void {
  try {
    const history = getQueryHistory();
    // Remove duplicate if it exists
    const filtered = history.filter(q => q !== query);
    // Add new query to front
    const updated = [query, ...filtered].slice(0, MAX_HISTORY);
    localStorage.setItem(QUERY_HISTORY_KEY, JSON.stringify(updated));
  } catch {
    // silently fail if localStorage is unavailable
  }
}

function exportCSV(result: QueryResponse) {
  const cols = result.rows.length > 0 ? Object.keys(result.rows[0] ?? {}) : [];
  if (!cols.length) return;
  const rows = [
    cols.join(","),
    ...result.rows.map((row) =>
      cols.map((c) => JSON.stringify(row[c] ?? "")).join(",")
    ),
  ];
  const blob = new Blob([rows.join("\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "results.csv";
  a.click();
  URL.revokeObjectURL(url);
}

function extractTimeseriesFromResults(result: QueryResponse): TimeseriesResponse | null {
  // Try to extract time-series data from query results
  // Looks for columns like: hour, severity, event_count or time, severity, count
  if (!result.rows || result.rows.length === 0) return null;

  const firstRow = result.rows[0];
  const hasHour = "hour" in firstRow;
  const hasTime = "time" in firstRow;
  const hasSeverity = "severity" in firstRow;
  const hasCount = "event_count" in firstRow || "count" in firstRow;

  // If already aggregated with time + severity + count, use it directly
  if ((hasHour || hasTime) && hasSeverity && hasCount) {
    const data = result.rows.map((row) => ({
      hour: String(hasHour ? row.hour : row.time),
      severity: String(row.severity || "unknown"),
      count: Number(hasCount ? (row.event_count || row.count) : 0),
    })) as TimeseriesPoint[];
    return { hours: 24, data };
  }

  // If raw events with timestamp and severity, aggregate by hour
  if (hasSeverity && ("timestamp" in firstRow)) {
    const aggregated: Record<string, Record<string, number>> = {};
    
    result.rows.forEach((row) => {
      const ts = String(row.timestamp || "");
      // Extract hour from ISO timestamp
      const hour = ts.substring(0, 13) + ":00"; // e.g., "2026-04-23T08:00"
      const severity = String(row.severity || "unknown");
      
      if (!aggregated[hour]) {
        aggregated[hour] = {};
      }
      aggregated[hour][severity] = (aggregated[hour][severity] || 0) + 1;
    });

    const data: TimeseriesPoint[] = [];
    Object.entries(aggregated).forEach(([hour, severities]) => {
      Object.entries(severities).forEach(([severity, count]) => {
        data.push({ hour, severity, count });
      });
    });

    return { hours: 24, data: data.sort((a, b) => a.hour.localeCompare(b.hour)) };
  }

  return null;
}

export function Analytics() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [timeseries, setTimeseries] = useState<TimeseriesResponse | null>(null);
  const [chartType, setChartType] = useState<"line" | "bar">("line");
  const [history, setHistory] = useState<string[]>([]);

  useEffect(() => {
    fetchTimeseries(12).then(setTimeseries).catch(console.error);
    setHistory(getQueryHistory());
  }, []);

  const handleRun = useCallback(async () => {
    if (!query.trim()) return;
    setRunning(true);
    setQueryError(null);
    try {
      const data = await runQuery(query);
      setResult(data);
      // Try to extract timeseries from query results
      const ts = extractTimeseriesFromResults(data);
      if (ts) {
        setTimeseries(ts);
      }
      // Save to history
      saveQueryHistory(query);
      setHistory(getQueryHistory());
    } catch (e: unknown) {
      setQueryError(e instanceof Error ? e.message : "Query failed");
    } finally {
      setRunning(false);
    }
  }, [query]);

  const columns = result?.rows && result.rows.length > 0 ? Object.keys(result.rows[0]) : [];

  return (
    <div className="page">
      <Header
        title="Analytics"
        subtitle="Natural language query & event analysis"
      />
      <div className="page-content">
        {/* Query input */}
        <div>
          <div className="section-label">QUERY</div>
          <div className="query-box">
            <span className="query-prompt">›</span>
            <input
              className="query-input"
              value={query}
              placeholder="Show me all critical errors from the last 24 hours…"
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleRun()}
            />
            <button className="btn-run" onClick={handleRun} disabled={running}>
              {running ? "…" : "RUN"}
            </button>
          </div>

          {/* Query History */}
          {history.length > 0 && (
            <div className="query-history">
              <div className="history-label">RECENT QUERIES</div>
              <div className="history-list">
                {history.map((h, i) => (
                  <button
                    key={i}
                    className="history-item"
                    onClick={() => {
                      setQuery(h);
                      setTimeout(() => {
                        const input = document.querySelector(".query-input") as HTMLInputElement;
                        if (input) input.focus();
                      }, 0);
                    }}
                    title={h}
                  >
                    {h.length > 60 ? h.substring(0, 60) + "…" : h}
                  </button>
                ))}
              </div>
            </div>
          )}

          {result?.generated_sql && (
            <div className="sql-block">
              <div className="sql-label">GENERATED SQL</div>
              <pre className="sql-code">{result.generated_sql}</pre>
            </div>
          )}
          {queryError && <p className="error-banner" style={{ marginTop: 12 }}>{queryError}</p>}
        </div>

        {/* Chart */}
        <div>
          <div className="section-label">ANALYSIS</div>
          <EventChart
            data={timeseries}
            chartType={chartType}
            onChartTypeChange={setChartType}
          />
        </div>

        {/* Results */}
        {result && (
          <div>
            <div className="section-label">RESULTS</div>
            <div className="results-panel">
              <div className="results-meta">
                <span>
                  {result.row_count} row
                  {result.row_count !== 1 ? "s" : ""}
                </span>
                {result.execution_time_ms !== undefined && (
                  <span>· {Math.round(result.execution_time_ms)}ms</span>
                )}
                {result.rows.length > 0 && (
                  <button
                    className="btn-export"
                    onClick={() => exportCSV(result)}
                  >
                    export ↓
                  </button>
                )}
              </div>

              <div className="table-wrapper">
                <table className="results-table">
                  <thead>
                    <tr>
                      {columns.map((col) => (
                        <th key={col}>{col.toUpperCase()}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.length === 0 ? (
                      <tr>
                        <td colSpan={columns.length} className="no-results">
                          No results
                        </td>
                      </tr>
                    ) : (
                      result.rows.map((row, i) => (
                        <tr key={i}>
                          {columns.map((col) => (
                            <td key={col}>{String(row[col] ?? "—")}</td>
                          ))}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
