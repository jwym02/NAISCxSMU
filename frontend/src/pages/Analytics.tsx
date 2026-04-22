import { useState, useEffect } from "react";
import { Header } from "../components/Header";
import { EventChart } from "../components/EventChart";
import { runQuery, QueryResponse } from "../api/query";
import { fetchTimeseries, TimeseriesResponse } from "../api/pipeline";

function exportCSV(result: QueryResponse) {
  const cols = Object.keys(result.results[0] ?? {});
  if (!cols.length) return;
  const rows = [
    cols.join(","),
    ...result.results.map((row) =>
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

export function Analytics() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [timeseries, setTimeseries] = useState<TimeseriesResponse | null>(null);
  const [chartType, setChartType] = useState<"line" | "bar">("line");

  useEffect(() => {
    fetchTimeseries(12).then(setTimeseries).catch(console.error);
  }, []);

  const handleRun = async () => {
    if (!query.trim()) return;
    setRunning(true);
    setQueryError(null);
    try {
      const data = await runQuery(query);
      setResult(data);
    } catch (e: unknown) {
      setQueryError(e instanceof Error ? e.message : "Query failed");
    } finally {
      setRunning(false);
    }
  };

  const columns = result?.results[0] ? Object.keys(result.results[0]) : [];

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

          {result?.sql && (
            <div className="sql-block">
              <div className="sql-label">GENERATED SQL</div>
              <pre className="sql-code">{result.sql}</pre>
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
                  {result.results.length} row
                  {result.results.length !== 1 ? "s" : ""}
                </span>
                {result.execution_time_ms !== undefined && (
                  <span>· {result.execution_time_ms}ms</span>
                )}
                {result.results.length > 0 && (
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
                    {result.results.length === 0 ? (
                      <tr>
                        <td colSpan={columns.length} className="no-results">
                          No results
                        </td>
                      </tr>
                    ) : (
                      result.results.map((row, i) => (
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
