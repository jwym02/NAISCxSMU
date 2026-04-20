import { useCallback, useEffect, useState } from "react";
import {
  getJobStatus,
  getPipelineHealth,
  getQueryHealth,
  keywordSearch,
  parsePreview,
  processLogFile,
  runNlQuery,
} from "./api/pipeline";
import type {
  JobAccepted,
  KeywordSearchResponse,
  ParsePreviewOut,
  ProcessResult,
  QueryResponse,
} from "./api/types";
import { estimateApproxTokens } from "./lib/estimateTokens";
import { optimizeLogText } from "./lib/optimizeText";
import "./App.css";

const PREVIEW_MAX = 200_000;

type Tab = "parse" | "query";
type OutView = "summary" | "json" | "records";

async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

export default function App() {
  const [tab, setTab] = useState<Tab>("parse");

  const [pipeOk, setPipeOk] = useState<boolean | null>(null);
  const [queryOk, setQueryOk] = useState<boolean | null>(null);
  const [healthErr, setHealthErr] = useState<string | null>(null);

  const [file, setFile] = useState<File | null>(null);
  const [pasteText, setPasteText] = useState("");
  const [formatHint, setFormatHint] = useState<string>("");
  const [inputMode, setInputMode] = useState<"file" | "paste">("file");
  const [rawOptimized, setRawOptimized] = useState<"raw" | "optimized">("raw");
  const [drag, setDrag] = useState(false);

  const [busy, setBusy] = useState(false);
  const [procError, setProcError] = useState<string | null>(null);
  const [result, setResult] = useState<ProcessResult | null>(null);
  const [outView, setOutView] = useState<OutView>("summary");
  const [previewBusy, setPreviewBusy] = useState(false);
  const [previewErr, setPreviewErr] = useState<string | null>(null);
  const [previewData, setPreviewData] = useState<ParsePreviewOut | null>(null);
  const [copyHint, setCopyHint] = useState<string | null>(null);

  const [qText, setQText] = useState(
    "Show recent normalized events in the last 24 hours"
  );
  const [qBusy, setQBusy] = useState(false);
  const [qErr, setQErr] = useState<string | null>(null);
  const [qRes, setQRes] = useState<QueryResponse | null>(null);
  const [asyncMode, setAsyncMode] = useState(false);
  const [jobPollingId, setJobPollingId] = useState<string | null>(null);
  const [jobStatusLine, setJobStatusLine] = useState<string | null>(null);

  const [kw, setKw] = useState("temperature");
  const [kwBusy, setKwBusy] = useState(false);
  const [kwErr, setKwErr] = useState<string | null>(null);
  const [kwRes, setKwRes] = useState<KeywordSearchResponse | null>(null);

  const refreshHealth = useCallback(async () => {
    setHealthErr(null);
    try {
      await getPipelineHealth();
      setPipeOk(true);
    } catch {
      setPipeOk(false);
    }
    try {
      await getQueryHealth();
      setQueryOk(true);
    } catch {
      setQueryOk(false);
    }
  }, []);

  useEffect(() => {
    void refreshHealth();
  }, [refreshHealth]);

  useEffect(() => {
    if (!jobPollingId) return;
    const id = window.setInterval(() => {
      void (async () => {
        try {
          const j = await getJobStatus(jobPollingId);
          setJobStatusLine(`${j.status}${j.progress?.step ? ` · ${j.progress.step}` : ""}`);
          if (j.status === "completed" || j.status === "failed") {
            window.clearInterval(id);
            setJobPollingId(null);
            setJobStatusLine(null);
            if (j.result) {
              setResult(j.result as ProcessResult);
              setProcError(j.error || null);
            } else if (j.error) {
              setProcError(j.error);
            }
          }
        } catch (e) {
          window.clearInterval(id);
          setJobPollingId(null);
          setJobStatusLine(null);
          setProcError(e instanceof Error ? e.message : String(e));
        }
      })();
    }, 1000);
    return () => window.clearInterval(id);
  }, [jobPollingId]);

  const [filePreview, setFilePreview] = useState("");

  useEffect(() => {
    let cancelled = false;
    if (!file || inputMode !== "file") {
      setFilePreview("");
      return;
    }
    void (async () => {
      try {
        const t = await file.text();
        if (!cancelled) {
          setFilePreview(
            t.length > PREVIEW_MAX
              ? t.slice(0, PREVIEW_MAX) +
                  `\n\n… truncated preview (${t.length} chars total)`
              : t
          );
        }
      } catch {
        if (!cancelled) setFilePreview("(could not read file as text preview)");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [file, inputMode]);

  const displayedInput =
    inputMode === "paste"
      ? pasteText
      : filePreview || "";

  const textForApi =
    inputMode === "paste"
      ? pasteText
      : filePreview.split("\n\n… truncated preview")[0] ?? filePreview;

  const shownInput =
    rawOptimized === "optimized"
      ? optimizeLogText(displayedInput || "")
      : displayedInput || "";

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) {
      setFile(f);
      setInputMode("file");
      setProcError(null);
    }
  };

  const submitPreviewOnly = async () => {
    setPreviewErr(null);
    setPreviewData(null);
    if (!textForApi.trim()) {
      setPreviewErr("Load file preview or paste text before preview.");
      return;
    }
    setPreviewBusy(true);
    try {
      const p = await parsePreview(textForApi, formatHint || undefined);
      setPreviewData(p);
    } catch (e) {
      setPreviewErr(e instanceof Error ? e.message : String(e));
    } finally {
      setPreviewBusy(false);
    }
  };

  const submitProcess = async () => {
    setProcError(null);
    setResult(null);
    setPreviewData(null);
    let upload: File | null = file;
    if (inputMode === "paste") {
      if (!pasteText.trim()) {
        setProcError("Paste log text or switch to file upload.");
        return;
      }
      upload = new File([pasteText], "pasted.log", { type: "text/plain" });
    }
    if (!upload) {
      setProcError("Choose a file or paste log content.");
      return;
    }
    const hint = formatHint || undefined;
    setBusy(true);
    setJobStatusLine(null);
    try {
      const res = await processLogFile(upload, hint, { async: asyncMode });
      if (
        asyncMode &&
        res &&
        typeof res === "object" &&
        "job_id" in res &&
        "message" in res &&
        !("events_processed" in res)
      ) {
        const ja = res as JobAccepted;
        setJobPollingId(ja.job_id);
        setJobStatusLine("queued");
        setResult(null);
      } else {
        setResult(res as ProcessResult);
      }
    } catch (e) {
      setProcError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const submitKeywordSearch = async () => {
    setKwErr(null);
    setKwRes(null);
    if (!kw.trim()) {
      setKwErr("Enter keywords.");
      return;
    }
    setKwBusy(true);
    try {
      const res = await keywordSearch(kw, 50);
      setKwRes(res);
    } catch (e) {
      setKwErr(e instanceof Error ? e.message : String(e));
    } finally {
      setKwBusy(false);
    }
  };

  const submitQuery = async () => {
    setQErr(null);
    setQRes(null);
    if (!qText.trim()) {
      setQErr("Enter a question.");
      return;
    }
    setQBusy(true);
    try {
      const res = await runNlQuery({ query: qText, limit: 100, time_range_hours: 24 });
      setQRes(res);
    } catch (e) {
      setQErr(e instanceof Error ? e.message : String(e));
    } finally {
      setQBusy(false);
    }
  };

  const bannerClass =
    pipeOk === true && queryOk === true
      ? "ok"
      : pipeOk === false || queryOk === false
        ? "warn"
        : "warn";

  return (
    <main className="app">
      <h1>Smart Tool Log Parser</h1>
      <p className="sub">
        Upload or paste manufacturing logs, run the pipeline, and query stored
        events.
      </p>

      <div className={`banner ${bannerClass}`}>
        <span className="pill">
          Pipeline {pipeOk === true ? "● up" : pipeOk === false ? "○ down" : "…"}
        </span>
        <span className="pill">
          Query {queryOk === true ? "● up" : queryOk === false ? "○ down" : "…"}
        </span>
        {healthErr && <span>{healthErr}</span>}
        <button type="button" className="btn secondary" onClick={() => void refreshHealth()}>
          Refresh health
        </button>
      </div>

      <div className="row">
        <button
          type="button"
          className={`btn ${tab === "parse" ? "" : "secondary"}`}
          onClick={() => setTab("parse")}
        >
          Parse &amp; process
        </button>
        <button
          type="button"
          className={`btn ${tab === "query" ? "" : "secondary"}`}
          onClick={() => setTab("query")}
        >
          NL query
        </button>
      </div>

      {tab === "parse" && (
        <section className="card" aria-labelledby="parse-h">
          <h2 id="parse-h">Log input</h2>

          <div className="row">
            <label className="fmt" style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
              <input
                type="checkbox"
                checked={asyncMode}
                onChange={(e) => setAsyncMode(e.target.checked)}
                aria-label="Process in background (async job)"
              />
              Async job (poll status)
            </label>
          </div>

          <div className="row">
            <label className="fmt">
              Format hint
              <select
                value={formatHint}
                onChange={(e) => setFormatHint(e.target.value)}
                aria-label="Optional format hint for parser"
              >
                <option value="">Auto</option>
                <option value="json">json</option>
                <option value="csv">csv</option>
                <option value="xml">xml</option>
                <option value="log">log</option>
                <option value="txt">txt</option>
              </select>
            </label>
          </div>

          <div className="row">
            <button
              type="button"
              className={`btn ${inputMode === "file" ? "" : "secondary"}`}
              onClick={() => setInputMode("file")}
            >
              File
            </button>
            <button
              type="button"
              className={`btn ${inputMode === "paste" ? "" : "secondary"}`}
              onClick={() => setInputMode("paste")}
            >
              Paste
            </button>
          </div>

          {inputMode === "file" && (
            <div
              className={`dropzone ${drag ? "drag" : ""}`}
              onDragOver={(e) => {
                e.preventDefault();
                setDrag(true);
              }}
              onDragLeave={() => setDrag(false)}
              onDrop={onDrop}
              onClick={() => document.getElementById("file-in")?.click()}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  document.getElementById("file-in")?.click();
                }
              }}
            >
              <input
                id="file-in"
                type="file"
                accept=".log,.txt,.json,.csv,.xml,text/*"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) setFile(f);
                  setProcError(null);
                }}
              />
              {file ? (
                <strong>{file.name}</strong>
              ) : (
                <span>Drop a log file here or click to choose</span>
              )}
            </div>
          )}

          {inputMode === "paste" && (
            <textarea
              className="input-log"
              value={pasteText}
              onChange={(e) => setPasteText(e.target.value)}
              placeholder="Paste log lines here…"
              aria-label="Pasted log text"
            />
          )}

          {displayedInput !== "" && (
            <p className="sub" style={{ marginTop: 0 }}>
              Chars: {displayedInput.length} · ~tokens (heuristic):{" "}
              {estimateApproxTokens(displayedInput)}
            </p>
          )}

          {(inputMode === "file" && file) || inputMode === "paste" ? (
            <div className="toggle-row">
              <span>Input view:</span>
              <button
                type="button"
                className={rawOptimized === "raw" ? "btn" : "btn secondary"}
                onClick={() => setRawOptimized("raw")}
              >
                Raw
              </button>
              <button
                type="button"
                className={rawOptimized === "optimized" ? "btn" : "btn secondary"}
                onClick={() => setRawOptimized("optimized")}
              >
                Optimized
              </button>
            </div>
          ) : null}

          {displayedInput !== "" && (
            <textarea
              className="input-log"
              readOnly
              value={shownInput}
              aria-label={
                rawOptimized === "optimized"
                  ? "Optimized log preview"
                  : "Raw log preview"
              }
            />
          )}

          <div className="row">
            <button
              type="button"
              className="btn"
              disabled={busy}
              onClick={() => void submitProcess()}
            >
              {busy ? "Processing…" : "Run pipeline"}
            </button>
            <button
              type="button"
              className="btn secondary"
              disabled={previewBusy || busy || pipeOk === false}
              onClick={() => void submitPreviewOnly()}
            >
              {previewBusy ? "Parsing…" : "Preview parse only"}
            </button>
            {displayedInput !== "" && (
              <>
                <button
                  type="button"
                  className="btn secondary"
                  onClick={() =>
                    void copyToClipboard(
                      rawOptimized === "optimized"
                        ? optimizeLogText(displayedInput)
                        : displayedInput
                    ).then((ok) =>
                      setCopyHint(ok ? "Copied log text." : "Clipboard blocked.")
                    )
                  }
                >
                  Copy {rawOptimized === "optimized" ? "optimized" : "raw"} text
                </button>
              </>
            )}
          </div>
          {copyHint && (
            <p className="sub" role="status">
              {copyHint}
            </p>
          )}
          {jobStatusLine && (
            <p className="sub" role="status">
              Job: {jobStatusLine}
            </p>
          )}

          {procError && <div className="errors">{procError}</div>}
          {previewErr && <div className="errors">{previewErr}</div>}

          {previewData && (
            <div style={{ marginTop: "1rem" }}>
              <h3 className="sub" style={{ margin: "0 0 0.5rem", fontSize: "1rem" }}>
                Parse preview ({previewData.detected_format}) — {previewData.record_count}{" "}
                records · tokenizer v{previewData.tokenizer_version}
              </h3>
              <pre className="json-out" style={{ maxHeight: 280 }}>
                {JSON.stringify(previewData, null, 2)}
              </pre>
            </div>
          )}

          {result && (
            <>
              <div
                className={`status-msg ${
                  result.status === "success"
                    ? "success"
                    : result.status === "partial_success"
                      ? "partial"
                      : "failed"
                }`}
              >
                Status: <strong>{result.status}</strong> — job_id {result.job_id}{" "}
                — {result.events_processed} events — review queue{" "}
                {result.events_in_review}
              </div>
              {result.errors?.length > 0 && (
                <ul className="errors">
                  {result.errors.map((err, i) => (
                    <li key={i}>{err}</li>
                  ))}
                </ul>
              )}
              <div className="output-toggle">
                <button
                  type="button"
                  className={outView === "summary" ? "active" : ""}
                  onClick={() => setOutView("summary")}
                >
                  Summary
                </button>
                <button
                  type="button"
                  className={outView === "json" ? "active" : ""}
                  onClick={() => setOutView("json")}
                >
                  JSON
                </button>
                {(result.records_preview?.length ?? 0) > 0 && (
                  <button
                    type="button"
                    className={outView === "records" ? "active" : ""}
                    onClick={() => setOutView("records")}
                  >
                    Records ({result.records_preview?.length})
                  </button>
                )}
                <button
                  type="button"
                  className="btn secondary"
                  onClick={() =>
                    void copyToClipboard(JSON.stringify(result, null, 2)).then((ok) =>
                      setCopyHint(ok ? "Copied full JSON result." : "Clipboard blocked.")
                    )
                  }
                >
                  Copy result JSON
                </button>
              </div>
              {outView === "summary" ? (
                <div className="table-wrap">
                  <table className="result">
                    <thead>
                      <tr>
                        <th>Topic</th>
                        <th>Count</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(result.events_routed || {}).map(([k, v]) => (
                        <tr key={k}>
                          <td>{k}</td>
                          <td>{v}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : outView === "json" ? (
                <pre className="json-out">{JSON.stringify(result, null, 2)}</pre>
              ) : (
                <div className="table-wrap">
                  <table className="result">
                    <thead>
                      <tr>
                        {(result.records_preview?.[0]
                          ? Object.keys(result.records_preview[0] as object)
                          : []
                        ).map((k) => (
                          <th key={k}>{k}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(result.records_preview ?? []).map((row, i) => (
                        <tr key={i}>
                          {Object.values(row as Record<string, unknown>).map((v, j) => (
                            <td key={j}>{String(v)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </section>
      )}

      {tab === "query" && (
        <section className="card" aria-labelledby="query-h">
          <h2 id="query-h">Natural language query</h2>
          <h3 className="sub" style={{ fontSize: "0.95rem", marginBottom: "0.5rem" }}>
            Keyword search (<code>/search</code> · <code>search_text</code> + GIN)
          </h3>
          <div className="row">
            <input
              type="text"
              className="input-log"
              style={{ minHeight: "unset", flex: "1 1 200px" }}
              value={kw}
              onChange={(e) => setKw(e.target.value)}
              aria-label="Keyword search"
              placeholder="e.g. vacuum thermal machine_001"
            />
            <button
              type="button"
              className="btn"
              disabled={kwBusy || queryOk === false}
              onClick={() => void submitKeywordSearch()}
            >
              {kwBusy ? "Searching…" : "Search"}
            </button>
          </div>
          {kwErr && <div className="errors">{kwErr}</div>}
          {kwRes && kwRes.rows.length > 0 && (
            <div className="table-wrap" style={{ marginBottom: "1rem" }}>
              <table className="result">
                <thead>
                  <tr>
                    {Object.keys(kwRes.rows[0]).map((k) => (
                      <th key={k}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {kwRes.rows.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((v, j) => (
                        <td key={j}>{String(v)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {kwRes && kwRes.rows.length === 0 && (
            <p className="sub">No matches (try other terms or ingest logs first).</p>
          )}

          <h3 className="sub" style={{ fontSize: "0.95rem", margin: "1rem 0 0.5rem" }}>
            Natural language (LLM → SQL)
          </h3>
          <textarea
            className="input-log"
            value={qText}
            onChange={(e) => setQText(e.target.value)}
            aria-label="Natural language question"
          />
          <div className="row">
            <button
              type="button"
              className="btn"
              disabled={qBusy || queryOk === false}
              onClick={() => void submitQuery()}
            >
              {qBusy ? "Querying…" : "Run query"}
            </button>
          </div>
          {qErr && <div className="errors">{qErr}</div>}
          {qRes && (
            <>
              <p className="sub" style={{ margin: "0.5rem 0" }}>
                {qRes.row_count} rows in {qRes.execution_time_ms.toFixed(1)} ms
              </p>
              <div className="sql-box">{qRes.generated_sql}</div>
              <div className="table-wrap">
                <table className="result">
                  <thead>
                    <tr>
                      {qRes.rows[0]
                        ? Object.keys(qRes.rows[0]).map((k) => <th key={k}>{k}</th>)
                        : null}
                    </tr>
                  </thead>
                  <tbody>
                    {qRes.rows.map((row, i) => (
                      <tr key={i}>
                        {Object.values(row).map((v, j) => (
                          <td key={j}>{String(v)}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </section>
      )}
    </main>
  );
}
