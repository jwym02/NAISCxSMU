import { useState, useEffect, useCallback, useRef } from "react";
import "./ReviewQueue.css";
import { Header } from "../components/Header";
import { FileUpload } from "../components/FileUpload";
import { SummaryPanel } from "../components/SummaryPanel";
import {
  fetchStats,
  fetchReviewQueue,
  fetchRawLog,
  fetchCategories,
  addCategory,
  submitReview,
  fetchTrendAlerts,
  Stats,
  ReviewItem,
  TrendAlert,
} from "../api/pipeline";

// ── ReviewCard ────────────────────────────────────────────────────────────────

const SEVERITY_OPTIONS = ["critical", "error", "warning", "info"] as const;
type SeverityLevel = typeof SEVERITY_OPTIONS[number];

interface ReviewCardProps {
  item: ReviewItem;
  categories: string[];
  onDecision: (jobId: string, decision: "approved" | "rejected", notes: string, category: string, severity: string) => void;
  onCategoryAdded: (name: string) => void;
  disabled: boolean;
}

function ReviewCard({ item, categories, onDecision, onCategoryAdded, disabled }: ReviewCardProps) {
  const [notes, setNotes]                   = useState("");
  const [category, setCategory]             = useState(item.ai_category || "unknown");
  const [severity, setSeverity]             = useState<SeverityLevel>(
    (item.severity?.toLowerCase() as SeverityLevel) || "info"
  );
  const [showRaw, setShowRaw]               = useState(false);
  const [rawFile, setRawFile]               = useState<{ content: string; truncated: boolean } | null>(null);
  const [loadingRaw, setLoadingRaw]         = useState(false);
  const [rawError, setRawError]             = useState<string | null>(null);
  const [addingNew, setAddingNew]           = useState(false);
  const [newCatName, setNewCatName]         = useState("");
  const [addingError, setAddingError]       = useState<string | null>(null);
  const [confirmingForward, setConfirmingForward] = useState(false);

  const confidencePct = Math.round((item.confidence_score ?? 0) * 100);
  const confidenceColor =
    confidencePct >= 70 ? "#22c55e" :
    confidencePct >= 40 ? "#f59e0b" : "#ef4444";

  const isCritical = severity === "critical";
  const canRoute = category && category !== "unknown";

  const handleToggleRaw = async () => {
    if (showRaw) { setShowRaw(false); return; }
    setShowRaw(true);
    if (rawFile) return;
    setLoadingRaw(true);
    setRawError(null);
    try {
      const data = await fetchRawLog(item.job_id);
      setRawFile(data);
    } catch {
      setRawError("Could not load raw file from storage.");
    } finally {
      setLoadingRaw(false);
    }
  };

  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value;
    if (val === "__new__") {
      setAddingNew(true);
      setNewCatName("");
      setAddingError(null);
    } else {
      setCategory(val);
    }
  };

  const handleSaveNewCategory = async () => {
    const trimmed = newCatName.trim().toLowerCase();
    if (!trimmed) return;
    try {
      await addCategory(trimmed);
      onCategoryAdded(trimmed);
      setCategory(trimmed);
      setAddingNew(false);
    } catch (e: unknown) {
      setAddingError(e instanceof Error ? e.message : "Invalid category name");
    }
  };

  const handleRouteClick = () => {
    if (isCritical) {
      setConfirmingForward(true);
    } else {
      onDecision(item.job_id, "approved", notes, category, severity);
    }
  };

  return (
    <div className="review-card">
      <div className="review-card-header">
        <div className="review-header-top">
          <select
            className={`severity-select severity-select-${severity}`}
            value={severity}
            onChange={(e) => setSeverity(e.target.value as SeverityLevel)}
            disabled={disabled}
            title="Set severity (your choice overrides the AI)"
          >
            {SEVERITY_OPTIONS.map((s) => (
              <option key={s} value={s}>{s.toUpperCase()}</option>
            ))}
          </select>
          {severity !== (item.severity?.toLowerCase() ?? "info") && (
            <span className="severity-changed-hint">↑ changed by reviewer</span>
          )}
          <span className="review-confidence" style={{ color: confidenceColor }}>
            {confidencePct}% confidence
          </span>
        </div>
        <div className="review-header-bottom">
          <span className="review-source-label">Source:</span>
          <span className="review-source">{item.source}</span>
          <button className="btn-view-file" onClick={handleToggleRaw} disabled={loadingRaw}>
            {showRaw ? "Hide" : "View"} Raw File
          </button>
        </div>
      </div>

      <div className="review-card-body">
        {/* Event details — most important for human decision-making */}
        <div className="review-field review-event-message">
          <label>Event</label>
          <p className="review-text review-message">{item.message || "—"}</p>
        </div>
        <div className="review-meta-row">
          <span className="review-meta-item">
            <span className="review-meta-label">Type</span>
            <span className="review-meta-value">{item.event_type || "unknown"}</span>
          </span>
          <span className="review-meta-item">
            <span className="review-meta-label">Time</span>
            <span className="review-meta-value">
              {item.timestamp ? new Date(item.timestamp).toLocaleString() : "—"}
            </span>
          </span>
          <span className="review-meta-item">
            <span className="review-meta-label">Flagged</span>
            <span className="review-meta-value">{item.review_reason || "Low confidence"}</span>
          </span>
        </div>

        <div className="review-divider" />

        {/* AI analysis */}
        <div className="review-field">
          <label>
            Category
            {!canRoute && <span className="review-label-hint"> — select one to enable routing</span>}
          </label>
          {addingNew ? (
            <div className="review-category-add">
              <input
                className="review-input"
                value={newCatName}
                onChange={(e) => setNewCatName(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") handleSaveNewCategory(); if (e.key === "Escape") setAddingNew(false); }}
                placeholder="new_category_name"
                autoFocus
              />
              <button className="btn-category-save" onClick={handleSaveNewCategory}>Save</button>
              <button className="btn-category-cancel" onClick={() => setAddingNew(false)}>Cancel</button>
              {addingError && <span className="review-add-error">{addingError}</span>}
            </div>
          ) : (
            <select
              className="review-select"
              value={category}
              onChange={handleSelectChange}
              disabled={disabled}
            >
              {categories.map((c) => (
                <option key={c} value={c}>{c}</option>
              ))}
              {!categories.includes(category) && category && (
                <option value={category}>{category}</option>
              )}
              <option value="__new__">+ Add new category…</option>
            </select>
          )}
        </div>
        <div className="review-field">
          <label>Root cause</label>
          <p className="review-text">{item.ai_root_cause || "—"}</p>
        </div>
        <div className="review-field">
          <label>Recommended action</label>
          <p className="review-text">{item.ai_recommended_action || "—"}</p>
        </div>
        <div className="review-field">
          <label>Notes (optional)</label>
          <textarea
            className="review-textarea"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            rows={2}
            placeholder="Add context for this decision…"
            disabled={disabled}
          />
        </div>
      </div>

      {showRaw && (
        <div className="review-file-data">
          {loadingRaw && <p className="review-file-loading">Loading raw file…</p>}
          {rawError && <p className="review-file-error">{rawError}</p>}
          {rawFile && (
            <>
              {rawFile.truncated && (
                <p className="review-file-truncated">Showing first 500 KB — file truncated</p>
              )}
              <pre>{rawFile.content}</pre>
            </>
          )}
        </div>
      )}

      {/* CRITICAL forward confirmation strip */}
      {confirmingForward && (
        <div className="review-confirm-strip">
          <span>Forward this CRITICAL event to the live pipeline?</span>
          <button
            className="btn-confirm-forward"
            onClick={() => { setConfirmingForward(false); onDecision(item.job_id, "approved", notes, category, severity); }}
          >
            Yes, Forward
          </button>
          <button className="btn-confirm-cancel" onClick={() => setConfirmingForward(false)}>
            Cancel
          </button>
        </div>
      )}

      <div className="review-card-actions">
        <button
          className="btn-approve"
          disabled={disabled || !canRoute || confirmingForward}
          onClick={handleRouteClick}
          title={!canRoute ? "Select a category before routing" : undefined}
        >
          ↑ Route to Pipeline
        </button>
        <button
          className="btn-reject"
          disabled={disabled || confirmingForward}
          onClick={() => onDecision(item.job_id, "rejected", notes, category, severity)}
        >
          — Dismiss
        </button>
      </div>
    </div>
  );
}

// ── ReviewQueueOverlay ────────────────────────────────────────────────────────

interface ReviewQueueOverlayProps {
  onClose: () => void;
  onReviewed: () => void;
}

function ReviewQueueOverlay({ onClose, onReviewed }: ReviewQueueOverlayProps) {
  const [items,      setItems]      = useState<ReviewItem[]>([]);
  const [loading,    setLoading]    = useState(true);
  const [error,      setError]      = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<Set<string>>(new Set());
  const [resolved,   setResolved]   = useState<Set<string>>(new Set());
  const [filterSeverity, setFilterSeverity] = useState<string>("all");
  const [filterSource, setFilterSource] = useState<string>("all");
  const [categories, setCategories] = useState<string[]>([]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [queueData, cats] = await Promise.all([fetchReviewQueue(), fetchCategories()]);
      setItems(queueData.items);
      setCategories(cats);
    } catch (err) {
      setError("Could not load review queue");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleDecision = async (
    jobId: string,
    decision: "approved" | "rejected",
    notes: string,
    category: string,
    severity: string,
  ) => {
    setSubmitting((prev) => new Set(prev).add(jobId));
    try {
      await submitReview(jobId, { decision, notes, category, severity });
      setResolved((prev) => new Set(prev).add(jobId));
      onReviewed();  // refresh dashboard stats
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Submission failed";
      setError(msg);
    } finally {
      setSubmitting((prev) => { const s = new Set(prev); s.delete(jobId); return s; });
    }
  };

  const pending = items.filter((i) => !resolved.has(i.job_id));

  // Apply filters
  const filteredPending = pending.filter((item) => {
    if (filterSeverity !== "all" && item.severity !== filterSeverity) return false;
    if (filterSource !== "all" && item.source !== filterSource) return false;
    return true;
  });

  // Get unique values for filter options
  const severities = Array.from(new Set(items.map((i) => i.severity))).sort();
  const sources = Array.from(new Set(items.map((i) => i.source))).sort();

  return (
    <>
      {/* Backdrop */}
      <div className="overlay-backdrop" onClick={onClose} />

      {/* Drawer */}
      <aside className="review-drawer">
        <div className="review-drawer-header">
          <h2 className="review-drawer-title">
            Review Queue
            {pending.length > 0 && (
              <span className="review-badge">{pending.length}</span>
            )}
          </h2>
          <div className="review-drawer-controls">
            <button className="btn-icon" onClick={load} title="Refresh">↻</button>
            <button className="btn-icon" onClick={onClose} title="Close">✕</button>
          </div>
        </div>

        <div className="review-drawer-body">
          {loading && <p className="review-status-msg">Loading…</p>}
          {error   && <p className="review-status-msg review-error">{error}</p>}

          {!loading && !error && (
            <>
              {/* Filters */}
              <div className="review-filters">
                <div className="filter-group">
                  <label>Severity</label>
                  <select 
                    value={filterSeverity} 
                    onChange={(e) => setFilterSeverity(e.target.value)}
                    className="filter-select"
                  >
                    <option value="all">All</option>
                    {severities.map((sev) => (
                      <option key={sev} value={sev}>{sev}</option>
                    ))}
                  </select>
                </div>

                <div className="filter-group">
                  <label>Source</label>
                  <select 
                    value={filterSource} 
                    onChange={(e) => setFilterSource(e.target.value)}
                    className="filter-select"
                  >
                    <option value="all">All</option>
                    {sources.map((src) => (
                      <option key={src} value={src}>{src}</option>
                    ))}
                  </select>
                </div>

                <button
                  className="filter-reset"
                  onClick={() => {
                    setFilterSeverity("all");
                    setFilterSource("all");
                  }}
                >
                  Reset
                </button>
              </div>

              {/* Results */}
              {pending.length === 0 && (
                <p className="review-status-msg">No items pending review.</p>
              )}

              {pending.length > 0 && filteredPending.length === 0 && (
                <p className="review-status-msg">No items match the selected filters.</p>
              )}

              {filteredPending.length > 0 && (
                <p className="filter-results">
                  Showing {filteredPending.length} of {pending.length} items
                </p>
              )}
            </>
          )}

          {!loading && pending.map((item) => (
            (filterSeverity === "all" || item.severity === filterSeverity) &&
            (filterSource === "all" || item.source === filterSource) ? (
              <ReviewCard
                key={item.job_id}
                item={item}
                categories={categories}
                onDecision={handleDecision}
                onCategoryAdded={(name) => setCategories((prev) => [...prev, name].sort())}
                disabled={submitting.has(item.job_id)}
              />
            ) : null
          ))}
        </div>
      </aside>
    </>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

const POLLING_INTERVAL = 30000; // 30 seconds
const RETRY_BASE_DELAY = 2000; // 2 seconds
const MAX_RETRIES = 3;

export function Dashboard() {
  const [stats,          setStats]          = useState<Stats | null>(null);
  const [loading,        setLoading]        = useState(true);
  const [error,          setError]          = useState<string | null>(null);
  const [reviewOpen,     setReviewOpen]     = useState(false);
  const [retryCount,     setRetryCount]     = useState(0);
  const [trendAlerts,    setTrendAlerts]    = useState<TrendAlert[]>([]);
  const [alertsOpen,     setAlertsOpen]     = useState(false);
  const wsRef            = useRef<WebSocket | null>(null);
  const pollingRef       = useRef<ReturnType<typeof setInterval> | null>(null);
  const alertsRef        = useRef<HTMLDivElement | null>(null);

  // Exponential backoff retry logic
  const loadStats = useCallback(async (attempt = 0) => {
    if (attempt === 0) {
      setLoading(true);
      setError(null);
    }

    try {
      const data = await fetchStats();
      setStats(data);
      setError(null);
      setRetryCount(0);
      setLoading(false);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Could not reach pipeline service";
      
      if (attempt < MAX_RETRIES) {
        const delay = RETRY_BASE_DELAY * Math.pow(2, attempt);
        setError(`Retrying... (${attempt + 1}/${MAX_RETRIES})`);
        setRetryCount(attempt + 1);
        
        await new Promise((resolve) => setTimeout(resolve, delay));
        return loadStats(attempt + 1);
      } else {
        setError(msg);
        setRetryCount(MAX_RETRIES);
        setLoading(false);
      }
    }
  }, []);

  const loadTrendAlerts = useCallback(async () => {
    try {
      const alerts = await fetchTrendAlerts();
      setTrendAlerts(alerts);
    } catch {
      // Silently fail — trend alerts are non-critical
    }
  }, []);

  // Close trend alerts dropdown when clicking outside
  useEffect(() => {
    const handleOutsideClick = (e: MouseEvent) => {
      if (alertsRef.current && !alertsRef.current.contains(e.target as Node)) {
        setAlertsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, []);

  // Initial load + polling + WebSocket
  useEffect(() => {
    let isMounted = true;

    // Start initial load
    loadStats();
    loadTrendAlerts();

    // Setup WebSocket
    const setupWs = () => {
      if (!isMounted) return;
      
      try {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws/updates`;
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          if (isMounted) {
            console.log("WebSocket connected");
            wsRef.current = ws;
          }
        };

        ws.onmessage = (event) => {
          if (!isMounted) return;
          try {
            const message = JSON.parse(event.data);
            if (message.type === "stats_update") {
              setStats((prev) => prev ? { ...prev, ...message.data } : message.data);
            } else if (message.type === "review_queue_update") {
              loadStats();
            }
          } catch (parseErr) {
            console.error("Failed to parse WebSocket message", parseErr);
          }
        };

        ws.onerror = (err) => {
          console.error("WebSocket error", err);
        };

        ws.onclose = () => {
          if (isMounted) {
            console.log("WebSocket disconnected, retrying in 5s");
            wsRef.current = null;
            setTimeout(setupWs, 5000);
          }
        };
      } catch (err) {
        console.error("Failed to setup WebSocket", err);
      }
    };

    setupWs();

    // Setup polling
    pollingRef.current = setInterval(() => {
      if (isMounted) {
        loadStats();
        loadTrendAlerts();
      }
    }, POLLING_INTERVAL);

    // Cleanup
    return () => {
      isMounted = false;
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
      }
    };
  }, [loadStats, loadTrendAlerts]);

  const handleUploaded = () => {
    // Refresh stats after a brief delay so the backend has time to process
    setTimeout(() => { loadStats(); loadTrendAlerts(); }, 1500);
  };

  const resolveAlert = (id: number) =>
    setTrendAlerts(prev => prev.filter(a => a.id !== id));

  const pendingCount = stats?.events_in_review ?? 0;

  // Show loading state until first successful load or all retries exhausted
  if (loading) {
    return (
      <div className="page">
        <Header
          title="Dashboard"
          subtitle="Log ingestion & system summary"
          jobsToday={undefined}
        />
        <div className="page-content">
          {error && <p className="error-banner">{error}</p>}
          <p className="review-status-msg" style={{ marginTop: "2rem" }}>
            {retryCount > 0
              ? `Connecting... (Retry ${retryCount}/${MAX_RETRIES})`
              : "Loading dashboard..."}
          </p>
        </div>
      </div>
    );
  }

  // Content render: stats should be non-null due to loading guard above
  return (
    <div className="page">
      <Header
        title="Dashboard"
        subtitle="Log ingestion & system summary"
        jobsToday={stats?.jobs_today}
      />

      <div className="page-content">
        {error && retryCount >= MAX_RETRIES && (
          <p className="error-banner">
            {error}
            {" "}
            <button
              className="btn-icon"
              onClick={() => { setRetryCount(0); loadStats(); }}
              style={{ marginLeft: "1rem" }}
              title="Retry"
            >
              ↻ Retry
            </button>
          </p>
        )}

        {/* Action bar — Trend Alerts (left) + Review Queue (right) */}
        <div className="review-queue-bar">

          {/* Trend Alerts dropdown */}
          <div className="trend-alert-wrapper" ref={alertsRef}>
            <button
              className={`btn-trend-alerts${trendAlerts.length > 0 ? " has-alerts" : ""}`}
              onClick={() => setAlertsOpen(o => !o)}
            >
              ⚠ Trend Alerts
              {trendAlerts.length > 0 && (
                <span className="review-badge alert-badge">{trendAlerts.length}</span>
              )}
            </button>

            {alertsOpen && (
              <div className="trend-alerts-dropdown">
                {trendAlerts.length === 0 ? (
                  <p className="no-alerts">No active trend alerts</p>
                ) : (
                  trendAlerts.map(alert => (
                    <div
                      key={alert.id}
                      className={`alert-item severity-${alert.predicted_severity}`}
                    >
                      <div className="alert-header">
                        <span className="alert-machine">{alert.machine}</span>
                        <span className={`alert-severity-badge ${alert.predicted_severity}`}>
                          {alert.predicted_severity.toUpperCase()}
                        </span>
                      </div>
                      <p className="alert-pattern">{alert.pattern}</p>
                      <p className="alert-detail">⏱ {alert.estimated_time_to_critical}</p>
                      <p className="alert-detail">→ {alert.recommended_action}</p>
                      <p className="alert-confidence">
                        Confidence: {Math.round(alert.confidence * 100)}%
                      </p>
                      <button
                        className="btn-resolve"
                        onClick={() => resolveAlert(alert.id)}
                      >
                        Resolve
                      </button>
                    </div>
                  ))
                )}
              </div>
            )}
          </div>

          {/* Review Queue button */}
          <button
            className="btn-review-queue"
            onClick={() => setReviewOpen(true)}
          >
            Review Queue
            {pendingCount > 0 && (
              <span className="review-badge">{pendingCount}</span>
            )}
          </button>
        </div>

        <FileUpload onUploaded={handleUploaded} />
        <SummaryPanel stats={stats} loading={false} />
      </div>

      {/* Overlay — mounted only when open to avoid background fetches */}
      {reviewOpen && (
        <ReviewQueueOverlay
          onClose={() => setReviewOpen(false)}
          onReviewed={() => loadStats()}
        />
      )}

    </div>
  );
}
