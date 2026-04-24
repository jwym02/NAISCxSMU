import { useState, useEffect, useCallback, useRef } from "react";
import "./ReviewQueue.css";
import { Header } from "../components/Header";
import { FileUpload } from "../components/FileUpload";
import { SummaryPanel } from "../components/SummaryPanel";
import {
  fetchStats,
  fetchReviewQueue,
  fetchReviewQueueItem,
  submitReview,
  Stats,
  ReviewItem,
} from "../api/pipeline";

// ── ReviewCard ────────────────────────────────────────────────────────────────

interface ReviewCardProps {
  item: ReviewItem;
  onDecision: (jobId: string, decision: "approved" | "rejected", notes: string, category: string) => void;
  disabled: boolean;
}

function ReviewCard({ item, onDecision, disabled }: ReviewCardProps) {
  const [notes, setNotes]       = useState("");
  const [category, setCategory] = useState(item.ai_category || "");
  const [showData, setShowData] = useState(false);
  const [fileData, setFileData] = useState<string | null>(null);
  const [loadingFile, setLoadingFile] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);

  // Fetch file name on mount
  useEffect(() => {
    const fetchFileName = async () => {
      try {
        const data = await fetchReviewQueueItem(item.job_id);
        // Try to extract file name from the event data
        if (data && typeof data === 'object') {
          const name = (data as any).file_name || 
                       (data as any).original_file_name || 
                       (data as any).filename;
          if (name) {
            setFileName(name);
          }
        }
      } catch (err) {
        console.error("Failed to fetch file name", err);
      }
    };
    
    fetchFileName();
  }, [item.job_id]);

  const confidencePct = Math.round((item.confidence_score ?? 0) * 100);
  const confidenceColor =
    confidencePct >= 70 ? "#22c55e" :
    confidencePct >= 40 ? "#f59e0b" : "#ef4444";

  const handleViewFile = async () => {
    setShowData(!showData);
    if (!showData && !fileData) {
      setLoadingFile(true);
      try {
        const data = await fetchReviewQueueItem(item.job_id);
        setFileData(JSON.stringify(data, null, 2));
      } catch (err) {
        setFileData("Error loading file data");
      } finally {
        setLoadingFile(false);
      }
    }
  };

  return (
    <div className="review-card">
      <div className="review-card-header">
        <div className="review-header-top">
          <span className={`severity-badge severity-${(item.severity ?? "unknown").toLowerCase()}`}>
            {item.severity ?? "UNKNOWN"}
          </span>
          <span className="review-confidence" style={{ color: confidenceColor }}>
            {confidencePct}% confidence
          </span>
        </div>
        <div className="review-header-bottom">
          <span className="review-source-label">File:</span>
          <span className="review-source">{fileName || item.source}</span>
          <button className="btn-view-file" onClick={handleViewFile} disabled={loadingFile}>
            {showData ? "Hide" : "View"} File
          </button>
        </div>
      </div>

      <div className="review-card-body">
        <div className="review-field">
          <label>Category</label>
          <input
            className="review-input"
            value={category}
            onChange={(e) => setCategory(e.target.value)}
            placeholder="e.g. thermal, mechanical…"
            disabled={disabled}
          />
        </div>
        <div className="review-field">
          <label>Root cause</label>
          <p className="review-text">{item.ai_root_cause || "—"}</p>
        </div>
        <div className="review-field">
          <label>Recommended action</label>
          <p className="review-text">{item.ai_recommended_action || "—"}</p>
        </div>
        {item.review_reason && (
          <div className="review-field review-reason">
            <label>Flagged because</label>
            <p className="review-text">{item.review_reason}</p>
          </div>
        )}
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

      {showData && (
        <div className="review-file-data">
          {loadingFile ? (
            <p>Loading file data...</p>
          ) : fileData ? (
            <pre>{fileData}</pre>
          ) : null}
        </div>
      )}

      <div className="review-card-actions">
        <button
          className="btn-approve"
          disabled={disabled}
          onClick={() => onDecision(item.job_id, "approved", notes, category)}
        >
          ✓ Approve
        </button>
        <button
          className="btn-reject"
          disabled={disabled}
          onClick={() => onDecision(item.job_id, "rejected", notes, category)}
        >
          ✗ Reject
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

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchReviewQueue();
      setItems(data.items);
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
  ) => {
    setSubmitting((prev) => new Set(prev).add(jobId));
    try {
      await submitReview(jobId, { decision, notes, category });
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
                onDecision={handleDecision}
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
  const wsRef            = useRef<WebSocket | null>(null);
  const pollingRef       = useRef<ReturnType<typeof setInterval> | null>(null);

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

  // Initial load + polling + WebSocket
  useEffect(() => {
    let isMounted = true;

    // Start initial load
    loadStats();

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
  }, [loadStats]);

  const handleUploaded = () => {
    // Refresh stats after a brief delay so the backend has time to process
    setTimeout(() => loadStats(), 1500);
  };

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

        {/* Review Queue button */}
        <div className="review-queue-bar">
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
