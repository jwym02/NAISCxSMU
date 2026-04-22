import { Stats } from "../api/pipeline";

interface SummaryPanelProps {
  stats: Stats | null;
  loading: boolean;
}

const SEVERITY_ROWS = [
  {
    label: "Critical",
    color: "#ef4444",
    getValue: (s: Stats) => s.severity_breakdown["CRITICAL"] ?? 0,
  },
  {
    label: "Errors",
    color: "#f59e0b",
    getValue: (s: Stats) => s.severity_breakdown["ERROR"] ?? 0,
  },
  {
    label: "Non-urgent",
    color: "#22c55e",
    getValue: (s: Stats) =>
      (s.severity_breakdown["WARNING"] ?? 0) +
      (s.severity_breakdown["INFO"] ?? 0),
  },
  {
    label: "Processing Failure",
    color: "#6e7681",
    getValue: (s: Stats) => s.errors,
  },
];

export function SummaryPanel({ stats, loading }: SummaryPanelProps) {
  if (loading) {
    return (
      <div>
        <div className="section-label">SUMMARY</div>
        <p className="loading-text">Loading…</p>
      </div>
    );
  }

  const s = stats ?? {
    events_processed: 0,
    events_in_review: 0,
    errors: 0,
    jobs_today: 0,
    severity_breakdown: {},
  };

  const maxCount = Math.max(...SEVERITY_ROWS.map((r) => r.getValue(s)), 1);

  return (
    <div>
      <div className="section-label">SUMMARY</div>
      <div className="summary-panel">
        <div className="summary-counts">
          <div className="count-card">
            <div className="count-label">EVENTS PROCESSED</div>
            <div className="count-value">{s.events_processed}</div>
          </div>
          <div className="count-card">
            <div className="count-label">EVENTS IN REVIEW</div>
            <div className="count-value amber">{s.events_in_review}</div>
          </div>
          <div className="count-card">
            <div className="count-label">ERRORS</div>
            <div className="count-value ok">{s.errors}</div>
          </div>
        </div>

        <div className="status-section">
          <div className="status-section-label">STATUS</div>
          {SEVERITY_ROWS.map(({ label, color, getValue }) => {
            const count = getValue(s);
            const pct = (count / maxCount) * 100;
            return (
              <div key={label} className="status-row">
                <span className="status-name" style={{ color }}>
                  {label}
                </span>
                <div className="status-bar-track">
                  <div
                    className="status-bar-fill"
                    style={{ width: `${pct}%`, backgroundColor: color }}
                  />
                </div>
                <span className="status-count">{count}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
