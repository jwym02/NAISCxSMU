import { useState, useEffect, useCallback } from "react";
import { Header } from "../components/Header";
import { FileUpload } from "../components/FileUpload";
import { SummaryPanel } from "../components/SummaryPanel";
import { fetchStats, Stats } from "../api/pipeline";

export function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadStats = useCallback(async () => {
    try {
      const data = await fetchStats();
      setStats(data);
      setError(null);
    } catch {
      setError("Could not reach pipeline service");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStats();
  }, [loadStats]);

  const handleUploaded = () => {
    // Refresh stats after a brief delay so the backend has time to process
    setTimeout(loadStats, 1500);
  };

  return (
    <div className="page">
      <Header
        title="Dashboard"
        subtitle="Log ingestion & system summary"
        jobsToday={stats?.jobs_today}
      />
      <div className="page-content">
        {error && <p className="error-banner">{error}</p>}
        <FileUpload onUploaded={handleUploaded} />
        <SummaryPanel stats={stats} loading={loading} />
      </div>
    </div>
  );
}
