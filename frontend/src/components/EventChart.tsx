import { useMemo } from "react";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { TimeseriesResponse } from "../api/pipeline";

interface ChartEntry {
  time: string;
  nonUrgent: number;
  critical: number;
  errors: number;
}

interface EventChartProps {
  data: TimeseriesResponse | null;
  chartType: "line" | "bar";
  onChartTypeChange: (t: "line" | "bar") => void;
}

const TOOLTIP_STYLE = {
  backgroundColor: "#1c2333",
  border: "1px solid #30363d",
  color: "#e6edf3",
  fontFamily: "Courier New, monospace",
  fontSize: 11,
};

export function EventChart({ data, chartType, onChartTypeChange }: EventChartProps) {
  const chartData = useMemo<ChartEntry[]>(() => {
    if (!data?.data.length) {
      console.log("[EventChart] No data provided", data);
      return [];
    }
    
    console.log("[EventChart] Processing data:", data.data);
    const map = new Map<string, ChartEntry>();

    for (const pt of data.data) {
      console.log(`[EventChart] Processing point: hour=${pt.hour}, severity=${pt.severity}, count=${pt.count}`);
      if (!map.has(pt.hour)) {
        const d = new Date(pt.hour);
        const time = d.toLocaleTimeString("en-US", {
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        });
        map.set(pt.hour, { time, nonUrgent: 0, critical: 0, errors: 0 });
      }
      const entry = map.get(pt.hour)!;
      if (pt.severity === "CRITICAL") {
        entry.critical += pt.count;
        console.log(`[EventChart] Added ${pt.count} CRITICAL events`);
      }
      else if (pt.severity === "ERROR") {
        entry.errors += pt.count;
        console.log(`[EventChart] Added ${pt.count} ERROR events`);
      }
      else {
        entry.nonUrgent += pt.count;
        console.log(`[EventChart] Added ${pt.count} NON-URGENT events (${pt.severity})`);
      }
    }

    const result = Array.from(map.values());
    console.log("[EventChart] Final chart data:", result);
    return result;
  }, [data]);

  const sharedAxisProps = {
    stroke: "#6e7681",
    tick: { fontSize: 10, fill: "#6e7681", fontFamily: "Courier New, monospace" },
  };

  const renderChart = () => {
    if (chartType === "line") {
      return (
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#30363d" />
          <XAxis dataKey="time" {...sharedAxisProps} />
          <YAxis {...sharedAxisProps} />
          <Tooltip contentStyle={TOOLTIP_STYLE} />
          <Legend wrapperStyle={{ fontSize: 11, fontFamily: "Courier New, monospace" }} />
          <Line
            type="monotone"
            dataKey="nonUrgent"
            name="Non-urgent"
            stroke="#22c55e"
            dot={{ r: 3, fill: "#22c55e" }}
            strokeWidth={2}
          />
          <Line
            type="monotone"
            dataKey="critical"
            name="Critical"
            stroke="#ef4444"
            dot={{ r: 3, fill: "#ef4444" }}
            strokeWidth={2}
          />
        </LineChart>
      );
    }
    return (
      <BarChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" stroke="#30363d" />
        <XAxis dataKey="time" {...sharedAxisProps} />
        <YAxis {...sharedAxisProps} />
        <Tooltip contentStyle={TOOLTIP_STYLE} />
        <Legend wrapperStyle={{ fontSize: 11, fontFamily: "Courier New, monospace" }} />
        <Bar dataKey="nonUrgent" name="Non-urgent" fill="#22c55e" />
        <Bar dataKey="critical" name="Critical" fill="#ef4444" />
      </BarChart>
    );
  };

  return (
    <div className="chart-panel">
      <div className="chart-header">
        <span className="chart-title">EVENT TRENDS</span>
        <div className="chart-type-toggle">
          <button
            className={`toggle-btn${chartType === "line" ? " active" : ""}`}
            onClick={() => onChartTypeChange("line")}
          >
            Line
          </button>
          <button
            className={`toggle-btn${chartType === "bar" ? " active" : ""}`}
            onClick={() => onChartTypeChange("bar")}
          >
            Bar
          </button>
        </div>
      </div>
      <div className="chart-subtitle">EVENTS OVER TIME (LAST 12 HRS)</div>

      {chartData.length === 0 ? (
        <div className="chart-empty">
          {data ? "No event data for this period" : "Loading..."}
        </div>
      ) : (
        <>
          <div style={{ fontSize: 10, color: "#6e7681", marginBottom: 8 }}>
            {chartData.length} time bucket{chartData.length !== 1 ? "s" : ""} • {JSON.stringify(chartData[0])}
          </div>
          <ResponsiveContainer width="100%" height={220}>
            {renderChart()}
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
