import { useMemo, useState } from "react";
import { estimateApproxTokens } from "./lib/estimateTokens";
import { optimizeLogText } from "./lib/optimizeText";
import "./App.css";

export default function App() {
  const [text, setText] = useState("");

  const optimized = useMemo(() => optimizeLogText(text), [text]);
  const rawTokens = useMemo(() => estimateApproxTokens(text), [text]);
  const optimizedTokens = useMemo(() => estimateApproxTokens(optimized), [optimized]);

  return (
    <main className="app">
      <h1>Log Parser Frontend</h1>
      <p className="sub">
        Frontend scaffold restored. Backend URLs come from <code>.env</code>.
      </p>

      <section className="panel">
        <label htmlFor="raw">Paste raw log text</label>
        <textarea
          id="raw"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Paste sample log lines here..."
        />
        <p className="sub">Raw chars: {text.length} | ~tokens: {rawTokens}</p>
      </section>

      <section className="panel">
        <h2>Optimized preview</h2>
        <pre>{optimized || "(empty)"}</pre>
        <p className="sub">
          Optimized chars: {optimized.length} | ~tokens: {optimizedTokens}
        </p>
      </section>
    </main>
  );
}
