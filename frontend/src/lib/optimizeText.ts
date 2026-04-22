/**
 * Lightweight normalization for log preview / copy (client-side only).
 * Does not change semantics for parsing — display and clipboard helper.
 */
const ANSI_ESCAPE = /\x1b\[[0-?]*[ -/]*[@-~]/g;

export function optimizeLogText(text: string): string {
  if (!text) return "";

  let s = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  s = s.replace(ANSI_ESCAPE, "");
  s = s
    .split("\n")
    .map((line) => line.trimEnd())
    .join("\n");
  s = s.replace(/\n{4,}/g, "\n\n\n");
  return s.trim();
}
