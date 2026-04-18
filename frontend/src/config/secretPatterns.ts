/**
 * Default regex patterns for masking pasted/upload preview text (client-side only).
 * Extend or override via future UI settings if needed.
 */
export const DEFAULT_SECRET_PATTERNS: RegExp[] = [
  /\b(sk-[A-Za-z0-9]{20,})\b/gi,
  /\b(AIza[0-9A-Za-z\-_]{35})\b/g,
  /\b(eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b/g,
  /\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/gi,
];
