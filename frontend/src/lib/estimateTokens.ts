/**
 * Rough token estimate for UI (not an LLM tokenizer).
 * Aligned with app/shared/text_tokens.py `estimate_token_count`.
 */
const DEFAULT_CHARS_PER_TOKEN = 4;

export function estimateApproxTokens(text: string): number {
  const ratio = DEFAULT_CHARS_PER_TOKEN;
  if (!text || ratio <= 0) return 0;
  return Math.round((text.length / ratio) * 10) / 10;
}
