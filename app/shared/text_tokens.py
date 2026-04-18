"""
Deterministic text normalization and tokenization for NL queries and log search.

Policy version is bumped when token rules change (keep in sync with any stored fts).
"""

from __future__ import annotations

import os
import re
import unicodedata

TOKENIZER_VERSION = "1"

# Hard cap for user natural-language query length (characters, pre-NFC). Reject above this.
MAX_USER_PROMPT_CHARS = int(os.getenv("NL_QUERY_MAX_CHARS", "8000"))

# Heuristic max token estimate for UI (chars / ratio), aligned with common ~4 chars/token.
CHARS_PER_TOKEN_ESTIMATE = float(os.getenv("CHARS_PER_TOKEN_ESTIMATE", "4"))


def normalize_prompt(text: str) -> str:
    """
    Unicode NFC, strip edges. Does not truncate — callers enforce MAX_USER_PROMPT_CHARS before calling LLM.
    """
    if not text:
        return ""
    return unicodedata.normalize("NFC", text.strip())


def tokenize_for_match(text: str) -> list[str]:
    """Lowercase, split on non-word runs; deterministic, no randomness."""
    s = normalize_prompt(text).lower()
    return [t for t in re.split(r"[^\w]+", s) if t]


def build_event_search_text(
    source: str,
    event_type: str,
    message: str,
    ai_category: str = "",
) -> str:
    """Space-separated normalized tokens for ILIKE / full-text indexing on stored events."""
    blob = " ".join(
        tokenize_for_match(f"{source} {event_type} {message} {ai_category}")
    )
    return blob[:16000] if len(blob) > 16000 else blob


def estimate_token_count(text: str) -> float:
    """Rough token count for UI previews (not an LLM tokenizer)."""
    if not text or CHARS_PER_TOKEN_ESTIMATE <= 0:
        return 0.0
    return len(text) / CHARS_PER_TOKEN_ESTIMATE
