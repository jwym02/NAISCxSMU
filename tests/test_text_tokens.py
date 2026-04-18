"""Tests for app.shared.text_tokens."""
from __future__ import annotations

import unittest

from app.shared.text_tokens import (
    TOKENIZER_VERSION,
    build_event_search_text,
    estimate_token_count,
    normalize_prompt,
    tokenize_for_match,
)


class TextTokensTests(unittest.TestCase):
    def test_normalize_prompt_nfc(self) -> None:
        self.assertTrue(len(TOKENIZER_VERSION) >= 1)
        self.assertEqual(normalize_prompt("  hello  "), "hello")

    def test_tokenize_for_match(self) -> None:
        self.assertEqual(
            tokenize_for_match("ERROR on machine_001: valve"),
            ["error", "on", "machine_001", "valve"],
        )

    def test_build_event_search_text(self) -> None:
        s = build_event_search_text("m1", "alarm", "Temp high", "thermal")
        self.assertIn("thermal", s)
        self.assertIn("m1", s)

    def test_estimate_token_count(self) -> None:
        self.assertGreater(estimate_token_count("abcd"), 0)


if __name__ == "__main__":
    unittest.main()
