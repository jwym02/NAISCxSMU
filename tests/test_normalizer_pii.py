"""PII scrubbing + fenced-JSON robustness for the normalizer."""
from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock

_boto = MagicMock()
_boto.client.return_value = MagicMock()
sys.modules.setdefault("boto3", _boto)

from app.pipeline.normalizer import _extract_json_blob, scrub_pii  # noqa: E402


class PiiScrubTests(unittest.TestCase):
    def test_email_ip_phone_hex_redacted(self) -> None:
        s = (
            "User alice@example.com from 10.0.0.5 phone +1 415-555-1212 "
            "token=abcdef0123456789abcdef0123456789abcdef01"
        )
        out = scrub_pii(s)
        self.assertNotIn("alice@example.com", out)
        self.assertNotIn("10.0.0.5", out)
        self.assertNotIn("abcdef0123456789abcdef0123456789abcdef01", out)
        self.assertIn("[EMAIL]", out)
        self.assertIn("[IP]", out)

    def test_secret_key_redacted(self) -> None:
        s = 'Authorization: Bearer sk-123abc456'
        out = scrub_pii(s)
        self.assertIn("[REDACTED]", out)
        self.assertNotIn("sk-123abc456", out)

    def test_empty_safe(self) -> None:
        self.assertEqual(scrub_pii(""), "")


class FencedJsonTests(unittest.TestCase):
    def test_plain_json(self) -> None:
        self.assertEqual(_extract_json_blob('{"a":1}'), '{"a":1}')

    def test_markdown_fenced(self) -> None:
        s = "```json\n{\"a\":1}\n```"
        self.assertEqual(_extract_json_blob(s), '{"a":1}')

    def test_bare_fenced(self) -> None:
        s = "```\n{\"a\":1}\n```"
        self.assertEqual(_extract_json_blob(s), '{"a":1}')


if __name__ == "__main__":
    unittest.main()
