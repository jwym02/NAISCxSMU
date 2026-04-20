"""Parser: unknown/binary format, vendor logs, multiline continuation."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_boto = MagicMock()
_boto.client.return_value = MagicMock()
sys.modules.setdefault("boto3", _boto)

from app.pipeline.parser import parse_log  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class ParserExtrasTests(unittest.TestCase):
    def test_unknown_binary(self) -> None:
        out = parse_log(b"\x00\x01\xff\xfe", "")
        self.assertEqual(out["detected_format"], "unknown")
        self.assertEqual(len(out["records"]), 0)

    def test_vendor_fixture_and_continuation(self) -> None:
        data = (FIXTURES / "vendor_keyvalue.log").read_bytes()
        out = parse_log(data, "log")
        self.assertGreaterEqual(len(out["records"]), 1)
        merged = [r for r in out["records"] if "ToolController" in (r.get("message") or "")]
        self.assertTrue(merged, "continuation lines should merge into prior record")


if __name__ == "__main__":
    unittest.main()
