"""Golden tests for parse_log — formats per BUILD-REQUIREMENTS WS-E."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Allow importing parser without full AWS/MinIO stack in dev CI.
_boto = MagicMock()
_boto.client.return_value = MagicMock()
sys.modules.setdefault("boto3", _boto)

from app.pipeline.parser import parse_log

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class ParserGoldenTests(unittest.TestCase):
    def test_json_records_and_line_no(self) -> None:
        data = (FIXTURES / "sample.json").read_bytes()
        out = parse_log(data, "json")
        self.assertEqual(out["detected_format"], "json")
        self.assertEqual(len(out["records"]), 2)
        r0 = out["records"][0]
        self.assertEqual(r0["source"], "machine_001")
        self.assertEqual(r0.get("line_no"), 1)
        self.assertIn("timestamp", r0)
        self.assertIn("raw_fields", r0)

    def test_csv_records_and_line_no(self) -> None:
        data = (FIXTURES / "sample.csv").read_bytes()
        out = parse_log(data, "csv")
        self.assertEqual(out["detected_format"], "csv")
        self.assertEqual(len(out["records"]), 2)
        self.assertEqual(out["records"][0].get("line_no"), 2)

    def test_plain_log_records(self) -> None:
        data = (FIXTURES / "sample.log").read_bytes()
        out = parse_log(data, "log")
        self.assertEqual(out["detected_format"], "log")
        self.assertEqual(len(out["records"]), 2)
        self.assertEqual(out["records"][0].get("line_no"), 1)

    def test_invalid_json_reports_errors(self) -> None:
        out = parse_log(b"not valid json {]", "json")
        self.assertEqual(len(out["records"]), 0)
        self.assertTrue(out["parse_errors"])

    def test_no_records_adds_hint_when_no_errors(self) -> None:
        """Degenerate file: only whitespace lines → no records, generic hint appended."""
        out = parse_log(b"\n\n  \n", "log")
        self.assertEqual(len(out["records"]), 0)
        self.assertTrue(any("No records could be extracted" in str(e) for e in out["parse_errors"]))


if __name__ == "__main__":
    unittest.main()
