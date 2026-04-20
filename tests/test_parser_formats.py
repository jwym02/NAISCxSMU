"""Extra format coverage: NDJSON, gzip, syslog, key=value extraction."""
from __future__ import annotations

import gzip
import sys
import unittest
from unittest.mock import MagicMock

_boto = MagicMock()
_boto.client.return_value = MagicMock()
sys.modules.setdefault("boto3", _boto)

from app.pipeline.parser import parse_log  # noqa: E402


class NdjsonTests(unittest.TestCase):
    def test_ndjson_detection_and_records(self) -> None:
        payload = (
            b'{"timestamp":"2024-04-17T10:30:00Z","source":"machine_001","msg":"ok"}\n'
            b'{"timestamp":"2024-04-17T10:31:00Z","source":"machine_002","msg":"warn"}\n'
        )
        out = parse_log(payload, "")
        self.assertEqual(out["detected_format"], "ndjson")
        self.assertEqual(len(out["records"]), 2)
        self.assertEqual(out["records"][0]["source"], "machine_001")
        self.assertEqual(out["records"][0]["line_no"], 1)

    def test_ndjson_hint_alias_jsonl(self) -> None:
        payload = b'{"a":1}\n{"a":2}\n'
        out = parse_log(payload, "jsonl")
        self.assertEqual(out["detected_format"], "ndjson")
        self.assertEqual(len(out["records"]), 2)


class GzipTests(unittest.TestCase):
    def test_gzipped_json_decompressed(self) -> None:
        raw = b'[{"ts":"2024-04-17T10:30:00Z","machine_id":"m1","msg":"hi"}]'
        gzdata = gzip.compress(raw)
        out = parse_log(gzdata, "")
        self.assertEqual(out["detected_format"], "json")
        self.assertEqual(len(out["records"]), 1)
        self.assertEqual(out["records"][0]["source"], "m1")


class SyslogTests(unittest.TestCase):
    def test_rfc3164_line(self) -> None:
        line = (
            b"<34>Oct 11 22:14:15 mymachine su[12345]: 'su root' failed for lonvick on /dev/pts/8"
        )
        out = parse_log(line, "")
        self.assertEqual(out["detected_format"], "syslog")
        self.assertEqual(len(out["records"]), 1)
        rec = out["records"][0]
        self.assertEqual(rec["source"], "mymachine")
        self.assertIn("su", rec["message"])

    def test_rfc5424_line(self) -> None:
        line = (
            b"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog "
            b"- ID47 - An application event log entry"
        )
        out = parse_log(line, "syslog")
        self.assertEqual(out["detected_format"], "syslog")
        self.assertEqual(len(out["records"]), 1)
        rec = out["records"][0]
        self.assertEqual(rec["source"], "mymachine.example.com")


class KeyValueExtractionTests(unittest.TestCase):
    def test_kv_pairs_lifted_into_raw_fields(self) -> None:
        line = (
            b'2024-04-17T10:30:00Z ERROR machine_007 temp=91 unit="C" '
            b'reason="cooling fan stuck"\n'
        )
        out = parse_log(line, "log")
        self.assertEqual(out["detected_format"], "log")
        self.assertEqual(len(out["records"]), 1)
        raw = out["records"][0]["raw_fields"]
        self.assertEqual(raw.get("temp"), "91")
        self.assertEqual(raw.get("unit"), "C")
        self.assertEqual(raw.get("reason"), "cooling fan stuck")

    def test_inline_json_keys_lifted(self) -> None:
        line = (
            b'2024-04-17T10:30:00Z WARN host_42 event={"code":"OVERHEAT","zone":"A1"}\n'
        )
        out = parse_log(line, "log")
        raw = out["records"][0]["raw_fields"]
        self.assertEqual(raw.get("code"), "OVERHEAT")
        self.assertEqual(raw.get("zone"), "A1")


if __name__ == "__main__":
    unittest.main()
