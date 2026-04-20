# Step 3+4:
# detect file format, extract fields into structured key-value pairs

import gzip
import json
import csv
import io
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from app.shared.dynamo import dynamo_client

"""
INSTRUCTIONS FOR parse_log() FUNCTION
======================================

PURPOSE:
  Parse a raw log file into structured data based on its format.
  This function detects what type of file it is and extracts key-value pairs.

INPUTS:
  - file_data: Raw bytes of the file content
  - file_format: String hint about file type ("json", "xml", "csv", "log", "txt")
                 If uncertain, auto-detect from content

OUTPUTS:
  - Return a dictionary with:
    {
      "detected_format": "json|xml|csv|log|txt",  # What format was detected
      "records": [                                 # List of parsed records
        {
          "timestamp": "2024-04-17T10:30:00Z",    # ISO 8601 timestamp if found
          "source": "machine_001",                 # Machine/source identifier
          "event_type": "temperature_warning",     # Type of event
          "severity": "warning|error|info",        # Severity level
          "message": "Raw event text here",        # Full message content
          "raw_fields": {...}                      # Original parsed fields
        },
        # ... more records
      ],
      "parse_errors": [                            # Any parsing issues encountered
        {
          "line": 5,
          "error": "Invalid JSON on line 5: expected comma"
        }
      ]
    }

SUPPORTED FORMATS:

  JSON:
    - Parse as JSON array or object
    - Flatten nested fields (e.g., {"machine": {"id": "001"}} → machine_id: "001")
    - Extract timestamp field (look for: timestamp, time, created_at, date)
    - Extract event/message field (look for: message, event, data, log)

  XML:
    - Parse XML structure
    - Convert to flat key-value pairs
    - Look for timestamp and message attributes/elements
    - Handle nested elements by joining path with underscores (log > event > type → log_event_type)

  CSV:
    - Parse using CSV reader (first row = headers)
    - Each row becomes one record
    - Use header names as field names
    - Try to infer timestamp column (common names: timestamp, date, time, created_at)

  LOG/TXT:
    - Parse line by line (one event per line or multi-line events)
    - Use regex patterns to extract common fields:
      * Timestamp: Look for ISO 8601 format (2024-04-17T...)
      * Severity: Look for keywords (ERROR, WARNING, INFO, DEBUG, CRITICAL)
      * Machine ID: Look for machine_*, tool_*, device_*, host_* patterns
      * Message: Remaining text after extracting structured fields

STEPS TO IMPLEMENT:
  1. Auto-detect format if file_format is not provided
  2. Parse file_data according to detected format
  3. For each parsed item/record:
     a. Extract or generate a timestamp (use current time if not found)
     b. Extract message/event text
     c. Extract source/machine identifier
     d. Infer severity level (ERROR > WARNING > INFO > DEBUG)
     e. Store original raw fields for reference
  4. Collect any parsing errors without stopping (be lenient)
  5. Return structured output with all records

SERVICES TO USE:
  - Standard library: json, xml.etree.ElementTree, csv
  - Helpers: Check app.shared for any utility functions

ERROR HANDLING:
  - Malformed records should be collected in parse_errors list
  - Don't fail entire file on one bad record - keep parsing
  - If format can't be detected: try each parser and use whichever succeeds
  - Empty files should return empty records list

NOTES:
  - Timestamps should be normalized to ISO 8601 format for consistency
  - The raw_fields dictionary preserves original data for audit trail
  - This is the "standardization" step - output format is the same regardless of input format
"""

RE_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
RE_TIMESTAMP_TZ = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)
RE_APACHE_TS = re.compile(
    r"\[(\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]"
)
RE_SYSLOG_BSD_TS = re.compile(
    r"\b([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\b"
)
RE_EPOCH_MS = re.compile(r"\b(1[5-9]\d{11}|2\d{12})\b")
RE_EPOCH_S = re.compile(r"\b(1[5-9]\d{8}|2\d{9})\b")
RE_SEVERITY = re.compile(
    r"\b(FATAL|EMERG|ALERT|CRITICAL|CRIT|ERROR|ERR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)\b",
    re.IGNORECASE,
)
RE_SOURCE = re.compile(r"\b(machine|tool|device|host|node|server)_\w+", re.IGNORECASE)
RE_KV = re.compile(
    r'([A-Za-z_][A-Za-z0-9_.\-]*)=("(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'|[^\s,;]+)'
)
RE_INLINE_JSON = re.compile(r"(\{[^{}]{0,2000}\})")
RE_SYSLOG_3164 = re.compile(
    r"^<(?P<pri>\d{1,3})>"
    r"(?P<ts>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+"
    r"(?P<app>[^\s:\[]+)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<msg>.*)$"
)
RE_SYSLOG_5424 = re.compile(
    r"^<(?P<pri>\d{1,3})>(?P<ver>\d)\s+"
    r"(?P<ts>\S+)\s+(?P<host>\S+)\s+(?P<app>\S+)\s+"
    r"(?P<pid>\S+)\s+(?P<msgid>\S+)\s+(?P<sd>-|\[[^\]]*\])\s*"
    r"(?P<msg>.*)$"
)

RULES_TABLE = os.getenv("DYNAMODB_TABLE_FIELD_RULES") or os.getenv(
    "DYNAMODB_TABLE_RULES", "normalization-rules"
)
SEVERITY_MAP = {
    "FATAL": "error", "EMERG": "error", "ALERT": "error",
    "CRITICAL": "error", "CRIT": "error", "ERROR": "error", "ERR": "error",
    "WARNING": "warning", "WARN": "warning",
    "NOTICE": "info", "INFO": "info",
    "DEBUG": "info", "TRACE": "info",
}
SYSLOG_SEVERITY = {
    0: "error", 1: "error", 2: "error", 3: "error",
    4: "warning", 5: "info", 6: "info", 7: "info",
}
_rules_cache: dict | None = None
_rules_cache_time: float | None = None
RULES_CACHE_TTL = 300  # basically 5 minutes


def _is_probably_binary(sample: bytes) -> bool:
    if not sample:
        return False
    if b"\x00" in sample[:8192]:
        return True
    chunk = sample[:4000]
    try:
        text = chunk.decode("utf-8")
    except UnicodeDecodeError:
        return True
    if not text:
        return False
    ctrl = sum(1 for c in text if ord(c) < 32 and c not in "\t\n\r")
    return (ctrl / max(len(text), 1)) > 0.12


def _maybe_gunzip(data: bytes) -> bytes:
    # gzip magic: 1f 8b. Vendors often ship logs as .gz dumps.
    if len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B:
        try:
            return gzip.decompress(data)
        except OSError:
            return data
    return data


def _looks_like_ndjson(sample: bytes) -> bool:
    text = sample[:8192].decode("utf-8", errors="replace")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return False
    # First two non-empty lines must both be JSON objects/arrays on their own line.
    good = 0
    for ln in lines[:4]:
        if not (ln.startswith("{") and ln.endswith("}")) and not (
            ln.startswith("[") and ln.endswith("]")
        ):
            return False
        try:
            json.loads(ln)
            good += 1
        except json.JSONDecodeError:
            return False
    return good >= 2


def _looks_like_syslog(sample: bytes) -> bool:
    text = sample[:4096].decode("utf-8", errors="replace")
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if RE_SYSLOG_5424.match(ln) or RE_SYSLOG_3164.match(ln):
            return True
        return False
    return False


def fetch_normalization_rules() -> dict:
    """
    Returns: {
        "timestamp": ["created_at", "ts", "time", "date", ...],
        "source":    ["machine_id", "host", "device", ...],
        "severity":  ["level", "log_level", "priority", ...],
        "message":   ["msg", "event", "description", ...]
    }
    """
    global _rules_cache, _rules_cache_time
    now = datetime.now(timezone.utc).timestamp()

    # take from cache if still there, if not then dynamodb
    if _rules_cache is not None and _rules_cache_time is not None:
        if (now - _rules_cache_time) < RULES_CACHE_TTL:
            return _rules_cache
    
    # Cache is empty / expired —-> fetch from DynamoDB

    field_types = ["timestamp", "source", "severity", "message", "event_type"]
    rules = {ft: [] for ft in field_types}

    for field_type in field_types:
        try:
            response = dynamo_client.query(
                TableName=RULES_TABLE,
                KeyConditionExpression="vendorId = :v",
                ExpressionAttributeValues={":v": {"S": field_type}}
            )
            rules[field_type] = [
                item["fieldName"]["S"] for item in response.get("Items", [])
            ]
        except Exception as e:
            logging.warning(f"Could not fetch rules for '{field_type}' from DynamoDB: {e}")

    _rules_cache = rules
    _rules_cache_time = now 
    return rules

def detect_format(file_data: bytes, hint: str) -> str:
    sample = file_data[:8192]
    if _is_probably_binary(sample):
        return "unknown"
    if hint:
        h = hint.lower()
        if h in ("json", "ndjson", "jsonl", "xml", "csv", "log", "txt", "syslog"):
            return "ndjson" if h in ("ndjson", "jsonl") else h
    head = file_data[:500].strip()
    if _looks_like_ndjson(file_data):
        return "ndjson"
    if head.startswith(b"{") or head.startswith(b"["):
        return "json"
    # Syslog must be checked before XML: RFC3164/5424 lines start with `<NN>`.
    if _looks_like_syslog(file_data):
        return "syslog"
    if head.startswith(b"<"):
        return "xml"
    first_line = head.split(b"\n")[0].decode(errors="ignore")
    if first_line.count(",") >= 2:
        return "csv"
    return "log"   # fallback

def flatten_dict(d: dict, prefix="") -> dict:
    out = {}
    for k, v in d.items():
        key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten_dict(v, key))
        else:
            out[key] = v
    return out

def xml_element_to_dict(element) -> dict:
    result = dict(element.attrib)   # grab XML attributes
    for child in element:
        tag = child.tag.split("}")[-1]  # strip namespace if present
        result[tag] = xml_element_to_dict(child) if len(child) else (child.text or "")
    return result

def parse_json(file_data: bytes):
    records, errors = [], []
    try:
        data = json.loads(file_data.decode("utf-8"))
        if isinstance(data, list):
          items = data # is already a list
        elif isinstance(data, dict):
          items = next(
              (v for v in data.values() if isinstance(v, list)), [data]
          ) # is a dict
        else:
          items = [data] # unknown structure, just put in a list lol

        for i, item in enumerate(items):
            try:
                flat = flatten_dict(item)
                flat["line_no"] = i + 1
                records.append(flat)
            except Exception as e:
                errors.append({"line": i, "error": str(e)})
    except json.JSONDecodeError as e:
      errors.append({"line": e.lineno, "error": str(e)})
    return records, errors

def parse_xml(file_data: bytes):
    records, errors = [], []
    try:
        root = ET.fromstring(file_data.decode("utf-8"))
        # Each direct child = one record
        children = list(root)
        items = children if children else [root]
        for i, elem in enumerate(items):
            try:
                flat = flatten_dict(xml_element_to_dict(elem))
                flat["line_no"] = i + 1
                records.append(flat)
            except Exception as e:
                errors.append({"line": i, "error": str(e)})
    except ET.ParseError as e:
        errors.append({"line": 0, "error": str(e)})
    return records, errors

def parse_csv(file_data: bytes):
    records, errors = [], []
    try:
        text = file_data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        for i, row in enumerate(reader):
            try:
                rec = dict(row)
                rec["line_no"] = i + 2  # header line 1; first data row line 2
                records.append(rec)
            except Exception as e:
                errors.append({"line": i + 2, "error": str(e)})  # +2: 1 header + 1-indexed
    except Exception as e:
        errors.append({"line": 0, "error": str(e)})
    return records, errors

def _log_line_looks_like_new_event(stripped: str) -> bool:
    if not stripped or stripped.startswith("#"):
        return False
    if RE_TIMESTAMP_TZ.search(stripped) or RE_APACHE_TS.search(stripped):
        return True
    if RE_SYSLOG_BSD_TS.search(stripped):
        return True
    if RE_SEVERITY.search(stripped):
        return True
    if RE_SOURCE.search(stripped):
        return True
    return False


def _extract_best_timestamp(stripped: str) -> str | None:
    """Try several common timestamp shapes and return the first match as a string."""
    m = RE_TIMESTAMP_TZ.search(stripped)
    if m:
        return m.group(0)
    m = RE_APACHE_TS.search(stripped)
    if m:
        return m.group(1)
    m = RE_EPOCH_MS.search(stripped)
    if m:
        return m.group(1)
    m = RE_EPOCH_S.search(stripped)
    if m:
        return m.group(1)
    m = RE_SYSLOG_BSD_TS.search(stripped)
    if m:
        return m.group(1)
    return None


def _extract_kv_pairs(stripped: str) -> dict:
    """Extract inline `key=value` pairs (handles quoted values)."""
    out: dict = {}
    for m in RE_KV.finditer(stripped):
        k = m.group(1)
        v = m.group(2)
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
            v = v[1:-1]
        if k and k not in out:
            out[k] = v
    return out


def _extract_inline_json(stripped: str) -> dict:
    """If the line contains a JSON object, merge its flat keys in."""
    m = RE_INLINE_JSON.search(stripped)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(1))
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(obj, dict):
        return {}
    try:
        return flatten_dict(obj)
    except Exception:
        return {}


def parse_log_txt(file_data: bytes):
    records, errors = [], []
    lines = file_data.decode("utf-8", errors="replace").splitlines()
    i = 0
    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.strip()
        if not stripped:
            i += 1
            continue
        try:
            if records and not _log_line_looks_like_new_event(stripped):
                prev = records[-1]
                prev["raw_line"] = (prev.get("raw_line") or "") + "\n" + stripped
                prev["message"] = (prev.get("message") or "") + "\n" + stripped
                i += 1
                continue
            raw: dict = {"raw_line": stripped, "line_no": i + 1}
            ts = _extract_best_timestamp(stripped)
            if ts:
                raw["timestamp"] = ts
            sev = RE_SEVERITY.search(stripped)
            if sev:
                raw["severity"] = sev.group().upper()
            src = RE_SOURCE.search(stripped)
            if src:
                raw["source"] = src.group()
            # Pull structured hints from key=value and inline-JSON fragments.
            kv = _extract_kv_pairs(stripped)
            for k, v in kv.items():
                raw.setdefault(k, v)
            inline = _extract_inline_json(stripped)
            for k, v in inline.items():
                raw.setdefault(k, v)
            raw["message"] = stripped
            records.append(raw)
        except Exception as e:
            errors.append({"line": i + 1, "error": str(e)})
        i += 1
    return records, errors


def parse_ndjson(file_data: bytes):
    """JSON-per-line (NDJSON / JSONL). One record per non-empty line."""
    records, errors = [], []
    text = file_data.decode("utf-8", errors="replace")
    for i, line in enumerate(text.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError as e:
            errors.append({"line": i + 1, "error": str(e)})
            continue
        if isinstance(obj, dict):
            try:
                flat = flatten_dict(obj)
            except Exception as e:
                errors.append({"line": i + 1, "error": str(e)})
                continue
        else:
            flat = {"value": obj}
        flat["line_no"] = i + 1
        records.append(flat)
    return records, errors


def parse_syslog(file_data: bytes):
    """RFC5424 first, then RFC3164. Falls back to plain log parsing per line."""
    records, errors = [], []
    text = file_data.decode("utf-8", errors="replace")
    for i, line in enumerate(text.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        m = RE_SYSLOG_5424.match(stripped) or RE_SYSLOG_3164.match(stripped)
        if not m:
            # Syslog-looking file but a malformed line: retain as free-text row.
            records.append({
                "line_no": i + 1, "raw_line": stripped, "message": stripped,
            })
            continue
        try:
            pri = int(m.group("pri"))
            sev_num = pri & 0x7
            facility = pri >> 3
            raw: dict = {
                "line_no": i + 1,
                "raw_line": stripped,
                "timestamp": m.group("ts"),
                "source": m.group("host"),
                "app": m.group("app"),
                "facility": facility,
                "syslog_severity": sev_num,
                "severity": {
                    "error": "ERROR", "warning": "WARN", "info": "INFO",
                }.get(SYSLOG_SEVERITY.get(sev_num, "info"), "INFO"),
                "message": m.group("msg"),
            }
            raw["pid"] = m.groupdict().get("pid")
            # Opportunistic field extraction from message payload.
            kv = _extract_kv_pairs(raw["message"])
            for k, v in kv.items():
                raw.setdefault(k, v)
            inline = _extract_inline_json(raw["message"])
            for k, v in inline.items():
                raw.setdefault(k, v)
            records.append(raw)
        except Exception as e:
            errors.append({"line": i + 1, "error": str(e)})
    return records, errors

def _parse_timestamp_string(ts_raw: str) -> str | None:
    """Best-effort parse of a timestamp string into ISO 8601. Returns None on failure."""
    if not ts_raw:
        return None
    s = str(ts_raw).strip()
    # Epoch ms / s (purely digits).
    if s.isdigit():
        try:
            n = int(s)
            if n >= 10**12:
                return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc).isoformat()
            if n >= 10**9:
                return datetime.fromtimestamp(n, tz=timezone.utc).isoformat()
        except (ValueError, OSError, OverflowError):
            return None
    # ISO 8601 (also handles `Z`, offsets, microseconds).
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).isoformat()
    except ValueError:
        pass
    # Apache/nginx style: 17/Apr/2024:10:30:00 +0000
    for fmt in (
        "%d/%b/%Y:%H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    # Syslog BSD: Apr 17 10:30:00 (no year — assume current UTC year).
    m = RE_SYSLOG_BSD_TS.fullmatch(s) or RE_SYSLOG_BSD_TS.match(s)
    if m:
        try:
            year = datetime.now(timezone.utc).year
            return datetime.strptime(
                f"{year} {m.group(1)}", "%Y %b %d %H:%M:%S"
            ).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            return None
    return None


def normalize_record(raw: dict, rules: dict) -> dict:
    def find(field_type):
        # First try DynamoDB rules
        for alias in rules.get(field_type, []):
            for k, v in raw.items():
                if k.lower() == alias.lower():
                    return str(v)
        
        # Fallback: check common field names for each type
        common_names = {
            "timestamp": ["timestamp", "ts", "time", "date", "created_at", "created"],
            "source": ["source", "machine", "machine_id", "host", "device", "tool"],
            "severity": ["severity", "level", "log_level", "priority"],
            "message": ["message", "msg", "event", "description", "text"],
            "event_type": ["event_type", "event", "type", "category"]
        }
        
        for alias in common_names.get(field_type, []):
            for k, v in raw.items():
                if k.lower() == alias.lower():
                    return str(v)
        return None

    ts_raw = find("timestamp")
    timestamp = _parse_timestamp_string(ts_raw) if ts_raw else None
    timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    severity_raw = (find("severity") or "").upper()

    line_no = raw.get("line_no")
    out = {
        "timestamp":  timestamp,
        "source":     find("source")     or "unknown",
        "event_type": find("event_type") or "unknown",
        "severity":   SEVERITY_MAP.get(severity_raw, "info"),
        "message":    find("message")    or raw.get("raw_line", ""),
        "raw_fields": raw,
    }
    if line_no is not None:
        out["line_no"] = line_no
    return out

def parse_log(file_data: bytes, file_format: str) -> dict:
    if not file_data:
        return {"detected_format": file_format or "unknown", "records": [], "parse_errors": []}

    # Transparently decompress gzipped inputs before any sniffing.
    file_data = _maybe_gunzip(file_data)

    detected_format = detect_format(file_data, file_format or "")

    if detected_format == "unknown":
        return {
            "detected_format": "unknown",
            "records": [],
            "parse_errors": [
                {
                    "line": 0,
                    "error": "Binary or unrecognizable format; use UTF-8 text logs or set an explicit format hint.",
                }
            ],
        }

    parsers = {
        "json":   parse_json,
        "ndjson": parse_ndjson,
        "xml":    parse_xml,
        "csv":    parse_csv,
        "log":    parse_log_txt,
        "txt":    parse_log_txt,
        "syslog": parse_syslog,
    }

    raw_records, errors = parsers[detected_format](file_data)

    rules = fetch_normalization_rules()
    normalized = [normalize_record(r, rules) for r in raw_records]

    if not normalized and file_data:
        if not errors:
            errors.append({
                "line": 0,
                "error": "No records could be extracted; verify file format or pass an explicit format hint.",
            })

    return {
        "detected_format": detected_format,
        "records": normalized,
        "parse_errors": errors
    }