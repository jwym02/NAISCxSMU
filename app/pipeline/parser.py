# Step 3+4:
# detect file format, extract fields into structured key-value pairs

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
RE_SEVERITY  = re.compile(r"\b(ERROR|CRITICAL|WARNING|WARN|INFO|DEBUG)\b", re.IGNORECASE)
RE_SOURCE    = re.compile(r"\b(machine|tool|device|host)_\w+", re.IGNORECASE)
RULES_TABLE = os.getenv("DYNAMODB_TABLE_RULES", "normalization-rules")
SEVERITY_MAP = {
    "CRITICAL": "critical", "ERROR": "error",
    "WARNING": "warning", "WARN": "warning",
    "INFO": "info", "DEBUG": "info"
}
_rules_cache: dict | None = None
_rules_cache_time: float | None = None
RULES_CACHE_TTL = 300  # basically 5 minutes

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
    if hint and hint.lower() in ("json", "xml", "csv", "log", "txt"):
        return hint.lower()
    # try detecting by checking the raw bytes
    sample = file_data[:500].strip()
    if sample.startswith(b"{") or sample.startswith(b"["):
        return "json"
    elif sample.startswith(b"<"):
        return "xml"
    first_line = sample.split(b"\n")[0].decode(errors="ignore")
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
                records.append(flatten_dict(item))
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
                records.append(flatten_dict(xml_element_to_dict(elem)))
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
                records.append(dict(row))
            except Exception as e:
                errors.append({"line": i + 2, "error": str(e)})  # +2: 1 header + 1-indexed
    except Exception as e:
        errors.append({"line": 0, "error": str(e)})
    return records, errors

def parse_log_txt(file_data: bytes):
  
    records, errors = [], []
    lines = file_data.decode("utf-8", errors="replace").splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        try:
            raw = {"raw_line": line}
            ts = RE_TIMESTAMP.search(line)
            if ts:
                raw["timestamp"] = ts.group()
            sev = RE_SEVERITY.search(line)
            if sev:
                raw["severity"] = sev.group().upper()
            src = RE_SOURCE.search(line)
            if src:
                raw["source"] = src.group()
            raw["message"] = line
            records.append(raw)
        except Exception as e:
            errors.append({"line": i + 1, "error": str(e)})
    return records, errors

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
    try:
        timestamp = datetime.fromisoformat(
            ts_raw.replace("Z", "+00:00")
        ).isoformat() if ts_raw else None
    except ValueError:
        timestamp = None
    timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    severity_raw = (find("severity") or "").upper()

    return {
        "timestamp":  timestamp,
        "source":     find("source")     or "unknown",
        "event_type": find("event_type") or "unknown",
        "severity":   SEVERITY_MAP.get(severity_raw, "info"),
        "message":    find("message")    or raw.get("raw_line", ""),
        "raw_fields": raw
    }

def parse_log(file_data: bytes, file_format: str) -> dict:
    if not file_data:
        return {"detected_format": file_format or "unknown", "records": [], "parse_errors": []}

    detected_format = detect_format(file_data, file_format)

    parsers = {
        "json": parse_json,
        "xml":  parse_xml,
        "csv":  parse_csv,
        "log":  parse_log_txt,
        "txt":  parse_log_txt,
    }

    raw_records, errors = parsers[detected_format](file_data)

    rules = fetch_normalization_rules()
    normalized = [normalize_record(r, rules) for r in raw_records]

    return {
        "detected_format": detected_format,
        "records": normalized,
        "parse_errors": errors
    }