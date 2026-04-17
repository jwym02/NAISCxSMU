from dotenv import load_dotenv
load_dotenv()   # must be first

from app.pipeline.ingest import ingest_log
from app.pipeline.parser import parse_log
from app.pipeline.normalizer import normalize_log

def test_ingest_and_parser(file_data, filename, format_hint, description):
    """Helper to test ingest + parser on various file formats"""
    print(f"\n--- {description} ---")
    
    # Ingest
    result = ingest_log(file_data, filename, format_hint)
    print(f"Ingest: {result['status']} (duplicate: {result['is_duplicate']})")
    
    # Parse
    parsed = parse_log(file_data, format_hint)
    print(f"Parser: format={parsed['detected_format']}, records={len(parsed['records'])}, errors={len(parsed['parse_errors'])}")
    if parsed["records"]:
        rec = parsed["records"][0]
        print(f"  → source={rec.get('source')}, event_type={rec.get('event_type')}, severity={rec.get('severity')}")
    
    return parsed

def test_normalizer(parsed_records, description):
    """Helper to test normalizer"""
    if not parsed_records:
        print("  (skipped: no records)")
        return
    
    normalized = normalize_log(parsed_records)
    for i, rec in enumerate(normalized["normalized_records"]):
        ai = rec["ai_normalized"]
        print(f"  [{i}] category={ai['category']}, confidence={ai['confidence']}, review={rec['requires_review']}")
    
    if normalized["review_queue_items"]:
        print(f"  Review queue: {len(normalized['review_queue_items'])} items")
        for item in normalized["review_queue_items"]:
            print(f"    - confidence={item['confidence']}, reason={item['review_reason']}")

# ── Test 1: JSON format ────────────────────────────────────────
print("\n" + "="*70)
print("FORMAT TESTS: JSON, CSV, XML, LOG")
print("="*70)

json_data = b'{"timestamp":"2024-04-17T10:30:00","source":"machine_001","event_type":"temperature_warning","severity":"WARNING","message":"Temperature exceeded 85C threshold"}'
parsed_json = test_ingest_and_parser(json_data, "test_json_001.json", "json", "JSON with WARNING severity")
test_normalizer(parsed_json["records"], "JSON normalizer")

# ── Test 2: CSV format ────────────────────────────────────────
csv_data = b"""timestamp,source,event_type,severity,message
2024-04-17T11:00:00,machine_002,vacuum_loss,ERROR,Vacuum pressure dropped below 1e-6 Torr
2024-04-17T11:01:00,machine_002,vacuum_loss,WARNING,Vacuum pressure trending downward"""
parsed_csv = test_ingest_and_parser(csv_data, "test_csv_001.csv", "csv", "CSV with multiple records and ERROR severity")
test_normalizer(parsed_csv["records"], "CSV normalizer")

# ── Test 3: XML format ────────────────────────────────────────
xml_data = b"""<?xml version="1.0"?>
<logs>
  <log>
    <timestamp>2024-04-17T12:00:00</timestamp>
    <source>machine_003</source>
    <event_type>rf_power_fault</event_type>
    <severity>CRITICAL</severity>
    <message>RF power loss in chamber</message>
  </log>
</logs>"""
parsed_xml = test_ingest_and_parser(xml_data, "test_xml_001.xml", "xml", "XML with CRITICAL severity")
test_normalizer(parsed_xml["records"], "XML normalizer")

# ── Test 4: LOG/TXT format (free-form text) ────────────────────
log_data = b"""[2024-04-17 13:00:00] ERROR on machine_004: GAS_VALVE_FAIL - Process gas valve stuck
[2024-04-17 13:01:00] WARNING on machine_004: MFC_OUT_OF_RANGE - Mass flow controller deviation detected
[2024-04-17 13:02:00] INFO on machine_004: system_check_passed - Routine diagnostics completed"""
parsed_log = test_ingest_and_parser(log_data, "test_log_001.log", "log", "LOG/TXT format with multiple severity levels")
test_normalizer(parsed_log["records"], "LOG normalizer")

# ── Test 5: Different severity levels ────────────────────────────────────────
print("\n" + "="*70)
print("SEVERITY TESTS")
print("="*70)

severities = [
    (b'{"timestamp":"2024-04-17T14:00:00","source":"m005","event_type":"test","severity":"CRITICAL","message":"Critical failure"}', "CRITICAL"),
    (b'{"timestamp":"2024-04-17T14:00:00","source":"m005","event_type":"test","severity":"ERROR","message":"Error occurred"}', "ERROR"),
    (b'{"timestamp":"2024-04-17T14:00:00","source":"m005","event_type":"test","severity":"WARNING","message":"Warning issued"}', "WARNING"),
    (b'{"timestamp":"2024-04-17T14:00:00","source":"m005","event_type":"test","severity":"INFO","message":"Informational event"}', "INFO"),
    (b'{"timestamp":"2024-04-17T14:00:00","source":"m005","event_type":"test","severity":"DEBUG","message":"Debug message"}', "DEBUG"),
]

for data, sev_name in severities:
    parsed = test_ingest_and_parser(data, f"test_severity_{sev_name}.json", "json", f"Severity: {sev_name}")
    test_normalizer(parsed["records"], "")

# ── Test 6: Data quality issues ────────────────────────────────────────
print("\n" + "="*70)
print("DATA QUALITY TESTS")
print("="*70)

quality_tests = [
    (b'{"timestamp":"2024-04-17T15:00:00","message":"Missing source and event_type fields"}', "missing_source_and_event_type"),
    (b'{"source":"machine_006","event_type":"test","message":"Missing timestamp"}', "missing_timestamp"),
    (b'{"timestamp":"invalid-date","source":"m006","event_type":"test","severity":"ERROR","message":"Invalid timestamp format"}', "invalid_timestamp_format"),
    (b'{"timestamp":"2024-04-17T16:00:00","source":"m006","event_type":"test","severity":"UNKNOWN_SEV","message":"Unknown severity"}', "unknown_severity"),
]

for data, test_name in quality_tests:
    parsed = test_ingest_and_parser(data, f"test_quality_{test_name}.json", "json", f"Data quality: {test_name}")
    test_normalizer(parsed["records"], "")

# ── Test 7: Multiple records in single file ────────────────────────────────────────
print("\n" + "="*70)
print("BATCH TESTS: Multiple records per file")
print("="*70)

batch_json = b"""[
  {"timestamp":"2024-04-17T17:00:00","source":"batch_m1","event_type":"thermal","severity":"WARNING","message":"Temperature high"},
  {"timestamp":"2024-04-17T17:01:00","source":"batch_m2","event_type":"mechanical","severity":"ERROR","message":"Motor stalled"},
  {"timestamp":"2024-04-17T17:02:00","source":"batch_m3","event_type":"electrical","severity":"CRITICAL","message":"Fuse blown"}
]"""
parsed_batch = test_ingest_and_parser(batch_json, "test_batch_001.json", "json", "JSON array with 3 records")
test_normalizer(parsed_batch["records"], "Batch normalizer")

print("\n" + "="*70)
print("✅ All comprehensive tests completed")
print("="*70)