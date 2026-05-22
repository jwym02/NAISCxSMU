# Step 5: AI normalize via AI model, DynamoDB rules lookup,
#  confidence score, route low-confidence to review queue

import os
import json
import logging
import uuid
import httpx
import numpy as np
from collections import defaultdict
from sklearn.ensemble import IsolationForest
from app.shared.dynamo import dynamo_client

AI_KEY         = os.getenv("AI_KEY")
AI_MODEL       = os.getenv("AI_MODEL", "nvidia/nemotron-nano-9b-v2")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
RULES_TABLE    = os.getenv("DYNAMODB_TABLE_RULES", "normalization-rules")
REVIEW_TABLE   = os.getenv("DYNAMODB_TABLE_REVIEW", "human-review-queue")
CONFIDENCE_THRESHOLD = float(os.getenv("NORMALIZE_CONFIDENCE_THRESHOLD", "0.70"))

DEFAULT_CATEGORIES = [
    "thermal", "mechanical", "electrical", "gas_leak", "contamination",
    "process_drift", "safety", "software", "maintenance", "unknown"
]

# ── Semiconductor Fab Grounding Context ──────────────────────────────────────
FAB_GROUNDING_CONTEXT = """
SEMICONDUCTOR FAB EQUIPMENT GROUNDING CONTEXT:
Use this reference to anchor your categorization to real fab failure modes.

THIN FILM DEPOSITION (PECVD / LPCVD / CVD / ALD / PVD / Sputter):
  Aliases: Centura, Vantage, VECTOR, Endura, Producer, any tool containing CVD/ALD/PVD/PECVD
  Common faults: gas flow anomalies (SiH4, N2O, WF6, TMA, precursors),
                 chamber pressure deviation, substrate temperature excursion,
                 plasma ignition failure, precursor concentration out-of-spec
  → categories: gas_leak | thermal | electrical | process_drift

PLASMA ETCH / DRY ETCH (Etch, RIE, DRIE):
  Aliases: Kiyo, Vantex, Flex, Episode, any tool containing ETCH or RIE
  Common faults: RF arc / plasma instability, MFC deviation (HBr, Cl2, CF4, O2, NF3),
                 electrostatic chuck temperature, etch rate drift, end-point detection miss
  → categories: electrical | gas_leak | mechanical | thermal | process_drift

LITHOGRAPHY / EUV (Scanner, Stepper, EUV, DUV):
  Aliases: TWINSCAN, LITHO-SCANNER, any tool containing LITHO, SCANNER, or EUV
  Common faults: overlay out-of-spec, focus/dose error, reticle particle,
                 stage vibration, Sn plasma source fault (EUV), collector mirror contamination
  → categories: mechanical | process_drift | electrical | maintenance

CHEMICAL MECHANICAL PLANARIZATION (CMP):
  Aliases: Reflexion, Mirra, any tool containing CMP
  Common faults: slurry flow/concentration, removal rate deviation,
                 pad condition, wafer carrier motor temp, chiller failure
  → categories: mechanical | thermal | process_drift | contamination

THERMAL PROCESSING (Furnace, RTP, Anneal, Oxidation, Diffusion):
  Aliases: ANNEAL, DIFFUSION, RTP, any tool containing FURNACE or THERMAL
  Common faults: zone temperature non-uniformity, lamp failure, ramp rate deviation,
                 ambient gas flow (O2, N2, H2), boat/quartz contamination
  → categories: thermal | electrical | process_drift

ATOMIC LAYER DEPOSITION (ALD):
  Aliases: ALD, Pulsar, any tool containing ALD
  Common faults: precursor pulse timing, half-cycle miscount, purge line contamination,
                 growth-per-cycle drift, valve actuation failure
  → categories: process_drift | gas_leak | mechanical

ION IMPLANTATION (Implanter):
  Aliases: IMPLANT, NV8200, any tool containing IMPLANT
  Common faults: beam current drop, source filament failure, neutralizer fault,
                 dose accuracy, energy contamination, lot quarantine
  → categories: electrical | mechanical | process_drift

WET PROCESSING (Wet Bench, Track, Spin Coater):
  Aliases: WET-BENCH, WET-STATION, TRACK, any tool containing WET or TRACK
  Common faults: chemical concentration out-of-spec (HF, SC1, H2SO4),
                 bath temperature deviation, rinse flow, chemical spill
  → categories: contamination | gas_leak | process_drift | safety

ELECTROCHEMICAL PLATING (ECP / Electrofill):
  Aliases: ECP, ELECTROFILL, any tool containing ECP or PLATING
  Common faults: plating current/voltage deviation, bath chemistry, agitation failure,
                 seed layer continuity, bath temperature
  → categories: process_drift | electrical | contamination

EPITAXIAL GROWTH (Epi):
  Aliases: EPI, EPITAXIAL, any tool containing EPI
  Common faults: growth rate deviation, dopant concentration, gas purity,
                 susceptor temperature uniformity, autodoping
  → categories: process_drift | thermal | gas_leak

WAFER HANDLING (Robot Arm, EFEM, Handler, Load Port):
  Aliases: ROBOT, ROBOT-ARM, HANDLER, EFEM
  Common faults: end-effector collision, wafer drop, slot mapping error,
                 vacuum loss at chuck, load port door fault
  → categories: mechanical

GAS DELIVERY (MFC, Gas Panel, Gas Cabinet, VMB):
  Aliases: GAS-PANEL, GAS-CABINET, MFC, VMB
  Common faults: pressure drop (possible line rupture), toxic gas sensor alarm
                 (SiH4, HBr, NF3, Cl2), isolation valve trip, flow deviation
  → categories: gas_leak | safety | mechanical

INSPECTION & METROLOGY (SEM, OCD, Particle Counter):
  Aliases: SEM, OCD, SURFSCAN, METROLOGY, KLA
  Common faults: particle count alarm, calibration drift, recipe load failure
  → categories: maintenance | contamination | process_drift

SUPPORT EQUIPMENT (Pump, Chiller, UPS, Abatement):
  Aliases: PUMP, CHILLER, UPS, ABATEMENT, VACUUM
  Common faults: pump overheat, coolant leak, vacuum loss, power fault
  → categories: mechanical | thermal | electrical

MANUFACTURING EXECUTION SYSTEM (MES, HOST, SECS-GEM):
  Aliases: MES-SERVER, HOST, SECS, E84
  Common faults: communication timeout, recipe load failure, lot tracking error
  → categories: software

ALARM SEVERITY REFERENCE:
  CRITICAL → immediate safety risk (fire, gas leak, E-stop, facility evacuation)
  ERROR    → process or equipment fault requiring engineer intervention
  WARNING  → parameter approaching spec limit; monitor and prepare to intervene
  INFO     → normal operational record; no action required
"""


# ── Category helpers ──────────────────────────────────────────────────────────

async def get_available_categories() -> list[str]:
    """Fetch all unique categories from DynamoDB rules table; fallback to defaults."""
    try:
        response = dynamo_client.scan(
            TableName=RULES_TABLE,
            ProjectionExpression="category"
        )
        categories = set(DEFAULT_CATEGORIES)
        for item in response.get("Items", []):
            if "category" in item and "S" in item["category"]:
                cat = item["category"]["S"]
                if cat:
                    categories.add(cat)
        return sorted(list(categories))
    except Exception as e:
        logging.warning(f"Failed to fetch categories from DynamoDB: {e}. Using defaults.")
        return DEFAULT_CATEGORIES


# ── Few-Shot RAG: Category Pool ───────────────────────────────────────────────

def fetch_approved_pool(limit: int = 50) -> dict[str, list]:
    """
    Scans the review table ONCE per batch for recently approved events.
    Returns { category: [{"message", "source", "confidence"}, ...] }
    One DB call per batch — every approval benefits every future machine in the same category.
    """
    pool: dict[str, list] = {}
    try:
        response = dynamo_client.scan(
            TableName=REVIEW_TABLE,
            FilterExpression="approved = :a",
            ExpressionAttributeValues={":a": {"BOOL": True}},
            Limit=limit,
        )
        for item in response.get("Items", []):
            cat = item.get("category", {}).get("S", "unknown")
            pool.setdefault(cat, []).append({
                "message":    item.get("message",    {}).get("S", ""),
                "source":     item.get("source",     {}).get("S", ""),
                "confidence": float(item.get("confidence", {}).get("N", "0")),
            })
    except Exception as e:
        logging.warning(f"fetch_approved_pool failed: {e}")
    return pool


def infer_category_heuristic(record: dict) -> str:
    """
    Fast keyword scan to get a tentative category for pool lookup.
    Not the final category — just good enough to pick relevant few-shot examples.
    """
    text = (
        (record.get("message")    or "") + " " +
        (record.get("event_type") or "") + " " +
        (record.get("source")     or "")
    ).lower()

    if any(k in text for k in ["fire", "evacuate", "toxic", "emergency", "gas detector"]):
        return "safety"
    if any(k in text for k in ["sih4", "hbr", "nf3", "cl2", "gas leak", "mfc", "pressure drop", "gas_leak"]):
        return "gas_leak"
    if any(k in text for k in ["temperature", "temp", "overheat", "thermal", "heater", "cooling"]):
        return "thermal"
    if any(k in text for k in ["voltage", "current", "rf", "arc", "power", "electrical", "vsup", "dcbx", "almid"]):
        return "electrical"
    if any(k in text for k in ["collision", "robot", "wafer drop", "mechanical", "valve", "pump"]):
        return "mechanical"
    if any(k in text for k in ["particle", "contamination", "spill", "chemical"]):
        return "contamination"
    if any(k in text for k in ["overlay", "removal rate", "etch rate", "drift", "out-of-spec", "recipe"]):
        return "process_drift"
    if any(k in text for k in ["timeout", "mes", "communication", "software", "crash"]):
        return "software"
    if any(k in text for k in ["pm due", "calibration", "maintenance", "scheduled"]):
        return "maintenance"
    return "unknown"


def rank_examples(pool_for_category: list, source: str, limit: int = 3) -> list:
    """
    Ranks approved examples by relevance to the current record's machine:
      1. Same exact machine (highest relevance)
      2. Same equipment class (e.g. all ETCH tools)
      3. Any machine in the category (cross-machine knowledge)
    """
    parts = source.upper().replace("TOOL_", "").replace("MACHINE_", "").replace("DEVICE_", "")
    machine_class = parts.split("-")[0] if "-" in parts else parts

    same_machine = [e for e in pool_for_category if e["source"] == source]
    same_class   = [e for e in pool_for_category if machine_class in e["source"].upper()
                    and e["source"] != source]
    other        = [e for e in pool_for_category if machine_class not in e["source"].upper()]

    return (same_machine + same_class + other)[:limit]


# ── AI call ───────────────────────────────────────────────────────────────────

async def call_ai(
    record: dict,
    category_hint: str | None = None,
    available_categories: list[str] | None = None,
    few_shot_examples: list[dict] | None = None,
) -> dict:
    if available_categories is None:
        available_categories = await get_available_categories()

    # Build RAG section from category pool examples
    rag_section = ""
    if few_shot_examples:
        lines = "\n".join([
            f"  {i+1}. Machine: {ex['source']}  |  Message: \"{ex['message'][:120]}\"\n"
            f"     → Approved category: {ex.get('category', '?')} | Confidence: {ex['confidence']:.2f}"
            for i, ex in enumerate(few_shot_examples)
        ])
        rag_section = f"""

PREVIOUSLY APPROVED EVENTS (few-shot examples — cross-machine learning):
{lines}
Use these as grounding examples. Classify the new event below with the same reasoning."""

    # Fallback hint if no examples but a rule exists
    hint_section = ""
    if category_hint and category_hint != "unknown" and not few_shot_examples:
        hint_section = f"""

PRIOR HUMAN FEEDBACK: Engineers confirmed the category as '{category_hint}'.
Weight this strongly unless the message clearly contradicts it."""

    prompt = f"""{FAB_GROUNDING_CONTEXT}
---
You are an expert analyst at a semiconductor fabrication plant (fab).
Analyze this machine log event and classify it.

Machine: {record.get('source', 'unknown')}
Event Type: {record.get('event_type', 'unknown')}
Severity: {record.get('severity', 'unknown')}
Message: {record.get('message', '')}{rag_section}{hint_section}

You MUST respond with ONLY a valid JSON object — no explanation, no markdown.
Use exactly these keys:
{{
  "category": "thermal|mechanical|electrical|gas_leak|contamination|process_drift|safety|software|maintenance|unknown",
  "root_cause": "1-2 sentences on the most likely root cause",
  "recommended_action": "specific engineer action",
  "confidence": <0.0-1.0>
}}
"""

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {AI_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": AI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "log_classification",
                            "strict": True,
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "category": {
                                        "type": "string",
                                        "enum": [
                                            "thermal", "mechanical", "electrical", "gas_leak",
                                            "contamination", "process_drift", "safety",
                                            "software", "maintenance", "unknown"
                                        ]
                                    },
                                    "root_cause":         {"type": "string"},
                                    "recommended_action": {"type": "string"},
                                    "confidence":         {"type": "number", "minimum": 0, "maximum": 1}
                                },
                                "required": ["category", "root_cause", "recommended_action", "confidence"],
                                "additionalProperties": False
                            }
                        }
                    },
                },
                timeout=15.0
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            result = json.loads(content)

        result["confidence"] = max(0.0, min(1.0, float(result.get("confidence", 0.5))))

        if result.get("category") not in available_categories:
            logging.warning(f"AI returned unknown category '{result.get('category')}', defaulting to 'unknown'")
            result["category"] = "unknown"

        return result
    except Exception as e:
        logging.warning(f"AI call failed: {e}")
        return {"category": "unknown", "root_cause": "unknown",
                "recommended_action": "manual review", "confidence": 0.0}


# ── DynamoDB rule lookup ──────────────────────────────────────────────────────

def lookup_rule(record: dict) -> dict | None:
    source = record.get("source", "unknown")
    try:
        response = dynamo_client.query(
            TableName=RULES_TABLE,
            KeyConditionExpression="vendorId = :v",
            ExpressionAttributeValues={":v": {"S": source}}
        )
        items = response.get("Items", [])
        if not items:
            return None

        best = max(items, key=lambda i: int(i.get("approval_count", {}).get("N", "0")))
        return {
            "category":           best.get("category",           {}).get("S"),
            "recommended_action": best.get("recommended_action", {}).get("S"),
            "min_confidence":     float(best.get("min_confidence", {}).get("N", 0)),
            "confidence_boost":   sum(
                float(i.get("confidence_boost", {}).get("N", "0")) for i in items
            ),
        }
    except Exception as e:
        logging.warning(f"DynamoDB rule lookup failed for '{source}': {e}")
    return None


def combine_and_score(record: dict, ai_result: dict, rule: dict | None):
    confidence = float(ai_result.get("confidence", 0.0))
    review_reason = None

    for field in ("source", "event_type", "message"):
        if not record.get(field) or record.get(field) == "unknown":
            confidence -= 0.1

    if rule:
        if rule.get("category") and rule["category"] != ai_result.get("category"):
            confidence -= 0.1
            review_reason = "Rule and AI categorization conflict"
        else:
            confidence += 0.15

        feedback_boost = float(rule.get("confidence_boost", 0.0))
        if feedback_boost != 0.0:
            confidence += feedback_boost
            logging.debug(
                "Applied feedback_boost=%.4f for source=%s → confidence now %.3f",
                feedback_boost, record.get("source"), confidence,
            )

    confidence = max(0.0, min(1.0, confidence))

    final = {
        "category":           rule["category"] if rule and rule.get("category") else ai_result.get("category", "unknown"),
        "root_cause":         ai_result.get("root_cause", "unknown"),
        "recommended_action": rule["recommended_action"] if rule and rule.get("recommended_action") else ai_result.get("recommended_action", ""),
        "confidence":         round(confidence, 3)
    }

    return final, confidence, review_reason


# ── Temporal anomaly detection ────────────────────────────────────────────────

async def detect_temporal_anomaly(records: list[dict]) -> dict | None:
    """
    Analyzes 2+ normalized records from the SAME source for escalation patterns.
    Returns a trend_alert dict if anomaly detected, None if sequence is normal.
    """
    if len(records) < 2:
        return None

    source = records[0].get("source", "unknown")

    event_lines = "\n".join([
        f"  [{r.get('timestamp', '?')}] "
        f"severity={r.get('severity','?')} "
        f"category={r.get('ai_normalized', {}).get('category', '?')} "
        f"confidence={r.get('ai_normalized', {}).get('confidence', 0):.2f} "
        f"message=\"{r.get('message', '')[:120]}\""
        for r in records
    ])

    prompt = f"""You are a predictive maintenance AI at a semiconductor fab.

Machine: {source}
Recent event sequence ({len(records)} events, chronological):
{event_lines}

Analyze for temporal patterns. Respond with ONLY a JSON object:
{{
  "is_anomaly": <true|false>,
  "pattern": "<one sentence describing the observed trend, or 'No significant trend'>",
  "predicted_severity": "normal|warning|critical",
  "estimated_time_to_critical": "<e.g. '~25 minutes' or 'N/A'>",
  "recommended_action": "<specific engineer action, or 'Continue monitoring'>",
  "confidence": <0.0-1.0>
}}

Mark is_anomaly=true if you observe any of:
- Monotonically increasing or decreasing numeric parameter values
- Severity escalation (INFO → WARNING → ERROR → CRITICAL)
- Same fault code repeating at increasing frequency
- Category shifting mid-sequence toward safety or critical
- Any pattern that suggests imminent equipment failure"""

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                OPENROUTER_URL,
                headers={"Authorization": f"Bearer {AI_KEY}", "Content-Type": "application/json"},
                json={
                    "model": AI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {"type": "json_object"},
                },
                timeout=20.0,
            )
            response.raise_for_status()
            result = json.loads(response.json()["choices"][0]["message"]["content"])
            result["machine"] = source
            return result if result.get("is_anomaly") else None
    except Exception as e:
        logging.warning(f"Temporal anomaly detection failed for {source}: {e}")
        return None


# ── Isolation Forest: online novelty scoring ──────────────────────────────────
#
# Trains an IsolationForest model incrementally as events arrive.
# Every normalized event receives a novelty_score (0.0 = normal, 1.0 = highly anomalous).
# Complements the LLM: the LLM understands semantics; the IF detects statistical outliers.
#
# Design:
#   - Features are fully numerical — severity level, confidence, category, field presence,
#     message length, review flag.
#   - Model is retrained every _IF_RETRAIN_EVERY events once _IF_MIN_SAMPLES is reached.
#   - Until enough data is collected, novelty_score is None (model warming up).
#   - A score above _IF_ALERT_THRESHOLD forces requires_review=True regardless of LLM confidence.

_IF_MIN_SAMPLES   = 50   # events needed before first fit — keeps IF silent during small demos
_IF_RETRAIN_EVERY = 20   # refit every N new events after that
_IF_ALERT_THRESHOLD = 0.72  # novelty_score above this → escalate to review

_if_model: IsolationForest | None = None
_if_buffer: list[list[float]] = []   # accumulates feature vectors across all batches

_SEVERITY_ENC = {
    "critical": 4, "error": 3, "warning": 2, "warn": 2,
    "info": 1, "debug": 0, "unknown": 0,
}
_CATEGORY_ENC = {
    "safety": 0, "electrical": 1, "thermal": 2, "gas_leak": 3,
    "mechanical": 4, "contamination": 5, "process_drift": 6,
    "software": 7, "maintenance": 8, "unknown": 9,
}


def _extract_features(record: dict, ai_result: dict, confidence: float) -> list[float]:
    """
    Build a fixed-length numerical feature vector for one event.

    Features (7 dimensions):
      0  severity_enc     — severity level as integer (0–4)
      1  confidence       — final blended confidence score (0.0–1.0)
      2  category_enc     — category as integer (0–9)
      3  has_source       — 1 if source is known, else 0
      4  has_event_type   — 1 if event_type is known, else 0
      5  msg_length_norm  — message length capped and normalised to [0, 1]
      6  requires_review  — 1 if LLM+rules already flagged for review, else 0
    """
    severity  = (record.get("severity") or "unknown").lower()
    category  = (ai_result.get("category") or "unknown").lower()
    message   = record.get("message") or ""

    return [
        float(_SEVERITY_ENC.get(severity, 0)),
        float(np.clip(confidence, 0.0, 1.0)),
        float(_CATEGORY_ENC.get(category, 9)),
        1.0 if (record.get("source") or "unknown") != "unknown" else 0.0,
        1.0 if (record.get("event_type") or "unknown") != "unknown" else 0.0,
        float(min(len(message) / 500.0, 1.0)),
        1.0 if record.get("requires_review", False) else 0.0,
    ]


def _score_novelty(features: list[float]) -> float | None:
    """
    Add features to the buffer, optionally refit the model, and return a novelty score.

    Returns:
        float in [0.0, 1.0] — higher = more anomalous (model sees this as an outlier).
        None  — not enough data yet (model warming up).

    The raw IsolationForest output is score_samples() which returns negative floats;
    more negative = more anomalous.  We invert and clip to [0, 1] so that:
        0.0  → perfectly normal (model has seen many events like this)
        1.0  → highly anomalous (statistically unlike anything seen before)
    """
    global _if_model, _if_buffer

    _if_buffer.append(features)
    n = len(_if_buffer)

    # Fit (or refit) the model when we cross the minimum threshold or hit a retrain tick
    should_fit = (n == _IF_MIN_SAMPLES) or (n > _IF_MIN_SAMPLES and n % _IF_RETRAIN_EVERY == 0)
    if should_fit:
        _if_model = IsolationForest(
            n_estimators=100,
            contamination=0.1,   # expect ~10 % of events to be anomalous
            random_state=42,
        )
        _if_model.fit(np.array(_if_buffer))
        logging.info(
            "[IsolationForest] model %s on %d events",
            "fitted" if n == _IF_MIN_SAMPLES else "refitted", n,
        )

    if _if_model is None:
        return None   # warming up — not enough events yet

    raw = _if_model.score_samples(np.array([features]))[0]
    # score_samples returns values roughly in [-0.8, 0.1]; invert so anomaly → high score
    novelty = float(np.clip(-raw, 0.0, 1.0))
    return round(novelty, 4)


# ── Main pipeline entry point ─────────────────────────────────────────────────

async def normalize_log(parsed_records: list) -> dict:
    import asyncio

    normalized_records = []
    review_queue_items = []

    # Fetch available categories and approved pool ONCE for the whole batch
    available_categories, approved_pool = await asyncio.gather(
        get_available_categories(),
        asyncio.get_event_loop().run_in_executor(None, lambda: fetch_approved_pool(limit=50)),
    )

    # Pre-compute per-record inputs (rules + few-shot examples) — all fast/synchronous
    per_record_inputs = []
    for record in parsed_records:
        rule          = lookup_rule(record)
        category_hint = rule.get("category") if rule and rule.get("category") else None
        tentative_cat = category_hint or infer_category_heuristic(record)
        few_shot_examples = rank_examples(
            pool_for_category=approved_pool.get(tentative_cat, []),
            source=record.get("source", "unknown"),
        )
        per_record_inputs.append((record, rule, category_hint, few_shot_examples))

    # Fire ALL AI calls in parallel — N records → N concurrent requests
    # Each call_ai() has its own 15s timeout; total wall-clock ≈ 1 slow call, not N×slow
    ai_results = await asyncio.gather(
        *[
            call_ai(
                record,
                category_hint=category_hint,
                available_categories=available_categories,
                few_shot_examples=few_shot_examples if few_shot_examples else None,
            )
            for record, _rule, category_hint, few_shot_examples in per_record_inputs
        ],
        return_exceptions=True,   # one failure won't abort the others
    )

    # Merge AI results with their records
    for (record, rule, _hint, _examples), ai_result in zip(per_record_inputs, ai_results):
        # If gather caught an exception for this slot, fall back gracefully
        if isinstance(ai_result, Exception):
            logging.warning("AI call raised exception for record %s: %s", record.get("source"), ai_result)
            ai_result = {"category": "unknown", "root_cause": "unknown",
                         "recommended_action": "manual review", "confidence": 0.0}

        final, confidence, review_reason = combine_and_score(record, ai_result, rule)

        requires_review = (
            confidence < CONFIDENCE_THRESHOLD or
            review_reason is not None
        )

        # ── Isolation Forest novelty scoring ──────────────────────────────────
        # Score the event against the statistical distribution of all events seen
        # so far this session.  A high novelty_score means the IF model considers
        # this event an outlier — independently of what the LLM decided.
        features      = _extract_features(record, final, confidence)
        novelty_score = _score_novelty(features)

        # If ML flags this as highly anomalous, escalate to review regardless of
        # LLM confidence — the two systems disagree, which is itself a signal.
        if novelty_score is not None and novelty_score >= _IF_ALERT_THRESHOLD:
            requires_review = True
            ml_reason = f"ML anomaly detected (novelty_score={novelty_score:.2f})"
            review_reason = (review_reason + " | " + ml_reason) if review_reason else ml_reason
            logging.info(
                "[IsolationForest] anomaly — source=%s  category=%s  "
                "confidence=%.2f  novelty_score=%.4f",
                record.get("source"), final.get("category"), confidence, novelty_score,
            )

        normalized_records.append({
            **record,
            "ai_normalized":   final,
            "requires_review": requires_review,
            "review_reason":   review_reason,
            "novelty_score":   novelty_score,   # None until model warms up (≥10 events)
        })

        if requires_review:
            review_queue_items.append({
                "id":              str(uuid.uuid4()),
                "original_record": record,
                "ai_suggestion":   final,
                "confidence":      confidence,
                "novelty_score":   novelty_score,
                "review_reason":   review_reason or "Low confidence in categorization"
            })

    # Temporal anomaly detection — group records by machine, run in parallel
    by_source: dict[str, list] = defaultdict(list)
    for record in normalized_records:
        by_source[record.get("source", "unknown")].append(record)

    sources_with_multi = [(src, recs) for src, recs in by_source.items() if len(recs) >= 2]

    anomaly_results = await asyncio.gather(
        *[detect_temporal_anomaly(recs) for _src, recs in sources_with_multi],
        return_exceptions=True,
    ) if sources_with_multi else []

    trend_alerts = []
    for result in anomaly_results:
        if isinstance(result, Exception):
            logging.warning("Temporal anomaly detection raised: %s", result)
            continue
        if result:
            trend_alerts.append(result)
            logging.info("Temporal anomaly detected for %s: %s", result.get("machine"), result.get("pattern"))

    return {
        "normalized_records": normalized_records,
        "review_queue_items": review_queue_items,
        "trend_alerts":       trend_alerts,
    }
