# Step 5: AI normalize via AI model, DynamoDB rules lookup,
#  confidence score, route low-confidence to review queue

"""
INSTRUCTIONS FOR normalize_log() FUNCTION
===========================================

PURPOSE:
  Take parsed log records and normalize them using AI, apply business rules,
  assign confidence scores, and route low-confidence events to a review queue.

INPUTS:
  - parsed_records: List of dictionaries from parse_log() function
    Each record has: timestamp, source, event_type, severity, message, raw_fields

OUTPUTS:
  - Return a dictionary with:
    {
      "normalized_records": [
        {
          "timestamp": "2024-04-17T10:30:00Z",
          "source": "machine_001",
          "event_type": "temperature_warning",
          "severity": "warning",
          "message": "Temperature exceeded threshold",
          "ai_normalized": {
            "category": "thermal_event",           # AI categorization
            "root_cause": "cooling_system_malfunction",  # AI inference
            "recommended_action": "check cooling system",
            "confidence": 0.92                     # 0.0 to 1.0 score
          },
          "requires_review": False,                # Flag for manual review queue
          "review_reason": None                    # Why it needs review (if applicable)
        }
      ],
      "review_queue_items": [                      # Items that need human review
        {
          "id": "uuid",
          "original_record": {...},
          "ai_suggestion": {...},
          "confidence": 0.45,
          "review_reason": "Low confidence in categorization"
        }
      ]
    }

STEPS TO IMPLEMENT:

  1. FOR EACH parsed record:
     a. Call AI model (OpenRouter API via app.config) to categorize and understand the event
        - Send: timestamp, source, event_type, severity, message, raw_fields
        - Receive: category, root_cause, recommended_action, confidence_score
     
     b. Look up business rules in DynamoDB (app.shared.dynamo_client)
        - Query rules table for matching patterns (machine type, event type, etc)
        - Apply any overrides or special handling
     
     c. Combine AI results + business rules
        - If business rule overrides AI: use rule result with high confidence
        - If rule matches but AI disagrees: lower confidence
        - If no matching rule: use AI result as-is
     
     d. Calculate final confidence score (0.0 to 1.0)
        - Start with AI confidence
        - Adjust based on data quality (missing fields = lower confidence)
        - Adjust based on rule matching
     
     e. Decide if needs review:
        - If confidence < 0.7: requires_review = True
        - If conflicting signals (rule vs AI): requires_review = True
        - If any parse errors in raw_fields: requires_review = True
     
     f. If requires_review: add to review_queue_items
     
  2. Return results with all normalized records and review queue items

SERVICES TO USE:
  - app.shared.dynamo_client: Look up business rules in DynamoDB
  - OpenRouter API: Call AI model for categorization (configured in app.config)
    - API endpoint: Check environment variable for OPENROUTER_API_KEY
    - Model: Use gpt-4 or similar (check config)

BUSINESS RULES (stored in DynamoDB):
  - Table name: "rules" or similar
  - Query by: machine_type, event_pattern, severity_threshold
  - Return: category override, min_confidence, recommended_action

AI MODEL PROMPTS (example):
  "Given a manufacturing log event:
   - Machine: {source}
   - Event Type: {event_type}
   - Severity: {severity}
   - Message: {message}
   
   Analyze and provide:
   1. What category of failure/event is this? (thermal, mechanical, electrical, software, unknown)
   2. What is the likely root cause?
   3. What action should be taken?
   4. How confident are you (0-1)? Explain why.
   
   Return JSON with: category, root_cause, recommended_action, confidence"

CONFIDENCE SCORING LOGIC:
  - Start with AI confidence (usually 0.5 to 0.95)
  - Penalize missing fields: -0.1 per important missing field
  - Boost if DynamoDB rule matches: +0.15
  - Boost if multiple signals agree: +0.1
  - Cap at 0.0 (min) and 1.0 (max)

ROUTING LOGIC:
  - High confidence (>0.85): Direct to hot/cold path based on severity
  - Medium confidence (0.7-0.85): Hot path for urgent, cold path for routine
  - Low confidence (<0.7): Add to review queue for human approval

ERROR HANDLING:
  - If AI API fails: set confidence to 0.0 and mark for review
  - If DynamoDB lookup fails: use AI result without rule boost
  - If record is malformed: mark for review with reason "Data quality issues"

NOTES:
  - Review queue items have short TTL (24-48 hours) in DynamoDB
  - Humans can approve/reject items via POST /queue/{id}/review endpoint
  - Approved items bypass confidence checks and go to hot/cold path
"""

import os
import json
import logging
import uuid
import httpx
from app.shared.dynamo import dynamo_client

AI_KEY         = os.getenv("AI_KEY")
AI_MODEL       = os.getenv("AI_MODEL", "nvidia/nemotron-nano-9b-v2")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
RULES_TABLE    = os.getenv("DYNAMODB_TABLE_RULES", "normalization-rules")
REVIEW_TABLE   = os.getenv("DYNAMODB_TABLE_REVIEW", "human-review-queue")
CONFIDENCE_THRESHOLD = float(os.getenv("NORMALIZE_CONFIDENCE_THRESHOLD", "0.85"))

def call_ai(record: dict) -> dict:
    prompt = f"""You are analyzing a manufacturing machine log event.

    Machine: {record.get('source', 'unknown')}
    Event Type: {record.get('event_type', 'unknown')}
    Severity: {record.get('severity', 'unknown')}
    Message: {record.get('message', '')}

    You MUST respond with ONLY a valid JSON object. No explanation, no markdown, no code blocks.
    Use exactly these keys:
    {{
      "category": "thermal|mechanical|electrical|software|unknown",
      "root_cause": "brief string describing the likely root cause",
      "recommended_action": "brief string describing recommended action",
      "confidence": <number between 0.0 and 1.0 representing your certainty>
    }}

    For confidence: use 0.9+ if very clear, 0.7-0.9 if fairly clear, 0.5-0.7 if uncertain, <0.5 if very unclear."""

    try:
        response = httpx.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {AI_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15.0
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        result = json.loads(content)
        
        # Ensure confidence is a valid float between 0 and 1
        if "confidence" in result:
            result["confidence"] = float(result["confidence"])
            result["confidence"] = max(0.0, min(1.0, result["confidence"]))
        else:
            result["confidence"] = 0.5  # Default mid-range confidence if not provided
        
        return result
    except Exception as e:
        logging.warning(f"AI call failed: {e}")
        return {"category": "unknown", "root_cause": "unknown",
                "recommended_action": "manual review", "confidence": 0.0}

def lookup_rule(record: dict) -> dict | None:
    source = record.get("source", "unknown")
    try:
        response = dynamo_client.query(
            TableName=RULES_TABLE,
            KeyConditionExpression="vendorId = :v",
            ExpressionAttributeValues={":v": {"S": source}}
        )
        items = response.get("Items", [])
        if items:
            # Return first matching rule (most specific)
            item = items[0]
            return {
                "category":           item.get("category",           {}).get("S"),
                "recommended_action": item.get("recommended_action", {}).get("S"),
                "min_confidence":     float(item.get("min_confidence", {}).get("N", 0))
            }
    except Exception as e:
        logging.warning(f"DynamoDB rule lookup failed for '{source}': {e}")
    return None

def combine_and_score(record: dict, ai_result: dict, rule: dict | None):
    confidence = float(ai_result.get("confidence", 0.0))
    review_reason = None

    # Penalise missing important fields
    for field in ("source", "event_type", "message"):
        if not record.get(field) or record.get(field) == "unknown":
            confidence -= 0.1

    if rule:
        if rule.get("category") and rule["category"] != ai_result.get("category"):
            # Rule and AI disagree — lower confidence, flag it
            confidence -= 0.1
            review_reason = "Rule and AI categorization conflict"
        else:
            # Rule confirms AI — boost confidence
            confidence += 0.15

    confidence = max(0.0, min(1.0, confidence))  # clamp to [0, 1]

    # Build the final AI result, letting rule override if present
    final = {
        "category":           rule["category"] if rule and rule.get("category") else ai_result.get("category", "unknown"),
        "root_cause":         ai_result.get("root_cause", "unknown"),
        "recommended_action": rule["recommended_action"] if rule and rule.get("recommended_action") else ai_result.get("recommended_action", ""),
        "confidence":         round(confidence, 3)
    }

    return final, confidence, review_reason

def normalize_log(parsed_records: list) -> dict:
    normalized_records = []
    review_queue_items = []

    for record in parsed_records:
        ai_result = call_ai(record)
        rule      = lookup_rule(record)
        final, confidence, review_reason = combine_and_score(record, ai_result, rule)

        requires_review = (
            confidence < CONFIDENCE_THRESHOLD or
            review_reason is not None
        )

        normalized_records.append({
            **record,
            "ai_normalized":  final,
            "requires_review": requires_review,
            "review_reason":   review_reason
        })

        if requires_review:
            review_queue_items.append({
                "id":              str(uuid.uuid4()),
                "original_record": record,
                "ai_suggestion":   final,
                "confidence":      confidence,
                "review_reason":   review_reason or "Low confidence in categorization"
            })

    return {
        "normalized_records":  normalized_records,
        "review_queue_items":  review_queue_items
    }