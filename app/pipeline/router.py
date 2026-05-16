# Step 6: urgency classify (P0/P1/P2/DeadLetter), produce event to Kafka topic

import os
import json
import logging

TOPIC_P0         = "logs.p0"
TOPIC_P1         = "logs.p1"
TOPIC_P2         = "logs.p2"
TOPIC_DEADLETTER = "logs.deadletter"

# Categories considered immediately critical (P0 — human safety risk)
P0_CATEGORIES = {"fire", "safety", "explosion", "vacuum_fault", "critical"}


def determine_topic(normalized_record: dict) -> str | None:
    """
    Classify a normalized record into a Kafka topic.

    P0         → fire / safety / critical events
    P1         → standard machine errors
    P2         → warnings and informational
    deadletter → very low confidence or unclassifiable
    None       → requires human review (goes to DynamoDB queue, not Kafka)
    """
    if normalized_record.get("requires_review"):
        return None  # already routed to review queue by normalizer

    ai         = normalized_record.get("ai_normalized", {})
    severity   = normalized_record.get("severity", "info").lower()
    category   = (ai.get("category") or "unknown").lower()
    confidence = float(ai.get("confidence", 0.0))

    # Very low confidence or completely unclassifiable → dead letter
    if confidence < 0.3 or (category == "unknown" and severity == "error"):
        return TOPIC_DEADLETTER

    # P0: Any CRITICAL severity event
    if severity == "critical":
        return TOPIC_P0

    # P1: Standard errors
    if severity == "error":
        return TOPIC_P1

    # P2: Warnings and informational
    return TOPIC_P2


def route_event(normalized_record: dict, job_id: str = "") -> dict:
    """
    Classify a single normalized record and return its routing decision.
    Does NOT produce to Kafka — main.py step 4.5 handles the actual send
    using get_kafka_client() so it can be properly awaited in async context.

    Args:
        normalized_record: One record from normalize_log()["normalized_records"]
        job_id:            The job UUID from ingest_log(), for message tracing

    Returns:
        {
            "topic":  "logs.p0" | "logs.p1" | "logs.p2" | "logs.deadletter" | None,
            "status": "routed" | "review",
            "reason": "..."   (only present when status == "review")
        }
    """
    topic = determine_topic(normalized_record)

    if topic is None:
        return {
            "topic":  None,
            "status": "review",
            "reason": normalized_record.get("review_reason") or "Low confidence"
        }

    return {"topic": topic, "status": "routed"}
