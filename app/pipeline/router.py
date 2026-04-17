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

    # P0: Critical safety events
    if severity == "error" and any(p0 in category for p0 in P0_CATEGORIES):
        return TOPIC_P0

    # P1: Standard errors
    if severity == "error":
        return TOPIC_P1

    # P2: Warnings and informational
    return TOPIC_P2


def produce_to_kafka(topic: str, message: dict):
    """
    Produce a message to a Kafka topic.
    Fails gracefully if kafka_client is not yet implemented.
    """
    try:
        from app.shared.kafka_client import kafka_producer
        kafka_producer.produce(
            topic,
            key=(message.get("job_id") or "").encode("utf-8"),
            value=json.dumps(message, default=str).encode("utf-8")
        )
        kafka_producer.flush()
        logging.info(f"Produced to {topic}: job_id={message.get('job_id')}")
    except ImportError:
        logging.warning("kafka_client not yet implemented — skipping Kafka produce")
    except Exception as e:
        logging.error(f"Failed to produce to Kafka topic '{topic}': {e}")
        raise


def route_event(normalized_record: dict, job_id: str = "") -> dict:
    """
    Classify a single normalized record and produce it to the correct Kafka topic.

    Args:
        normalized_record: One record from normalize_log()["normalized_records"]
        job_id:            The job UUID from ingest_log(), for message tracing

    Returns:
        {
            "topic":  "logs.p0" | "logs.p1" | "logs.p2" | "logs.deadletter" | None,
            "status": "routed" | "review" | "failed",
            "reason": "..."   (only present when status != "routed")
        }
    """
    topic = determine_topic(normalized_record)

    # Goes to human review queue — not Kafka
    if topic is None:
        return {
            "topic":  None,
            "status": "review",
            "reason": normalized_record.get("review_reason") or "Low confidence"
        }

    message = {"job_id": job_id, **normalized_record}

    try:
        produce_to_kafka(topic, message)
        return {"topic": topic, "status": "routed"}
    except Exception as e:
        logging.error(f"Routing failed for topic '{topic}', falling back to dead letter: {e}")
        # Best-effort fallback to dead letter
        try:
            produce_to_kafka(TOPIC_DEADLETTER, message)
        except Exception:
            pass
        return {"topic": TOPIC_DEADLETTER, "status": "failed", "error": str(e)}
