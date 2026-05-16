import os
import logging
import boto3

log = logging.getLogger(__name__)

dynamo_client = boto3.client(
    "dynamodb",
    endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)

RULES_TABLE = os.getenv("DYNAMODB_TABLE_RULES", "normalization-rules")


def update_feedback_rule(source: str, category: str, approved: bool) -> None:
    """
    Record a human review decision in the normalization-rules DynamoDB table
    and update the confidence_boost for the given (source, category) pair.

    The item stored in the rules table is keyed on (vendorId = source, fieldName = category).
    We track:
      - approval_count   (N)
      - rejection_count  (N)
      - confidence_boost (N) — range [-0.20, +0.20]

    Formula:
        boost = (approvals - rejections) / max(total, 1) * 0.20

    This is a read-modify-write (non-atomic), which is acceptable because
    human reviews arrive at low frequency and occasional races are tolerable.
    """
    try:
        # 1. Read the current rule item (create defaults if absent)
        # Use composite key: vendorId (HASH) + fieldName (RANGE)
        resp = dynamo_client.get_item(
            TableName=RULES_TABLE,
            Key={
                "vendorId": {"S": source},
                "fieldName": {"S": category if category else "default"},
            },
        )
        item = resp.get("Item", {})

        approvals   = int(item.get("approval_count",   {}).get("N", "0"))
        rejections  = int(item.get("rejection_count",  {}).get("N", "0"))

        # 2. Increment the appropriate counter
        if approved:
            approvals += 1
        else:
            rejections += 1

        total = approvals + rejections
        boost = round((approvals - rejections) / max(total, 1) * 0.20, 4)

        # 3. Write the updated item back, preserving any existing fields.
        # Also write the "category" attribute explicitly so lookup_rule() in
        # normalizer.py can read it back as a category override for this source.
        # Without this, the human-corrected category is stored in the key
        # (fieldName) but never surfaced as a field the normalizer can use.
        new_item = {
            **item,                                          # keep recommended_action / etc.
            "vendorId":         {"S": source},
            "fieldName":        {"S": category if category else "default"},
            "category":         {"S": category if category else "default"},
            "approval_count":   {"N": str(approvals)},
            "rejection_count":  {"N": str(rejections)},
            "confidence_boost": {"N": str(boost)},
        }

        dynamo_client.put_item(TableName=RULES_TABLE, Item=new_item)

        log.info(
            "[DYNAMO] feedback recorded  source=%s  category=%s  approved=%s  "
            "approvals=%d  rejections=%d  boost=%.4f",
            source, category, approved, approvals, rejections, boost,
        )

    except Exception as e:
        # Non-fatal — feedback failure must not block the review API response
        log.warning("[DYNAMO] update_feedback_rule failed for source=%s: %s", source, e)