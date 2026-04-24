"""
Kafka client for producing events to priority-based topics.

Topics:
  - logs.p0         → Critical events (fire/safety alarms)
  - logs.p1         → Standard errors
  - logs.p2         → Informational/non-urgent
  - logs.deadletter → Parse failures and unclassified
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from enum import Enum
from uuid import UUID
from datetime import datetime

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for UUID and datetime objects."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class EventPriority(str, Enum):
    """Event priority levels mapping to Kafka topics."""
    P0 = "logs.p0"           # Critical
    P1 = "logs.p1"           # Errors
    P2 = "logs.p2"           # Info/Non-urgent
    DEADLETTER = "logs.deadletter"  # Failed/unclassified


class KafkaClient:
    """
    Async Kafka producer for event streaming.
    
    Usage:
        client = KafkaClient()
        await client.connect()
        await client.send_event(EventPriority.P0, {"message": "Critical alert"})
        await client.close()
    """
    
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        """Initialize Kafka client."""
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        logger.info(f"KafkaClient initialized with {bootstrap_servers}")
    
    async def connect(self) -> None:
        """Connect to Kafka broker."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, cls=JSONEncoder).encode('utf-8'),
                compression_type='gzip'
            )
            await self.producer.start()
            logger.info("Kafka producer connected")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    async def close(self) -> None:
        """Close Kafka connection."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer closed")
    
    async def send_event(
        self,
        priority: EventPriority,
        event: Dict[str, Any],
        key: Optional[str] = None
    ) -> None:
        """
        Send an event to the appropriate Kafka topic.
        
        Args:
            priority: Event priority level (P0, P1, P2, DEADLETTER)
            event: Event payload (dict, will be JSON serialized)
            key: Optional partition key for message ordering
        
        Raises:
            RuntimeError: If producer not connected
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")
        
        try:
            topic = priority.value
            logger.info(f"Sending event to {topic}: {event.get('job_id', 'unknown')}")
            
            await self.producer.send_and_wait(
                topic,
                value=event,
                key=key.encode('utf-8') if key else None
            )
            logger.debug(f"Event sent to {topic}")
        except Exception as e:
            logger.error(f"Failed to send event to {priority.value}: {e}")
            raise
    
    async def send_p0_event(self, event: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send a critical (P0) event."""
        await self.send_event(EventPriority.P0, event, key)
    
    async def send_p1_event(self, event: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send an error (P1) event."""
        await self.send_event(EventPriority.P1, event, key)
    
    async def send_p2_event(self, event: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send an informational (P2) event."""
        await self.send_event(EventPriority.P2, event, key)
    
    async def send_deadletter_event(self, event: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send a deadletter event (failed/unclassified)."""
        await self.send_event(EventPriority.DEADLETTER, event, key)


# Global client instance
_kafka_client: Optional[KafkaClient] = None


async def get_kafka_client() -> KafkaClient:
    """Get or create global Kafka client instance."""
    global _kafka_client
    if _kafka_client is None:
        _kafka_client = KafkaClient()
        await _kafka_client.connect()
    return _kafka_client


async def close_kafka_client() -> None:
    """Close global Kafka client."""
    global _kafka_client
    if _kafka_client:
        await _kafka_client.close()
        _kafka_client = None
