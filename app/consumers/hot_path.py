"""
Hot Path Consumer - Real-time Processing of Critical & Error Events

Consumes from:
  - logs.p0 (Critical safety events) → Process immediately
  - logs.p1 (Standard errors) → Process with slight buffering

Actions:
  - Write to TimescaleDB immediately
  - Push WebSocket alerts to connected clients
  - Broadcast severity level to determine alert urgency

Entry point:
  python -m app.consumers.hot_path

Container:
  app-consumer-hot (port 8083)
"""

import os
import json
import gzip
import logging
import asyncio
from typing import Set
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer

from app.shared.db import insert_normalized_event
from app.shared.kafka_client import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

# Configuration
KAFKA_TOPICS = ["logs.p0", "logs.p1"]
KAFKA_GROUP = "consumer-hot-path"

# WebSocket connection manager
class ConnectionManager:
    """Manage multiple WebSocket connections for broadcasting alerts."""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        """Remove disconnected WebSocket."""
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, alert: dict):
        """Send alert to all connected clients."""
        if not self.active_connections:
            logger.debug("No WebSocket clients connected")
            return
        
        message = json.dumps(alert)
        disconnected = set()
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                disconnected.add(connection)
        
        # Clean up failed connections
        for connection in disconnected:
            await self.disconnect(connection)


# Global manager
manager = ConnectionManager()

# FastAPI app
app = FastAPI(title="Hot Path Consumer")

# Kafka consumer
kafka_consumer: AIOKafkaConsumer = None


async def process_event(topic: str, event: dict):
    """
    Process a single event from Kafka.
    
    Args:
        topic: Source topic (logs.p0 or logs.p1)
        event: Parsed event dictionary
    """
    try:
        # Extract event details
        job_id = event.get("job_id") or str(uuid4())
        timestamp_str = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        source = event.get("source", "unknown")
        severity = event.get("severity", "ERROR")
        message = event.get("message", "")
        
        # AI normalized data
        ai_data = event.get("ai_normalized", {})
        ai_category = ai_data.get("category", "unknown")
        ai_root_cause = ai_data.get("root_cause", "unknown")
        ai_recommended_action = ai_data.get("recommended_action", "")
        confidence = float(ai_data.get("confidence", 0.0))
        
        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except Exception:
            timestamp = datetime.now(timezone.utc)
        
        # Insert into database
        await insert_normalized_event(
            job_id=job_id,
            timestamp=timestamp,
            source=source,
            event_type=event.get("event_type", "unknown"),
            severity=severity,
            message=message,
            ai_category=ai_category,
            ai_root_cause=ai_root_cause,
            ai_recommended_action=ai_recommended_action,
            confidence_score=confidence,
            requires_review=False,
            review_reason=""
        )
        logger.info(f"Inserted event from {source}: {message}")
        
        # Build WebSocket alert
        alert = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "job_id": job_id,
            "source": source,
            "severity": severity,
            "message": message,
            "category": ai_category,
            "confidence": confidence,
            "topic": topic,
            "action": ai_recommended_action
        }
        
        # Broadcast to WebSocket clients
        await manager.broadcast(alert)
        logger.info(f"Alert broadcast for {source}")
        
    except Exception as e:
        logger.error(f"Failed to process event: {e}")
        # Log to deadletter for manual review
        logger.error(f"Event would be sent to deadletter: {event}")


async def consume_messages():
    """Consume messages from Kafka topics."""
    global kafka_consumer
    
    try:
        kafka_consumer = AIOKafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: _deserialize_message(m)
        )
        
        await kafka_consumer.start()
        logger.info(f"Kafka consumer started. Listening to topics: {KAFKA_TOPICS}")
        
        try:
            async for message in kafka_consumer:
                topic = message.topic
                event = message.value
                
                logger.info(f"Received event from {topic}: job_id={event.get('job_id')}")
                await process_event(topic, event)
                
        finally:
            await kafka_consumer.stop()
            
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        await asyncio.sleep(5)  # Retry after 5 seconds
        # Could implement reconnect logic here


def _deserialize_message(data: bytes) -> dict:
    """
    Deserialize Kafka message value.
    Handles both gzip-compressed and plain JSON formats.
    """
    try:
        # Try gzip decompression first
        decompressed = gzip.decompress(data)
        return json.loads(decompressed.decode('utf-8'))
    except (OSError, EOFError):
        # Not gzipped, try plain JSON
        try:
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            return {}


# Background task for consuming
async def startup_consumer():
    """Start Kafka consumer on app startup."""
    asyncio.create_task(consume_messages())
    logger.info("Hot path consumer task started")


async def shutdown_consumer():
    """Stop Kafka consumer on app shutdown."""
    global kafka_consumer
    if kafka_consumer:
        await kafka_consumer.stop()
    logger.info("Hot path consumer stopped")


@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    await startup_consumer()


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    await shutdown_consumer()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "consumer-hot",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_clients": len(manager.active_connections)
    }


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint for real-time alert streaming.
    
    Clients connect here to receive P0 and P1 alerts in real-time
    as they're processed by the hot path consumer.
    
    Example client:
        const ws = new WebSocket('ws://localhost:8083/ws/alerts');
        ws.onmessage = (event) => {
            const alert = JSON.parse(event.data);
            console.log(`${alert.severity}: ${alert.message}`);
        };
    """
    await manager.connect(websocket)
    
    try:
        # Keep connection alive, receive heartbeats
        while True:
            data = await websocket.receive_text()
            # Client can send ping/heartbeat to keep connection alive
            if data == "ping":
                await websocket.send_text("pong")
    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        logger.info("WebSocket client disconnected")
    
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8083)
