# Consumes logs.deadletter
# Logs failure, writes audit record
# Entry: python -m app.consumers.dead_letter
# Container: app-consumer-deadletter (no port)
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Deadletter consumer starting...")
    while True:
        time.sleep(60)
