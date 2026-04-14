# Consumes logs.p2
# Batches 100 records / 5 min window
# Bulk inserts into TimescaleDB
# Entry: python -m app.consumers.cold_path
# Container: app-consumer-cold (no port)
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Cold consumer starting...")
    while True:
        time.sleep(60)
