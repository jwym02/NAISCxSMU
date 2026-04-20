"""
app/config.py — Centralised configuration for all subsystems.

All settings are read from environment variables with sensible defaults.
Import individual config objects:
    from app.config import DATABASE, KAFKA, AI, MINIO, REDIS, SERVICES, CONSUMERS
Or validate/log everything at startup:
    from app.config import validate_config, log_config_summary
"""

import os
import logging

logger = logging.getLogger(__name__)


# ── Logging ────────────────────────────────────────────────────────────────────

class LoggingConfig:
    """Process-wide logging settings."""

    level: str = os.getenv("LOG_LEVEL", "INFO")
    debug_mode: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"

    @classmethod
    def apply(cls) -> None:
        """Apply log level to the root logger. Call once at startup."""
        numeric = getattr(logging, cls.level.upper(), logging.INFO)
        logging.basicConfig(
            level=numeric,
            format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        if cls.debug_mode:
            logging.getLogger().setLevel(logging.DEBUG)


# ── Database (TimescaleDB / asyncpg) ──────────────────────────────────────────

class DatabaseConfig:
    """TimescaleDB connection and pool settings."""

    host: str     = os.getenv("TIMESCALE_HOST",     "timescaledb")
    port: int     = int(os.getenv("TIMESCALE_PORT", "5432"))
    database: str = os.getenv("TIMESCALE_DB",       "logparser_db")
    user: str     = os.getenv("TIMESCALE_USER",     "logparser")
    password: str = os.getenv("TIMESCALE_PASSWORD", "logparser_secret")
    pool_size: int = int(os.getenv("TIMESCALE_POOL_SIZE", "5"))

    @classmethod
    def dsn(cls) -> str:
        """Return a asyncpg-compatible connection string."""
        return (
            f"postgresql://{cls.user}:{cls.password}"
            f"@{cls.host}:{cls.port}/{cls.database}"
        )


# ── Kafka ─────────────────────────────────────────────────────────────────────

class KafkaConfig:
    """Kafka broker, topic, and consumer-group configuration."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Topics
    topic_p0:         str = os.getenv("KAFKA_TOPIC_P0",         "logs.p0")
    topic_p1:         str = os.getenv("KAFKA_TOPIC_P1",         "logs.p1")
    topic_p2:         str = os.getenv("KAFKA_TOPIC_P2",         "logs.p2")
    topic_deadletter: str = os.getenv("KAFKA_TOPIC_DEADLETTER", "logs.deadletter")

    # Consumer group IDs
    group_id_hot:        str = os.getenv("KAFKA_GROUP_ID_HOT",        "consumer-hot-path")
    group_id_cold:       str = os.getenv("KAFKA_GROUP_ID_COLD",       "consumer-group-cold")
    group_id_deadletter: str = os.getenv("KAFKA_GROUP_ID_DEADLETTER", "consumer-group-deadletter")

    # Producer settings
    compression_type: str = os.getenv("KAFKA_COMPRESSION_TYPE", "gzip")
    request_timeout_ms: int = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))
    retry_backoff_ms:   int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS",   "500"))

    @classmethod
    def all_topics(cls) -> list:
        return [cls.topic_p0, cls.topic_p1, cls.topic_p2, cls.topic_deadletter]


# ── AI / LLM (OpenRouter) ─────────────────────────────────────────────────────

class AIConfig:
    """OpenRouter / LLM settings for log normalisation and NL2SQL."""

    api_key: str  = os.getenv("AI_KEY", "")          # Required — validated at startup
    model: str    = os.getenv("AI_MODEL", "nvidia/nemotron-nano-9b-v2")
    base_url: str = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")

    # Temperature presets
    # Lower = more deterministic (better for structured extraction)
    # Higher = more creative (acceptable for summaries)
    temperature_normalization: float = float(
        os.getenv("AI_TEMP_NORMALIZATION", "0.1")
    )
    temperature_nl2sql: float = float(
        os.getenv("AI_TEMP_NL2SQL", "0.0")
    )

    # Confidence threshold below which events are sent to review queue
    confidence_threshold: float = float(
        os.getenv("NORMALIZE_CONFIDENCE_THRESHOLD", "0.85")
    )

    @classmethod
    def is_configured(cls) -> bool:
        """Return True if an API key is present."""
        return bool(cls.api_key)


# ── MinIO (object storage) ────────────────────────────────────────────────────

class MinIOConfig:
    """MinIO connection and bucket settings."""

    endpoint:   str = os.getenv("MINIO_ENDPOINT",   "minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    use_ssl:   bool = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

    # Buckets
    bucket_raw_logs: str = os.getenv("MINIO_BUCKET_RAW_LOGS", "raw-logs")


# ── Redis ─────────────────────────────────────────────────────────────────────

class RedisConfig:
    """Redis connection settings (used for caching / rate-limiting)."""

    host:     str           = os.getenv("REDIS_HOST",     "redis")
    port:     int           = int(os.getenv("REDIS_PORT", "6379"))
    password: str | None    = os.getenv("REDIS_PASSWORD") or None   # None = no auth

    @classmethod
    def url(cls) -> str:
        """Return a redis:// URL for use with aioredis / redis-py."""
        auth = f":{cls.password}@" if cls.password else ""
        return f"redis://{auth}{cls.host}:{cls.port}/0"


# ── Service ports ─────────────────────────────────────────────────────────────

class ServicesConfig:
    """Internal service port map. Matches docker-compose port bindings."""

    pipeline:   int = int(os.getenv("PORT_PIPELINE",   "8080"))
    query:      int = int(os.getenv("PORT_QUERY",      "8081"))
    hot_path:   int = int(os.getenv("PORT_HOT",        "8083"))
    cold_path:  int = int(os.getenv("PORT_COLD",       "8084"))
    deadletter: int = int(os.getenv("PORT_DEADLETTER", "8085"))


# ── Consumer-specific tuning ──────────────────────────────────────────────────

class ConsumerConfig:
    """Behavioural knobs for the three Kafka consumers."""

    # Cold path — batch flush settings
    cold_batch_size:     int = int(os.getenv("COLD_BATCH_SIZE",             "100"))
    cold_batch_interval: int = int(os.getenv("COLD_BATCH_INTERVAL_SECONDS", "300"))

    # Dead-letter — spike detection
    dl_spike_threshold:   int = int(os.getenv("DL_SPIKE_THRESHOLD",       "10"))
    dl_spike_window_secs: int = int(os.getenv("DL_SPIKE_WINDOW_SECONDS",  "300"))


# ── Module-level singletons ───────────────────────────────────────────────────
# Import these directly:  from app.config import DATABASE, KAFKA, AI …

LOGGING   = LoggingConfig()
DATABASE  = DatabaseConfig()
KAFKA     = KafkaConfig()
AI        = AIConfig()
MINIO     = MinIOConfig()
REDIS     = RedisConfig()
SERVICES  = ServicesConfig()
CONSUMERS = ConsumerConfig()


# ── Utility functions ─────────────────────────────────────────────────────────

def get_database_config() -> dict:
    """Return database connection kwargs suitable for asyncpg.create_pool()."""
    return {
        "host":     DATABASE.host,
        "port":     DATABASE.port,
        "database": DATABASE.database,
        "user":     DATABASE.user,
        "password": DATABASE.password,
        "min_size": 2,
        "max_size": DATABASE.pool_size,
        "command_timeout": 60,
    }


def validate_config() -> bool:
    """
    Check that all required settings are present.
    Logs a warning for each missing value.
    Returns True if the config is fully valid, False otherwise.
    """
    ok = True

    if not AI.is_configured():
        logger.warning(
            "AI_KEY is not set — AI normalisation will be unavailable. "
            "Set the AI_KEY environment variable."
        )
        ok = False

    if not MINIO.access_key or MINIO.access_key == "minioadmin":
        logger.warning("MINIO_ACCESS_KEY is using the default insecure value.")

    if not DATABASE.password or DATABASE.password == "logparser_secret":
        logger.warning("TIMESCALE_PASSWORD is using the default insecure value.")

    return ok


def log_config_summary() -> None:
    """
    Log a non-sensitive summary of the active configuration.
    Call once at service startup so operators can confirm settings.
    Secrets (passwords, API keys) are redacted.
    """
    logger.info("─── Configuration summary ───────────────────────────────")
    logger.info("  Database  : %s:%s/%s (pool=%s)",
                DATABASE.host, DATABASE.port, DATABASE.database, DATABASE.pool_size)
    logger.info("  Kafka     : %s  topics=%s",
                KAFKA.bootstrap_servers, KAFKA.all_topics())
    logger.info("  AI model  : %s  url=%s  confidence_threshold=%.2f",
                AI.model, AI.base_url, AI.confidence_threshold)
    logger.info("  AI key    : %s", "set" if AI.is_configured() else "MISSING")
    logger.info("  MinIO     : %s  bucket=%s  ssl=%s",
                MINIO.endpoint, MINIO.bucket_raw_logs, MINIO.use_ssl)
    logger.info("  Redis     : %s:%s", REDIS.host, REDIS.port)
    logger.info("  Cold batch: size=%d  interval=%ds",
                CONSUMERS.cold_batch_size, CONSUMERS.cold_batch_interval)
    logger.info("  DL spike  : threshold=%d  window=%ds",
                CONSUMERS.dl_spike_threshold, CONSUMERS.dl_spike_window_secs)
    logger.info("  Log level : %s  debug=%s", LOGGING.level, LOGGING.debug_mode)
    logger.info("────────────────────────────────────────────────────────")
