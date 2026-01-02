"""
Lazy Kafka consumer initialization using environment variables.
"""

import os
from typing import Optional

from kafka import KafkaConsumer, TopicPartition
from reggie_core import logs

LOG = logs.logger(__file__)

# Global consumer instance (lazy loaded) - used for offset queries only
_consumer: Optional[KafkaConsumer] = None


def create_consumer() -> KafkaConsumer:
    """
    Create a new Kafka consumer instance for streaming.
    Each SSE connection should get its own consumer.
    Uses environment variables:
    - KAFKA_SERVER (default: kafka.lfpconnect.io:443)
    - KAFKA_SSL (default: true) - if 'true', uses SSL security protocol

    Raises ValueError if required configuration is missing or invalid.
    """
    server = os.getenv("KAFKA_SERVER", "kafka.lfpconnect.io:443")
    ssl_str = os.getenv("KAFKA_SSL", "true").lower()

    if not server:
        raise ValueError("KAFKA_SERVER environment variable is required")

    security_protocol = "SSL" if ssl_str == "true" else "PLAINTEXT"

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=server,
            security_protocol=security_protocol,
            enable_auto_commit=False,
            consumer_timeout_ms=None,  # No timeout - wait indefinitely for new messages
        )
        LOG.info(
            f"Kafka consumer created: server={server}, security={security_protocol}"
        )
        return consumer
    except Exception as e:
        raise ValueError(f"Failed to create Kafka consumer: {e}") from e


def get_consumer() -> KafkaConsumer:
    """
    Lazy load and return a shared Kafka consumer instance (for offset queries only).
    For streaming, use create_consumer() instead.
    Uses environment variables:
    - KAFKA_SERVER (default: kafka.lfpconnect.io:443)
    - KAFKA_SSL (default: true) - if 'true', uses SSL security protocol

    Raises ValueError if required configuration is missing or invalid.
    """
    global _consumer

    if _consumer is not None:
        return _consumer

    server = os.getenv("KAFKA_SERVER", "kafka.lfpconnect.io:443")
    ssl_str = os.getenv("KAFKA_SSL", "true").lower()

    if not server:
        raise ValueError("KAFKA_SERVER environment variable is required")

    security_protocol = "SSL" if ssl_str == "true" else "PLAINTEXT"

    try:
        _consumer = KafkaConsumer(
            bootstrap_servers=server,
            security_protocol=security_protocol,
            enable_auto_commit=False,
            consumer_timeout_ms=5000,  # Timeout for offset queries
        )
        LOG.info(
            f"Kafka consumer initialized: server={server}, security={security_protocol}"
        )
    except Exception as e:
        raise ValueError(f"Failed to initialize Kafka consumer: {e}") from e

    return _consumer


def get_latest_offset(topic: str, partition: int = 0) -> int:
    """
    Get the latest offset for a topic partition.

    Args:
        topic: Kafka topic name
        partition: Partition number (default: 0)

    Returns:
        Latest offset value

    Raises:
        ValueError if topic/partition not found or consumer error
    """
    consumer = get_consumer()

    partitions = consumer.partitions_for_topic(topic)
    if partitions is None or partition not in partitions:
        raise ValueError(f"Topic '{topic}' partition {partition} not found")

    tp = TopicPartition(topic, partition)
    end_offsets = consumer.end_offsets([tp])

    if tp not in end_offsets:
        raise ValueError(
            f"Could not get offset for topic '{topic}' partition {partition}"
        )

    return end_offsets[tp]
