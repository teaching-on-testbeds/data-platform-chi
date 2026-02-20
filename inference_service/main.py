#!/usr/bin/env python3
"""
Moderation Risk Scoring Service - Entry Point

Consumes moderation requests from Kafka (published by the stream consumer
when viral/suspicious thresholds are crossed) and runs inference.
"""
import json
import logging
import signal
import sys
import psycopg2

from kafka import KafkaConsumer
from config import config
from moderator import Moderator

logger = logging.getLogger(__name__)


def setup_logging():
    level = getattr(logging, config.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def store_decision(conn, result):
    """Persist moderation decision to Postgres."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO moderation
                    (target_type, target_id, inference_mode, model_version,
                     risk_score, trigger_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    'image',
                    result['image_id'],
                    result['inference_mode'],
                    result.get('model_version'),
                    result['moderation_probability'],
                    result.get('trigger'),
                )
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to store moderation decision: {e}")
        conn.rollback()


def main():
    setup_logging()

    logger.info("=" * 70)
    logger.info("GOURMETGRAM MODERATION INFERENCE SERVICE")
    logger.info("=" * 70)
    logger.info(f"Kafka: {config.kafka_bootstrap_servers}")
    logger.info(f"Topic: {config.moderation_topic}")
    logger.info(f"Redis: {config.redis_host}:{config.redis_port}")
    logger.info("=" * 70)

    # Connect to Postgres
    db_conn = psycopg2.connect(config.database_url)
    db_conn.autocommit = False
    logger.info("Connected to PostgreSQL")

    # Initialize moderator (connects to Redis + Postgres for feature fetching)
    moderator = Moderator()

    # Connect to Kafka
    consumer = KafkaConsumer(
        config.moderation_topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=config.kafka_consumer_group,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logger.info(f"Listening on topic: {config.moderation_topic}")
    logger.info("")
    logger.info("Waiting for moderation requests...")
    logger.info("")

    def shutdown(signum, frame):
        logger.info("Shutting down...")
        consumer.close()
        moderator.close()
        db_conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Main loop
    for message in consumer:
        data = message.value
        image_id = data.get('image_id')
        trigger = data.get('trigger', 'unknown')

        if not image_id:
            continue

        result = moderator.run_inference(image_id, trigger)

        if 'error' not in result:
            store_decision(db_conn, result)


if __name__ == "__main__":
    main()
