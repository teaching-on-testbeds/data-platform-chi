#!/usr/bin/env python3
import logging
import signal
import sys
from typing import Dict, Any

from config import config
from redis_client import redis_client
from db_client import db_client
from kafka_consumer import stream_consumer
from alert_manager import get_alert_stats, init_producer
import aggregators


def setup_logging():
    level = getattr(logging, config.log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Reduce noise from kafka and redis libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)


def message_handler(topic: str, message_data: Dict[str, Any]):
    event_type = message_data.get('event_type', 'unknown')

    try:
        if event_type == 'view':
            aggregators.process_view_event(message_data)

        elif event_type == 'comment':
            aggregators.process_comment_event(message_data)

        elif event_type == 'upload':
            aggregators.process_upload_event(message_data)

        elif event_type == 'flag':
            aggregators.process_flag_event(message_data)

        else:
            logger.warning(f"Unknown event type '{event_type}' from topic {topic}")

    except Exception as e:
        logger.error(f"Error processing {event_type} event: {e}", exc_info=True)


def graceful_shutdown(signum, frame):
    logger.info(f"\nReceived signal {signum}, initiating graceful shutdown...")

    # Print final statistics
    logger.info("")
    logger.info("=" * 70)
    logger.info("FINAL STATISTICS")
    logger.info("=" * 70)

    # Alert stats
    alert_stats = get_alert_stats()
    logger.info("Alert Statistics:")
    logger.info(f"  Viral content alerts: {alert_stats['viral_alerts']}")
    logger.info(f"  Suspicious activity alerts: {alert_stats['suspicious_alerts']}")
    logger.info(f"  Popular post alerts: {alert_stats['popular_alerts']}")
    logger.info(f"  Milestone alerts: {alert_stats['milestone_alerts']}")

    logger.info("=" * 70)

    # Close connections
    stream_consumer.close()
    redis_client.close()
    db_client.close()

    logger.info("Stream consumer shutdown complete.")
    sys.exit(0)


def main():
    global logger
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("GOURMETGRAM STREAM CONSUMER")
    logger.info("=" * 70)
    logger.info(f"Kafka Bootstrap: {config.kafka_bootstrap_servers}")
    logger.info(f"Consumer Group: {config.kafka_consumer_group}")
    logger.info(f"Topics: {config.kafka_topics}")
    logger.info(f"Redis: {config.redis_host}:{config.redis_port}")
    logger.info(f"Database: {config.database_url.split('@')[1] if '@' in config.database_url else 'configured'}")
    logger.info("")
    logger.info("Alert Thresholds:")
    logger.info(f"  Viral content: {config.viral_threshold_views_5min} views/5min")
    logger.info(f"  Suspicious activity: {config.suspicious_threshold_comments_1min} comments/1min")
    logger.info(f"  Popular post: {config.popular_threshold_views_1hr} views/1hr")
    logger.info("")
    logger.info("Milestone Persistence:")
    logger.info(f"  View milestones: {config.view_milestones}")
    logger.info(f"  Database table: image_milestones")
    logger.info("=" * 70)
    logger.info("")

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    try:
        # Connect to Redis
        logger.info("Connecting to Redis...")
        redis_client.connect()

        # Connect to database for milestone persistence
        logger.info("Connecting to PostgreSQL...")
        db_client.connect()

        # Initialize moderation request producer
        logger.info("Initializing moderation request producer...")
        init_producer()

        # Connect to Kafka
        logger.info("Connecting to Kafka...")
        stream_consumer.connect()

        # Start consuming messages
        logger.info("")
        logger.info("Stream consumer is running! Press Ctrl+C to stop.")
        logger.info("")

        stream_consumer.consume(message_handler)

    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
        graceful_shutdown(signal.SIGINT, None)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        graceful_shutdown(signal.SIGTERM, None)


if __name__ == "__main__":
    main()
