#!/usr/bin/env python3
import logging
import json
from typing import Dict, Any, List, Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import config

logger = logging.getLogger(__name__)

# Kafka consumer for GourmetGram event streams
class StreamConsumer:

    def __init__(self):
        self.consumer: KafkaConsumer = None
        self.message_count = 0
        self.messages_by_topic = {}

    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                *config.kafka_topics,
                bootstrap_servers=config.kafka_bootstrap_servers,
                group_id=config.kafka_consumer_group,
                auto_offset_reset='earliest',  # Start from beginning if no offset stored
                enable_auto_commit=config.consumer_auto_commit,
                max_poll_records=config.consumer_max_poll_records,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                # No consumer_timeout_ms - keep polling indefinitely
            )

            logger.info(f"Kafka consumer connected")
            logger.info(f"Bootstrap servers: {config.kafka_bootstrap_servers}")
            logger.info(f"Consumer group: {config.kafka_consumer_group}")
            logger.info(f"Subscribed topics: {config.kafka_topics}")

            return True

        except KafkaError as e:
            logger.error(f"Failed to connect Kafka consumer: {e}")
            raise

    def consume(self, message_handler: Callable[[str, Dict[str, Any]], None]):
        logger.info("Starting message consumption...")

        try:
            for message in self.consumer:
                # Track message count
                self.message_count += 1
                topic = message.topic
                self.messages_by_topic[topic] = self.messages_by_topic.get(topic, 0) + 1

                # Log every 10th message
                if self.message_count % 10 == 0:
                    logger.info(f"Processed {self.message_count} messages | By topic: {self.messages_by_topic}")

                # Process message
                try:
                    message_handler(topic, message.value)

                except Exception as e:
                    logger.error(f"Error processing message from {topic}: {e}", exc_info=True)
                    # Continue processing other messages

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
            raise

        except Exception as e:
            logger.error(f"Fatal error in consumer loop: {e}", exc_info=True)
            raise

    def close(self):
        if self.consumer:
            logger.info("=" * 60)
            logger.info("KAFKA CONSUMER STATISTICS")
            logger.info(f"Total messages consumed: {self.message_count}")
            logger.info("Messages by topic:")
            for topic, count in self.messages_by_topic.items():
                logger.info(f"  {topic}: {count}")
            logger.info("=" * 60)

            self.consumer.close()
            logger.info("Kafka consumer closed")


# Global consumer instance
stream_consumer = StreamConsumer()
