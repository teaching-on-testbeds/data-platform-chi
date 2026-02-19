import os
import json
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaProducerManager:

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.enabled = True  # Set to False if Kafka is not available
        self.event_count = 0  # Track total events published
        self.events_by_topic = {}  # Track events per topic

    async def start(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,  # Wait for leader acknowledgment
                compression_type='gzip',  # Compress messages
                request_timeout_ms=30000
            )

            await self.producer.start()
            logger.info(f"Kafka producer started successfully. Bootstrap servers: {self.bootstrap_servers}")

        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            logger.warning("Kafka publishing will be disabled. API will continue to work.")
            self.enabled = False
            self.producer = None

    async def stop(self):
        if self.producer:
            try:
                # Log final statistics
                logger.info("=" * 60)
                logger.info("KAFKA PRODUCER STATISTICS")
                logger.info(f"Total events published: {self.event_count}")
                logger.info("Events by topic:")
                for topic, count in self.events_by_topic.items():
                    logger.info(f"  {topic}: {count}")
                logger.info("=" * 60)

                await self.producer.stop()
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "total_events": self.event_count,
            "events_by_topic": self.events_by_topic.copy()
        }

    async def publish_event(self, topic: str, event_data: Dict[str, Any]) -> bool:
        if not self.enabled or not self.producer:
            logger.warning(f"Kafka disabled. Event not published to {topic}: {event_data.get('event_type', 'unknown')}")
            return False

        try:
            # Send event asynchronously without waiting for response
            await self.producer.send(topic, value=event_data)

            # Track event counts
            self.event_count += 1
            self.events_by_topic[topic] = self.events_by_topic.get(topic, 0) + 1

            # Log every event at INFO level with details
            event_type = event_data.get('event_type', 'unknown')
            event_id = event_data.get('image_id') or event_data.get('comment_id') or event_data.get('flag_id', 'N/A')
            logger.info(f"Kafka: Published {event_type} event to {topic} | ID: {event_id[:8] if isinstance(event_id, str) else event_id} | Total: {self.event_count}")

            return True

        except KafkaError as e:
            logger.error(f"Kafka error publishing {event_data.get('event_type', 'unknown')} to {topic}: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error publishing {event_data.get('event_type', 'unknown')} to {topic}: {e}")
            return False


# Global singleton instance
kafka_producer = KafkaProducerManager()


async def get_kafka_producer() -> KafkaProducerManager:
    return kafka_producer