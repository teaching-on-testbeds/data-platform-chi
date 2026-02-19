#!/usr/bin/env python3
import os


class Config:

    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "gourmetgram-stream-consumer")
        self.kafka_topics = os.getenv("KAFKA_TOPICS", "gourmetgram.views,gourmetgram.comments").split(",")

        # Redis Configuration
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))

        # PostgreSQL Configuration (for milestone persistence)
        self.database_url = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/gourmetgram")

        # Window Sizes (in seconds)
        self.window_1min = 60
        self.window_5min = 300
        self.window_1hr = 3600

        # Alert Thresholds
        self.viral_threshold_views_5min = int(os.getenv("VIRAL_THRESHOLD_VIEWS_5MIN", "100"))
        self.suspicious_threshold_comments_1min = int(os.getenv("SUSPICIOUS_THRESHOLD_COMMENTS_1MIN", "10"))
        self.popular_threshold_views_1hr = int(os.getenv("POPULAR_THRESHOLD_VIEWS_1HR", "50"))

        # View Milestones for Persistence
        self.view_milestones = [10, 100, 1000]

        # Moderation Request Topic (risk scoring service listens on this)
        self.moderation_topic = os.getenv("MODERATION_TOPIC", "gourmetgram.moderation_requests")

        # Logging Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # Consumer Configuration
        self.consumer_max_poll_records = int(os.getenv("CONSUMER_MAX_POLL_RECORDS", "100"))
        self.consumer_auto_commit = os.getenv("CONSUMER_AUTO_COMMIT", "true").lower() == "true"


# Global config instance
config = Config()
