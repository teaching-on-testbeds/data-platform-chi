#!/usr/bin/env python3
"""
Feature Fetcher - Retrieves features from Redis and Postgres
"""
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

from config import config

logger = logging.getLogger(__name__)


class FeatureFetcher:
    def __init__(self):
        # Redis client
        self.redis_client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            decode_responses=True
        )

        # Postgres connection
        self.db_conn = psycopg2.connect(config.database_url)

        logger.info(f"FeatureFetcher initialized (Redis: {config.redis_host}:{config.redis_port})")

    def fetch_redis_features(self, image_id: str) -> Dict[str, Any]:
        start_time = time.time()
        current_timestamp = time.time()

        features = {}

        try:
            # Get window counts for views (no 1-min due to 5-min granularity)
            features['views_5min'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['5min'], 'views'
            )
            features['views_1hr'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['1hr'], 'views'
            )

            # Get window counts for comments (no 1-min due to 5-min granularity)
            features['comments_5min'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['5min'], 'comments'
            )
            features['comments_1hr'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['1hr'], 'comments'
            )

            # Get window counts for flags
            features['flags_5min'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['5min'], 'flags'
            )
            features['flags_1hr'] = self._get_window_count(
                image_id, current_timestamp, config.window_sizes['1hr'], 'flags'
            )

            # Get total flags counter
            features['total_flags'] = int(self.redis_client.get(f"image:{image_id}:total_flags") or 0)

            # Get metadata (hash)
            metadata = self.redis_client.hgetall(f"image:{image_id}:metadata")
            features['redis_metadata'] = metadata  # Store for later use

            latency_ms = (time.time() - start_time) * 1000
            features['redis_latency_ms'] = latency_ms

            logger.debug(f"Redis features fetched for {image_id[:8]}... in {latency_ms:.2f}ms")

        except redis.RedisError as e:
            logger.error(f"Redis error fetching features for {image_id}: {e}")
            features['error'] = str(e)

        return features

    def _get_window_count(self, image_id: str, current_timestamp: float, window_size: int, metric_type: str) -> int:
        window_start = current_timestamp - window_size

        # Build key name (match stream_consumer naming)
        if window_size == 60:
            key = f"image:{image_id}:{metric_type}:1min"
        elif window_size == 300:
            key = f"image:{image_id}:{metric_type}:5min"
        elif window_size == 3600:
            key = f"image:{image_id}:{metric_type}:1hr"
        else:
            key = f"image:{image_id}:{metric_type}:{window_size}s"

        try:
            count = self.redis_client.zcount(key, window_start, current_timestamp)
            return count
        except redis.RedisError as e:
            logger.warning(f"Error getting window count for {key}: {e}")
            return 0

    def fetch_postgres_metadata(self, image_id: str) -> Dict[str, Any]:
        start_time = time.time()
        features = {}

        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        i.category,
                        i.caption,
                        i.created_at,
                        i.user_id,
                        u.created_at as user_created_at,
                        (SELECT COUNT(*) FROM images WHERE user_id = i.user_id) as user_image_count
                    FROM images i
                    JOIN users u ON i.user_id = u.id
                    WHERE i.id = %s
                """

                cursor.execute(query, (image_id,))
                result = cursor.fetchone()

                if result:
                    # Basic metadata
                    features['category'] = result['category'] or 'Unknown'
                    features['caption'] = result['caption'] or ''
                    features['caption_length'] = len(features['caption'])
                    features['has_caption'] = 1 if features['caption'] else 0
                    features['uploaded_at'] = result['created_at']

                    # Temporal features
                    now = datetime.utcnow()
                    if result['created_at'].tzinfo is None:
                        uploaded_at = result['created_at']
                    else:
                        uploaded_at = result['created_at'].replace(tzinfo=None)

                    time_diff = now - uploaded_at
                    features['time_since_upload_seconds'] = int(time_diff.total_seconds())

                    # User features
                    features['user_id'] = str(result['user_id'])
                    features['user_image_count'] = result['user_image_count']

                    user_created = result['user_created_at']
                    if user_created.tzinfo is not None:
                        user_created = user_created.replace(tzinfo=None)

                    user_age = now - user_created
                    features['user_age_days'] = int(user_age.total_seconds() / 86400)

                else:
                    logger.warning(f"Image {image_id} not found in database")
                    features['error'] = 'Image not found'

            latency_ms = (time.time() - start_time) * 1000
            features['postgres_latency_ms'] = latency_ms

            logger.debug(f"Postgres metadata fetched for {image_id[:8]}... in {latency_ms:.2f}ms")

        except psycopg2.Error as e:
            logger.error(f"Postgres error fetching metadata for {image_id}: {e}")
            features['error'] = str(e)

        return features

    def fetch_all_features(self, image_id: str) -> Dict[str, Any]:
        
        start_time = time.time()

        # Fetch from both sources
        redis_features = self.fetch_redis_features(image_id)
        postgres_features = self.fetch_postgres_metadata(image_id)

        # Merge features
        all_features = {**redis_features, **postgres_features}

        # Calculate total latency
        total_latency_ms = (time.time() - start_time) * 1000
        all_features['total_latency_ms'] = total_latency_ms

        # Check for errors
        if 'error' in redis_features or 'error' in postgres_features:
            logger.warning(f"Errors occurred while fetching features for {image_id}")

        logger.info(
            f"All features fetched for {image_id[:8]}... in {total_latency_ms:.2f}ms "
            f"(Redis: {all_features.get('redis_latency_ms', 0):.2f}ms, "
            f"Postgres: {all_features.get('postgres_latency_ms', 0):.2f}ms)"
        )

        return all_features

    def close(self):
        if self.redis_client:
            self.redis_client.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("FeatureFetcher connections closed")
