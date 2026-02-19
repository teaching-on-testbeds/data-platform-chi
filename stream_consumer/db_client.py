#!/usr/bin/env python3
import logging
import time
import uuid
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, DatabaseError

from config import config

logger = logging.getLogger(__name__)


class DatabaseClient:
    def __init__(self):
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._tracked_milestones = {}

    def connect(self, max_retries: int = 5, retry_delay: int = 2):
        for attempt in range(1, max_retries + 1):
            try:
                self.conn = psycopg2.connect(config.database_url)
                self.conn.autocommit = False
                logger.info(f"Connected to PostgreSQL database")
                return True

            except OperationalError as e:
                logger.warning(f"Database connection attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to database after all retries")
                    raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def persist_milestone(self, image_id: str, milestone_type: str, milestone_value: int, reached_at: float) -> bool:
        # Create tracking key
        tracking_key = f"{image_id}:{milestone_type}:{milestone_value}"

        # Check if we already persisted this milestone
        if tracking_key in self._tracked_milestones:
            return False

        try:
            with self.conn.cursor() as cursor:
                # Check if milestone already exists in database
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM image_milestones
                    WHERE image_id = %s AND milestone_type = %s AND milestone_value = %s
                    """,
                    (image_id, milestone_type, milestone_value)
                )

                count = cursor.fetchone()[0]
                if count > 0:
                    # Already exists, mark as tracked
                    self._tracked_milestones[tracking_key] = True
                    return False

                # Convert timestamp to datetime
                from datetime import datetime
                reached_at_dt = datetime.fromtimestamp(reached_at)

                # Generate UUID for milestone (convert to string for psycopg2)
                milestone_id = str(uuid.uuid4())

                # Insert milestone (ensure image_id is also a string)
                cursor.execute(
                    """
                    INSERT INTO image_milestones (id, image_id, milestone_type, milestone_value, reached_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (milestone_id, str(image_id), milestone_type, milestone_value, reached_at_dt)
                )

                self.conn.commit()

                # Mark as tracked
                self._tracked_milestones[tracking_key] = True

                logger.info(
                    f"Milestone persisted: {image_id[:8]}... reached {milestone_value} {milestone_type} "
                    f"at {reached_at_dt.isoformat()}"
                )

                return True

        except DatabaseError as e:
            logger.error(f"Error persisting milestone for {image_id}: {e}")
            self.conn.rollback()
            return False

    def get_milestones_for_image(self, image_id: str) -> list:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT milestone_type, milestone_value, reached_at
                    FROM image_milestones
                    WHERE image_id = %s
                    ORDER BY reached_at ASC
                    """,
                    (image_id,)
                )

                return cursor.fetchall()

        except DatabaseError as e:
            logger.error(f"Error retrieving milestones for {image_id}: {e}")
            return []


# Global database client instance
db_client = DatabaseClient()
