#!/usr/bin/env python3
import logging
import time
from typing import Dict, Any
from datetime import datetime

from config import config
from redis_client import redis_client
from alert_manager import check_and_alert

logger = logging.getLogger(__name__)

# Process a view event and update Redis aggregations
def process_view_event(event_data: Dict[str, Any]):
    image_id = event_data.get('image_id')
    if not image_id:
        logger.warning(f"View event missing image_id: {event_data}")
        return

    # Parse timestamp
    timestamp_str = event_data.get('viewed_at')
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp()
    else:
        timestamp = time.time()

    # Use timestamp for score, but add nanosecond precision to value for guaranteed uniqueness
    # Format: "timestamp:nanoseconds" ensures no collisions even under extreme load
    timestamp_value = f"{timestamp}:{time.time_ns() % 1000000}"

    # Add to sorted sets with TTL for each window
    redis_client.zadd_with_ttl(
        f"image:{image_id}:views:1min",
        score=timestamp,
        value=timestamp_value,
        ttl=config.window_1min
    )

    redis_client.zadd_with_ttl(
        f"image:{image_id}:views:5min",
        score=timestamp,
        value=timestamp_value,
        ttl=config.window_5min
    )

    redis_client.zadd_with_ttl(
        f"image:{image_id}:views:1hr",
        score=timestamp,
        value=timestamp_value,
        ttl=config.window_1hr
    )

    # Increment total views (no TTL - persistent)
    total_views = redis_client.incr(f"image:{image_id}:total_views")

    # Clean up expired entries from sorted sets
    _cleanup_expired_entries(image_id, timestamp, "views")

    # Get current counts for all windows
    views_1min = _get_window_count(image_id, timestamp, config.window_1min, "views")
    views_5min = _get_window_count(image_id, timestamp, config.window_5min, "views")
    views_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "views")

    # Log every 10th view or milestone
    if total_views % 10 == 0 or total_views in config.view_milestones:
        logger.info(
            f"View: {image_id[:8]}... | "
            f"1m:{views_1min} 5m:{views_5min} 1h:{views_1hr} | "
            f"Total:{total_views}"
        )

    # Check for alerts
    check_and_alert(
        image_id=image_id,
        views_1min=views_1min,
        views_5min=views_5min,
        views_1hr=views_1hr,
        comments_1min=0,  # Not available in view events
        total_views=total_views
    )

# Process a comment event and update Redis aggregations
def process_comment_event(event_data: Dict[str, Any]):
    image_id = event_data.get('image_id')
    comment_id = event_data.get('comment_id')

    if not image_id or not comment_id:
        logger.warning(f"Comment event missing image_id or comment_id: {event_data}")
        return

    # Parse timestamp
    timestamp_str = event_data.get('created_at')
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp()
    else:
        timestamp = time.time()

    # Use comment_id as value to ensure uniqueness (multiple comments can have same timestamp)
    value = f"{comment_id}:{timestamp}"

    # Add to sorted sets with TTL for each window (same pattern as views)
    redis_client.zadd_with_ttl(
        f"image:{image_id}:comments:1min",
        score=timestamp,
        value=value,
        ttl=config.window_1min
    )

    redis_client.zadd_with_ttl(
        f"image:{image_id}:comments:5min",
        score=timestamp,
        value=value,
        ttl=config.window_5min
    )

    redis_client.zadd_with_ttl(
        f"image:{image_id}:comments:1hr",
        score=timestamp,
        value=value,
        ttl=config.window_1hr
    )

    # Increment total comments (no TTL - persistent)
    total_comments = redis_client.incr(f"image:{image_id}:total_comments")

    # Clean up expired entries from sorted sets
    _cleanup_expired_entries(image_id, timestamp, "comments")

    # Get current counts for all windows
    comments_1min = _get_window_count(image_id, timestamp, config.window_1min, "comments")
    comments_5min = _get_window_count(image_id, timestamp, config.window_5min, "comments")
    comments_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "comments")

    # Log every comment
    logger.info(
        f"Comment: {image_id[:8]}... | "
        f"1m:{comments_1min} 5m:{comments_5min} 1h:{comments_1hr} | "
        f"Total:{total_comments}"
    )

    # Get current view counts (needed for alerts)
    views_1min = _get_window_count(image_id, timestamp, config.window_1min, "views")
    views_5min = _get_window_count(image_id, timestamp, config.window_5min, "views")
    views_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "views")
    total_views = int(redis_client.get(f"image:{image_id}:total_views") or 0)

    # Check for alerts
    check_and_alert(
        image_id=image_id,
        views_1min=views_1min,
        views_5min=views_5min,
        views_1hr=views_1hr,
        comments_1min=comments_1min,
        total_views=total_views
    )

# Process an upload event - initialize metadata in Redis
def process_upload_event(event_data: Dict[str, Any]):
    image_id = event_data.get('image_id')
    if not image_id:
        logger.warning(f"Upload event missing image_id: {event_data}")
        return

    # Extract metadata
    category = event_data.get('category', 'unknown')
    caption = event_data.get('caption', '')
    caption_length = len(caption) if caption else 0
    uploaded_at = event_data.get('created_at', event_data.get('uploaded_at', datetime.utcnow().isoformat()))

    # Store metadata in Redis hash
    metadata = {
        'category': category,
        'caption_length': str(caption_length),
        'uploaded_at': uploaded_at,
        'user_id': event_data.get('user_id', '')
    }

    redis_client.hset(f"image:{image_id}:metadata", metadata)

    logger.info(
        f"Upload: {image_id[:8]}... | "
        f"Category:{category} CaptionLen:{caption_length}"
    )

# Process a flag event - increment flag counter
def process_flag_event(event_data: Dict[str, Any]):
    image_id = event_data.get('image_id')
    if not image_id:
        # Flag might be on a comment, not an image
        return

    timestamp_str = event_data.get('created_at')
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp()
    else:
        timestamp = time.time()

    value = f"flag:{timestamp}:{time.time_ns() % 1000000}"

    redis_client.zadd_with_ttl(
        f"image:{image_id}:flags:1min",
        score=timestamp,
        value=value,
        ttl=config.window_1min
    )
    redis_client.zadd_with_ttl(
        f"image:{image_id}:flags:5min",
        score=timestamp,
        value=value,
        ttl=config.window_5min
    )
    redis_client.zadd_with_ttl(
        f"image:{image_id}:flags:1hr",
        score=timestamp,
        value=value,
        ttl=config.window_1hr
    )

    total_flags = redis_client.incr(f"image:{image_id}:total_flags")
    _cleanup_expired_entries(image_id, timestamp, "flags")

    flags_5min = _get_window_count(image_id, timestamp, config.window_5min, "flags")
    flags_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "flags")

    logger.info(
        f"Flag: {image_id[:8]}... | "
        f"5m:{flags_5min} 1h:{flags_1hr} Total:{total_flags} | "
        f"Reason:{event_data.get('reason', 'N/A')}"
    )

# Retrieve all aggregated features for an image from Redis
def get_image_features(image_id: str) -> Dict[str, Any]:
    timestamp = time.time()

    # Get window counts for views (no 1-min for train-test parity)
    views_5min = _get_window_count(image_id, timestamp, config.window_5min, "views")
    views_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "views")

    # Get window counts for comments (no 1-min for train-test parity)
    comments_5min = _get_window_count(image_id, timestamp, config.window_5min, "comments")
    comments_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "comments")

    # Get window counts for flags
    flags_5min = _get_window_count(image_id, timestamp, config.window_5min, "flags")
    flags_1hr = _get_window_count(image_id, timestamp, config.window_1hr, "flags")

    # Get totals
    total_views = int(redis_client.get(f"image:{image_id}:total_views") or 0)
    total_comments = int(redis_client.get(f"image:{image_id}:total_comments") or 0)
    total_flags = int(redis_client.get(f"image:{image_id}:total_flags") or 0)

    # Get metadata
    metadata = redis_client.hgetall(f"image:{image_id}:metadata")
    caption_length = int(metadata.get('caption_length', 0))
    uploaded_at_str = metadata.get('uploaded_at', '')

    # Compute derived features (using 5-min windows instead of 1-min)
    view_velocity = views_5min / 5.0 if views_5min > 0 else 0.0
    comment_ratio = total_comments / max(total_views, 1)
    recent_engagement = views_5min + (comments_5min * 5)  # Weight comments 5x

    # Compute temporal features
    has_caption = 1 if caption_length > 0 else 0

    # Compute time_since_upload_seconds
    time_since_upload_seconds = 0
    if uploaded_at_str:
        try:
            uploaded_at = datetime.fromisoformat(uploaded_at_str.replace('Z', '+00:00'))
            time_since_upload_seconds = int(timestamp - uploaded_at.timestamp())
        except Exception:
            pass

    return {
        # Window features (4 total, no 1-min)
        'views_5min': views_5min,
        'views_1hr': views_1hr,
        'comments_5min': comments_5min,
        'comments_1hr': comments_1hr,
        'flags_5min': flags_5min,
        'flags_1hr': flags_1hr,

        # Totals
        'total_views': total_views,
        'total_comments': total_comments,
        'total_flags': total_flags,

        # Derived features
        'view_velocity': view_velocity,
        'comment_ratio': comment_ratio,
        'recent_engagement': recent_engagement,

        # Content features
        'caption_length': caption_length,
        'has_caption': has_caption,

        # Temporal features
        'time_since_upload_seconds': time_since_upload_seconds,
        'uploaded_at': uploaded_at_str,

        # Metadata
        'category': metadata.get('category', 'unknown'),
        'user_id': metadata.get('user_id', '')
    }

# Helper to get count of events within a time window
def _get_window_count(image_id: str, current_timestamp: float, window_size: int, metric_type: str) -> int:
    window_start = current_timestamp - window_size
    key = f"image:{image_id}:{metric_type}:{window_size}s"

    # For 1min, 5min, 1hr use seconds suffix
    if window_size == 60:
        key = f"image:{image_id}:{metric_type}:1min"
    elif window_size == 300:
        key = f"image:{image_id}:{metric_type}:5min"
    elif window_size == 3600:
        key = f"image:{image_id}:{metric_type}:1hr"

    return redis_client.zcount(key, window_start, current_timestamp)

# Remove entries older than the window from sorted sets
def _cleanup_expired_entries(image_id: str, current_timestamp: float, metric_type: str):
    # Clean 1min window
    threshold_1min = current_timestamp - config.window_1min
    redis_client.zremrangebyscore(
        f"image:{image_id}:{metric_type}:1min",
        float('-inf'),
        threshold_1min
    )

    # Clean 5min window
    threshold_5min = current_timestamp - config.window_5min
    redis_client.zremrangebyscore(
        f"image:{image_id}:{metric_type}:5min",
        float('-inf'),
        threshold_5min
    )

    # Clean 1hr window
    threshold_1hr = current_timestamp - config.window_1hr
    redis_client.zremrangebyscore(
        f"image:{image_id}:{metric_type}:1hr",
        float('-inf'),
        threshold_1hr
    )
