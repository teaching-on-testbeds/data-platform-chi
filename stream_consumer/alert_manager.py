#!/usr/bin/env python3
import json
import logging
import time
from typing import Optional
from collections import OrderedDict

from kafka import KafkaProducer
from config import config

logger = logging.getLogger(__name__)

# Kafka producer for publishing moderation requests
_producer = None


def init_producer():
    """Initialize the Kafka producer for moderation requests.
    Called from main.py after connections are established."""
    global _producer
    try:
        _producer = KafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Moderation producer initialized (topic: {config.moderation_topic})")
    except Exception as e:
        logger.error(f"Failed to initialize moderation producer: {e}")


def _publish_moderation_request(image_id: str, trigger_type: str, context: dict):
    """Publish a moderation request to Kafka for the risk scoring service."""
    if _producer is None:
        return
    msg = {
        "image_id": image_id,
        "trigger": trigger_type,
        "context": context,
        "timestamp": time.time()
    }
    _producer.send(config.moderation_topic, value=msg)
    logger.info(f"Published moderation request for {image_id[:8]}... (trigger: {trigger_type})")

# Maximum number of images to track for each alert type (LRU eviction)
MAX_ALERT_TRACKING = 1000

# Track which images have already triggered alerts to avoid spam
# Using OrderedDict for LRU eviction
_alerted_images = {
    'viral': OrderedDict(),        # image_id -> True
    'suspicious': OrderedDict(),   # image_id -> True
    'popular': OrderedDict(),      # image_id -> True
    'milestones': OrderedDict()    # image_id -> set of milestones reached
}


def _add_to_alert_tracking(alert_type: str, image_id: str):
    _alerted_images[alert_type][image_id] = True
    _alerted_images[alert_type].move_to_end(image_id)

    # Evict oldest if over limit
    if len(_alerted_images[alert_type]) > MAX_ALERT_TRACKING:
        oldest_key = next(iter(_alerted_images[alert_type]))
        del _alerted_images[alert_type][oldest_key]
        logger.debug(f"LRU eviction: removed {oldest_key[:8]}... from {alert_type} tracking")


def check_and_alert(
    image_id: str,
    views_1min: int,
    views_5min: int,
    views_1hr: int,
    comments_1min: int,
    total_views: int
):

    # 1. Check for viral content (high views in short time)
    if views_5min >= config.viral_threshold_views_5min:
        if image_id not in _alerted_images['viral']:
            logger.warning(
                f"VIRAL CONTENT ALERT! Image {image_id[:8]}... "
                f"received {views_5min} views in 5 minutes "
                f"(threshold: {config.viral_threshold_views_5min})"
            )
            _add_to_alert_tracking('viral', image_id)
            _publish_moderation_request(image_id, "viral", {"views_5min": views_5min})

    # 2. Check for suspicious activity (comment spam)
    if comments_1min >= config.suspicious_threshold_comments_1min:
        if image_id not in _alerted_images['suspicious']:
            logger.warning(
                f"SUSPICIOUS ACTIVITY ALERT! Image {image_id[:8]}... "
                f"received {comments_1min} comments in 1 minute "
                f"(threshold: {config.suspicious_threshold_comments_1min})"
            )
            _add_to_alert_tracking('suspicious', image_id)
            _publish_moderation_request(image_id, "suspicious", {"comments_1min": comments_1min})

    # 3. Check for popular post
    if views_1hr >= config.popular_threshold_views_1hr:
        if image_id not in _alerted_images['popular']:
            logger.info(
                f"POPULAR POST! Image {image_id[:8]}... "
                f"received {views_1hr} views in 1 hour "
                f"(threshold: {config.popular_threshold_views_1hr})"
            )
            _add_to_alert_tracking('popular', image_id)

    # 4. Check for view milestones
    _check_milestones(image_id, total_views)


def _check_milestones(image_id: str, total_views: int):
    # Initialize milestone tracking for this image if needed
    if image_id not in _alerted_images['milestones']:
        _alerted_images['milestones'][image_id] = set()

    reached_milestones = _alerted_images['milestones'][image_id]

    for milestone in config.view_milestones:
        if total_views >= milestone and milestone not in reached_milestones:
            logger.info(
                f"MILESTONE REACHED! Image {image_id[:8]}... "
                f"hit {milestone:,} total views"
            )
            reached_milestones.add(milestone)

            # Move to end for LRU tracking
            _alerted_images['milestones'].move_to_end(image_id)

    # Evict oldest milestone tracking if over limit
    if len(_alerted_images['milestones']) > MAX_ALERT_TRACKING:
        oldest_key = next(iter(_alerted_images['milestones']))
        del _alerted_images['milestones'][oldest_key]
        logger.debug(f"LRU eviction: removed {oldest_key[:8]}... from milestone tracking")


def get_alert_stats() -> dict:
    return {
        'viral_alerts': len(_alerted_images['viral']),
        'suspicious_alerts': len(_alerted_images['suspicious']),
        'popular_alerts': len(_alerted_images['popular']),
        'milestone_alerts': sum(len(milestones) for milestones in _alerted_images['milestones'].values())
    }


def reset_alerts():
    global _alerted_images
    _alerted_images = {
        'viral': OrderedDict(),
        'suspicious': OrderedDict(),
        'popular': OrderedDict(),
        'milestones': OrderedDict()
    }
    logger.info("Alert tracking reset")
