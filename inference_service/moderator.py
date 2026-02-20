#!/usr/bin/env python3
"""Moderator - Orchestrates heuristic moderation inference."""
import logging
import time
from typing import Dict, Any

from config import config
from feature_fetcher import FeatureFetcher
from feature_constructor import FeatureConstructor
from heuristic_predictor import HeuristicModerationPredictor

logger = logging.getLogger(__name__)

class Moderator:

    def __init__(self):
        self.fetcher = FeatureFetcher()
        self.constructor = FeatureConstructor()
        self.heuristic = HeuristicModerationPredictor()
        logger.info("Moderator initialized | Mode: HEURISTIC")

    def run_inference(self, image_id: str, trigger: str) -> Dict[str, Any]:
        """Run moderation inference for a given image."""
        start = time.time()

        # Step 1: Fetch features from Redis + Postgres
        raw_features = self.fetcher.fetch_all_features(image_id)
        if 'error' in raw_features:
            return {'error': raw_features['error'], 'image_id': image_id}

        # Step 2: Build feature vector
        feature_vector = self.constructor.construct_feature_vector(raw_features)

        # Step 3: Run heuristic inference
        prediction = self.heuristic.predict(
            feature_vector, self.constructor.get_feature_names()
        )
        inference_mode = "heuristic"
        model_version = None

        latency_ms = (time.time() - start) * 1000

        result = {
            'image_id': image_id,
            'inference_mode': inference_mode,
            'model_version': model_version,
            'moderation_probability': prediction['moderation_probability'],
            'reasoning': prediction.get('reasoning', []),
            'trigger': trigger,
            'latency_ms': latency_ms,
        }

        logger.info(
            f"Moderation: image={image_id[:8]}... "
            f"mode={inference_mode} "
            f"prob={prediction['moderation_probability']:.3f} "
            f"latency={latency_ms:.1f}ms"
        )

        return result

    def close(self):
        self.fetcher.close()
