#!/usr/bin/env python3

import logging
from typing import Dict, Any, List
from datetime import datetime
import numpy as np

from config import config

logger = logging.getLogger(__name__)


class FeatureConstructor:

    def __init__(self):
        self.categories = config.food_categories
        self.feature_names = config.feature_names
        logger.info(f"FeatureConstructor initialized ({len(self.feature_names)} features)")

    def construct_feature_vector(self, raw_features: Dict[str, Any]) -> np.ndarray:
        features = []

        # === Temporal Features (4) ===
        # 1. time_since_upload_seconds
        features.append(raw_features.get('time_since_upload_seconds', 0))

        # 2. hour_of_day (0-23)
        uploaded_at = raw_features.get('uploaded_at')
        if uploaded_at:
            if isinstance(uploaded_at, str):
                uploaded_at = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
            features.append(uploaded_at.hour)
        else:
            features.append(0)

        # 3. day_of_week (0-6, Monday=0)
        if uploaded_at:
            features.append(uploaded_at.weekday())
        else:
            features.append(0)

        # 4. is_weekend (0 or 1)
        if uploaded_at:
            features.append(1 if uploaded_at.weekday() >= 5 else 0)
        else:
            features.append(0)

        # === Window Aggregates (6) ===
        # 5-6. views_5min, views_1hr
        features.append(raw_features.get('views_5min', 0))
        features.append(raw_features.get('views_1hr', 0))

        # 7-8. comments_5min, comments_1hr
        features.append(raw_features.get('comments_5min', 0))
        features.append(raw_features.get('comments_1hr', 0))

        # 9-10. flags_5min, flags_1hr
        features.append(raw_features.get('flags_5min', 0))
        features.append(raw_features.get('flags_1hr', 0))

        # 11. total_flags
        features.append(raw_features.get('total_flags', 0))

        # === Engagement Ratios (3) ===
        views_5min = raw_features.get('views_5min', 0)
        views_1hr = raw_features.get('views_1hr', 0)
        comments_5min = raw_features.get('comments_5min', 0)
        comments_1hr = raw_features.get('comments_1hr', 0)

        # 12. view_velocity_per_min = views_5min / 5
        features.append(views_5min / 5.0 if views_5min > 0 else 0.0)

        # 13. comment_to_view_ratio = comments_1hr / max(views_1hr, 1)
        features.append(comments_1hr / max(views_1hr, 1))

        # 14. recent_engagement_score = views_5min + (comments_5min * 5)
        features.append(views_5min + (comments_5min * 5))

        # === Content Features (2) ===
        # 15. caption_length
        features.append(raw_features.get('caption_length', 0))

        # 16. has_caption (0 or 1)
        features.append(raw_features.get('has_caption', 0))

        # === User Features (2) ===
        # 17. user_image_count
        features.append(raw_features.get('user_image_count', 0))

        # 18. user_age_days
        features.append(raw_features.get('user_age_days', 0))

        # === Category One-Hot Encoding (11) ===
        category = raw_features.get('category', 'Unknown')

        # Normalize category name for matching
        category_normalized = category.strip()

        for cat in self.categories:
            features.append(1 if cat == category_normalized else 0)

        # Convert to numpy array
        feature_vector = np.array(features, dtype=np.float32)

        # Validate
        if len(feature_vector) != len(self.feature_names):
            logger.error(f"Feature vector has {len(feature_vector)} dimensions, expected {len(self.feature_names)}")
            raise ValueError(f"Feature vector dimension mismatch: {len(feature_vector)} != {len(self.feature_names)}")

        if np.any(np.isnan(feature_vector)):
            logger.warning("Feature vector contains NaN values")
            # Replace NaN with 0
            feature_vector = np.nan_to_num(feature_vector, nan=0.0)

        if np.any(np.isinf(feature_vector)):
            logger.warning("Feature vector contains inf values")
            # Replace inf with large number
            feature_vector = np.nan_to_num(feature_vector, posinf=1e6, neginf=-1e6)

        return feature_vector

    def get_feature_names(self) -> List[str]:
        return self.feature_names.copy()

    def format_feature_summary(self, feature_vector: np.ndarray, compact: bool = False) -> str:
        if len(feature_vector) != len(self.feature_names):
            return f"Error: Feature vector has {len(feature_vector)} dims, expected {len(self.feature_names)}"

        lines = []

        for i, (name, value) in enumerate(zip(self.feature_names, feature_vector)):
            # Skip zero values in compact mode (except for important features)
            if compact and value == 0:
                # Always show these features even if zero
                important_features = ['total_views', 'total_comments', 'total_flags']
                if not any(imp in name for imp in important_features):
                    continue

            # Format value
            if 'category_' in name:
                value_str = "Yes" if value == 1 else "No"
            elif isinstance(value, float):
                value_str = f"{value:.2f}"
            else:
                value_str = f"{int(value)}"

            lines.append(f"  [{i:2d}] {name:30s} = {value_str}")

        return "\n".join(lines)

    def get_feature_dict(self, feature_vector: np.ndarray) -> Dict[str, float]:
        if len(feature_vector) != len(self.feature_names):
            raise ValueError(f"Feature vector dimension mismatch")

        return {
            name: float(value)
            for name, value in zip(self.feature_names, feature_vector)
        }
