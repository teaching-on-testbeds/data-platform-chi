#!/usr/bin/env python3
import logging
from typing import Dict, Any, List, Optional
import numpy as np

from config import config

logger = logging.getLogger(__name__)

# Simple rule-based predictor using domain knowledge.
class HeuristicModerationPredictor:

    def __init__(self, threshold: Optional[float] = None):
        self.threshold = threshold or config.moderation_threshold
        logger.info(f"HeuristicPredictor initialized (threshold: {self.threshold})")

    def predict(self, feature_vector: np.ndarray, feature_names: List[str]) -> Dict[str, Any]:
        # Convert feature vector to dict for easier access
        features = {name: float(value) for name, value in zip(feature_names, feature_vector)}

        # Initialize score
        base_score = 0.3
        score = base_score
        reasoning = []
        risk_factors = {'base': base_score}

        # === HIGH RISK SIGNALS ===

        # Flags reported
        if features.get('total_flags', 0) > 0:
            contribution = 0.4
            score += contribution
            risk_factors['flags_reported'] = contribution
            reasoning.append(f"[HIGH RISK] Content has been flagged {int(features['total_flags'])} time(s)")

        # Suspicious comment spam (5-minute window)
        comments_5min = features.get('comments_5min', 0)
        if comments_5min > 10:
            contribution = 0.3
            score += contribution
            risk_factors['comment_spam'] = contribution
            reasoning.append(f"[HIGH RISK] Unusual comment activity ({int(comments_5min)} comments in 5 minutes)")

        # Unusual engagement pattern
        comment_ratio = features.get('comment_to_view_ratio', 0)
        if comment_ratio > 0.5:
            contribution = 0.2
            score += contribution
            risk_factors['unusual_engagement'] = contribution
            reasoning.append(f"[HIGH RISK] High comment-to-view ratio ({comment_ratio:.2f})")

        # Bot-like behavior
        total_comments = features.get('comments_1hr', 0)
        total_views = features.get('views_1hr', 0)
        if total_comments > 50 and total_views < 100:
            contribution = 0.3
            score += contribution
            risk_factors['bot_pattern'] = contribution
            reasoning.append(f"[HIGH RISK] Suspicious pattern: {int(total_comments)} comments but only {int(total_views)} views in 1 hour")

        # === MEDIUM RISK SIGNALS ===

        # Viral content (needs human review)
        view_velocity = features.get('view_velocity_per_min', 0)
        if view_velocity > 20:
            contribution = 0.2
            score += contribution
            risk_factors['viral_content'] = contribution
            reasoning.append(f"[MEDIUM RISK] Viral content ({view_velocity:.1f} views/min) - may need review")

        # High recent engagement
        recent_engagement = features.get('recent_engagement_score', 0)
        if recent_engagement > 100:
            contribution = 0.15
            score += contribution
            risk_factors['high_activity'] = contribution
            reasoning.append(f"[MEDIUM RISK] High recent activity (score: {int(recent_engagement)})")

        # === LOW RISK SIGNALS (reduce score) ===

        # Established user
        user_image_count = features.get('user_image_count', 0)
        if user_image_count > 10:
            contribution = -0.1
            score += contribution
            risk_factors['established_user'] = contribution
            reasoning.append(f"[LOW RISK] Established user ({int(user_image_count)} images)")

        # Low activity, no flags
        if features.get('total_flags', 0) == 0 and total_comments < 5:
            if not reasoning:  # Only if no other signals
                reasoning.append("[LOW RISK] Normal activity, no flags")

        # Clamp score to [0, 1]
        score = max(0.0, min(1.0, score))

        # Add default reasoning if empty
        if not reasoning:
            reasoning.append("No significant risk signals detected")

        result = {
            'moderation_probability': score,
            'reasoning': reasoning,
            'risk_factors': risk_factors
        }

        logger.debug(f"Prediction: {score:.2f} - {len(reasoning)} factors")

        return result

    def explain(self, prediction: Dict[str, Any]) -> str:
        lines = []

        # Header
        prob = prediction['moderation_probability']
        lines.append("=" * 70)
        lines.append("MODERATION PREDICTION")
        lines.append("=" * 70)
        lines.append(f"Probability: {prob:.3f}")
        lines.append("")

        # Reasoning
        lines.append("Reasoning:")
        for reason in prediction['reasoning']:
            lines.append(f"  {reason}")

        lines.append("")

        # Risk factors breakdown
        lines.append("Risk Factor Contributions:")
        risk_factors = prediction['risk_factors']

        for factor, contribution in sorted(risk_factors.items(), key=lambda x: abs(x[1]), reverse=True):
            sign = "+" if contribution >= 0 else ""
            lines.append(f"  {factor:25s}: {sign}{contribution:+.2f}")

        lines.append("")
        lines.append(f"Final Score: {prob:.3f}")
        lines.append("=" * 70)

        return "\n".join(lines)
