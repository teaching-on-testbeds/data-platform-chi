CREATE TABLE IF NOT EXISTS moderation (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    inference_request_id UUID,
    target_type VARCHAR(20) NOT NULL,
    target_id UUID NOT NULL,
    inference_mode VARCHAR(20) NOT NULL,
    model_version VARCHAR(64),
    risk_score FLOAT NOT NULL,
    trigger_type VARCHAR(30),
    policy_version VARCHAR(20) NOT NULL DEFAULT 'v1',
    rationale TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CHECK (target_type IN ('image', 'comment', 'account')),
    CHECK (risk_score >= 0 AND risk_score <= 1)
);

CREATE INDEX IF NOT EXISTS idx_moderation_target ON moderation(target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_moderation_risk_score ON moderation(risk_score);
CREATE INDEX IF NOT EXISTS idx_moderation_inference_request ON moderation(inference_request_id);

ALTER TABLE comments
    ADD CONSTRAINT fk_comments_moderation_action
    FOREIGN KEY (moderator_action_id) REFERENCES moderation(id);

ALTER TABLE flags
    ADD CONSTRAINT fk_flags_moderation_action
    FOREIGN KEY (moderation_action_id) REFERENCES moderation(id);
