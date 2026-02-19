CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR NOT NULL UNIQUE,
    tos_version_accepted INTEGER NOT NULL DEFAULT 1,
    tos_accepted_at TIMESTAMPTZ,
    country_of_residence VARCHAR(2),
    year_of_birth INTEGER,
    is_test_account BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE users
    ADD CONSTRAINT chk_users_tos_version_nonnegative
    CHECK (tos_version_accepted >= 1);

ALTER TABLE users
    ADD CONSTRAINT chk_users_year_of_birth_range
    CHECK (year_of_birth IS NULL OR (year_of_birth >= 1900 AND year_of_birth <= EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER));

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

CREATE TABLE IF NOT EXISTS images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    s3_url VARCHAR NOT NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'user_upload',
    caption TEXT,
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    views INTEGER DEFAULT 0,
    CHECK (source IN ('user_upload', 'synthetic', 'import'))
);

CREATE INDEX IF NOT EXISTS idx_images_user_id ON images(user_id);
CREATE INDEX IF NOT EXISTS idx_images_created_at ON images(created_at);
CREATE INDEX IF NOT EXISTS idx_images_deleted_at ON images(deleted_at);

CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    image_id UUID NOT NULL REFERENCES images(id),
    user_id UUID NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    moderator_hidden BOOLEAN NOT NULL DEFAULT FALSE,
    moderator_action_id UUID,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_comments_image_id ON comments(image_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);
CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at);
CREATE INDEX IF NOT EXISTS idx_comments_deleted_at ON comments(deleted_at);

CREATE TABLE IF NOT EXISTS flags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    image_id UUID REFERENCES images(id),
    comment_id UUID REFERENCES comments(id),
    reason TEXT NOT NULL,
    moderation_action_id UUID,
    review_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CHECK (image_id IS NOT NULL OR comment_id IS NOT NULL),
    CHECK (review_status IN ('pending', 'confirmed', 'rejected'))
);

CREATE INDEX IF NOT EXISTS idx_flags_user_id ON flags(user_id);
CREATE INDEX IF NOT EXISTS idx_flags_image_id ON flags(image_id);
CREATE INDEX IF NOT EXISTS idx_flags_comment_id ON flags(comment_id);
CREATE INDEX IF NOT EXISTS idx_flags_created_at ON flags(created_at);
CREATE INDEX IF NOT EXISTS idx_flags_review_status ON flags(review_status);
