CREATE TABLE IF NOT EXISTS user_profiles (
    sub               VARCHAR(255) PRIMARY KEY,
    username          VARCHAR(255),
    email             VARCHAR(255),
    first_name        VARCHAR(255),
    last_name         VARCHAR(255),
    identity_provider VARCHAR(64),
    raw_profile       JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_email ON user_profiles (email);
