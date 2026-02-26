-- Migration: Multi-profile support
-- Adds chat_profiles, listing_profiles tables
-- Extends feedback and batch_runs with chat_id
-- Apply once: docker-compose exec bot python3 -c "..."

SET search_path TO "_4BR", public;

-- ── 1. Chat profiles: one row per Telegram chat / search profile ──────────────
CREATE TABLE IF NOT EXISTS chat_profiles (
    chat_id     BIGINT       PRIMARY KEY,
    name        VARCHAR(255),
    enabled     BOOLEAN      NOT NULL DEFAULT TRUE,
    bedrooms_min INTEGER     NOT NULL DEFAULT 1,
    bedrooms_max INTEGER,                          -- NULL = no upper limit
    price_max   BIGINT       NOT NULL,
    allowed_locations JSONB  NOT NULL DEFAULT '[]',
    stop_locations    JSONB  NOT NULL DEFAULT '[]',
    qfr_start_urls    JSONB  NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── 2. Listing × profile results ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS listing_profiles (
    id          SERIAL       PRIMARY KEY,
    fb_id       TEXT         NOT NULL REFERENCES listings(fb_id) ON DELETE CASCADE,
    chat_id     BIGINT       NOT NULL REFERENCES chat_profiles(chat_id),
    passed      BOOLEAN      NOT NULL,
    reason      TEXT,
    telegram_message_id BIGINT,
    sent_at     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (fb_id, chat_id)
);

CREATE INDEX IF NOT EXISTS idx_lp_pending
    ON listing_profiles (chat_id, created_at)
    WHERE passed = TRUE AND sent_at IS NULL;

-- ── 3. Extend feedback with chat_id ──────────────────────────────────────────
ALTER TABLE feedback
    ADD COLUMN IF NOT EXISTS chat_id BIGINT REFERENCES chat_profiles(chat_id);

CREATE INDEX IF NOT EXISTS idx_feedback_chat_id
    ON feedback (chat_id);

-- ── 4. Extend batch_runs with chat_id ────────────────────────────────────────
ALTER TABLE batch_runs
    ADD COLUMN IF NOT EXISTS chat_id BIGINT REFERENCES chat_profiles(chat_id);

CREATE INDEX IF NOT EXISTS idx_batch_runs_chat_id
    ON batch_runs (chat_id);
