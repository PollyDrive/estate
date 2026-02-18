-- Migration: Feedback Cycle System
-- Adds support for tracking Telegram reactions and batch runs
-- Usage: psql "$DATABASE_URL" -f db/migration_feedback_cycle.sql

-- Apply to _4BR schema
SET search_path TO "_4BR", public;

-- 1. Add telegram_message_id and llm_model to listings table
ALTER TABLE listings
ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT,
ADD COLUMN IF NOT EXISTS llm_model VARCHAR(100);

-- 2. Create feedback table for storing reactions
CREATE TABLE IF NOT EXISTS feedback (
    id SERIAL PRIMARY KEY,
    telegram_message_id BIGINT NOT NULL,
    fb_id TEXT NOT NULL,
    reaction_type VARCHAR(10) NOT NULL,  -- '❤️', '💩', '🤡'
    reaction_count INT DEFAULT 1,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    FOREIGN KEY (fb_id) REFERENCES listings(fb_id) ON DELETE CASCADE,
    UNIQUE(telegram_message_id, reaction_type)
);

-- 3. Create batch_runs table for tracking Stage5 batches
CREATE TABLE IF NOT EXISTS batch_runs (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL DEFAULT CURRENT_DATE,
    batch_number INT NOT NULL,  -- номер батча в этот день (1, 2, 3...)
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    listings_sent INT DEFAULT 0,
    no_desc_sent INT DEFAULT 0,
    blocked_count INT DEFAULT 0,
    error_count INT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running',  -- 'running', 'completed', 'failed'

    UNIQUE(batch_date, batch_number)
);

-- 4. Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_feedback_message_id ON feedback(telegram_message_id);
CREATE INDEX IF NOT EXISTS idx_feedback_fb_id ON feedback(fb_id);
CREATE INDEX IF NOT EXISTS idx_feedback_reaction_type ON feedback(reaction_type);
CREATE INDEX IF NOT EXISTS idx_batch_runs_date ON batch_runs(batch_date);
CREATE INDEX IF NOT EXISTS idx_batch_runs_status ON batch_runs(status);
CREATE INDEX IF NOT EXISTS idx_listings_telegram_message_id ON listings(telegram_message_id);

-- 5. Add comments for documentation
COMMENT ON TABLE feedback IS 'Stores Telegram reactions (❤️, 💩, 🤡) from users on sent listings';
COMMENT ON TABLE batch_runs IS 'Tracks Stage5 batch runs for daily statistics and feedback cycle';
COMMENT ON COLUMN feedback.reaction_type IS 'Emoji reaction: ❤️ (good), 💩 (bad), 🤡 (needs AI fix)';
COMMENT ON COLUMN batch_runs.batch_number IS 'Sequential batch number within the day (resets at midnight)';

-- 6. Verification queries
DO $$
BEGIN
    RAISE NOTICE 'Migration completed successfully!';
    RAISE NOTICE 'Tables created/updated: listings, feedback, batch_runs';
    RAISE NOTICE 'New columns in listings: telegram_message_id, llm_model';
END $$;
