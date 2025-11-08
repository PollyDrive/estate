-- Migration: Create stage1_candidates table for two-stage filtering
-- This table stores candidates from Stage 1 (title-only scraping)

-- Create table for Stage 1 candidates
CREATE TABLE IF NOT EXISTS stage1_candidates (
    id SERIAL PRIMARY KEY,
    fb_id TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    price TEXT,
    location TEXT,
    listing_url TEXT NOT NULL,
    pass_reason TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),
    processed_stage2 BOOLEAN DEFAULT FALSE
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_stage1_fb_id ON stage1_candidates(fb_id);
CREATE INDEX IF NOT EXISTS idx_stage1_processed ON stage1_candidates(processed_stage2);
CREATE INDEX IF NOT EXISTS idx_stage1_scraped_at ON stage1_candidates(scraped_at DESC);

COMMENT ON TABLE stage1_candidates IS 'Candidates from Stage 1 title-only scraping, pending Stage 2 full detail scraping';
COMMENT ON COLUMN stage1_candidates.fb_id IS 'Facebook listing ID';
COMMENT ON COLUMN stage1_candidates.title IS 'Listing title from Stage 1';
COMMENT ON COLUMN stage1_candidates.price IS 'Price text from Stage 1';
COMMENT ON COLUMN stage1_candidates.location IS 'Location text from Stage 1';
COMMENT ON COLUMN stage1_candidates.listing_url IS 'Full URL to listing';
COMMENT ON COLUMN stage1_candidates.pass_reason IS 'Why this candidate passed Stage 1 filters';
COMMENT ON COLUMN stage1_candidates.scraped_at IS 'When Stage 1 scraping happened';
COMMENT ON COLUMN stage1_candidates.processed_stage2 IS 'Whether Stage 2 full detail scraping was completed';
