-- Migration: Create final_listings table for Groq-approved listings ready for Telegram
-- This table stores only listings that passed Groq LLM analysis

CREATE TABLE IF NOT EXISTS final_listings (
    id SERIAL PRIMARY KEY,
    fb_id TEXT UNIQUE NOT NULL,
    
    -- Original data from fb_listings
    title TEXT,
    price TEXT,
    location TEXT,
    listing_url TEXT NOT NULL,
    description TEXT,
    phone_number TEXT,
    
    -- Parsed parameters
    bedrooms INTEGER,
    price_extracted NUMERIC,
    kitchen_type TEXT,
    has_ac BOOLEAN DEFAULT FALSE,
    has_wifi BOOLEAN DEFAULT FALSE,
    has_pool BOOLEAN DEFAULT FALSE,
    has_parking BOOLEAN DEFAULT FALSE,
    utilities TEXT,
    furniture TEXT,
    rental_term TEXT,
    
    -- Groq analysis results
    groq_passed BOOLEAN DEFAULT FALSE,
    groq_reason TEXT,
    groq_analyzed_at TIMESTAMP,
    
    -- Claude analysis results (optional, for future)
    claude_summary_ru TEXT,
    claude_analyzed_at TIMESTAMP,
    
    -- Telegram status
    sent_to_telegram BOOLEAN DEFAULT FALSE,
    telegram_sent_at TIMESTAMP,
    
    -- Metadata
    source TEXT DEFAULT 'marketplace',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_final_fb_id ON final_listings(fb_id);
CREATE INDEX IF NOT EXISTS idx_final_sent ON final_listings(sent_to_telegram);
CREATE INDEX IF NOT EXISTS idx_final_groq_passed ON final_listings(groq_passed);
CREATE INDEX IF NOT EXISTS idx_final_created_at ON final_listings(created_at DESC);

-- Comments
COMMENT ON TABLE final_listings IS 'Final listings that passed Groq analysis and are ready for Telegram';
COMMENT ON COLUMN final_listings.groq_passed IS 'Whether listing passed Groq LLM kitchen/AC/WiFi check';
COMMENT ON COLUMN final_listings.groq_reason IS 'Groq analysis result reason';
COMMENT ON COLUMN final_listings.claude_summary_ru IS 'Russian summary from Claude (optional)';
