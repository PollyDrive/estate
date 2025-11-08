-- Migration V2: Add source field and parsed parameters
-- This migration extends the existing schema without dropping old data

-- Add new columns to existing table
ALTER TABLE fb_listings 
ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'marketplace',
ADD COLUMN IF NOT EXISTS group_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS bedrooms INTEGER,
ADD COLUMN IF NOT EXISTS price_extracted NUMERIC,
ADD COLUMN IF NOT EXISTS kitchen_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS has_ac BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS has_wifi BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS has_pool BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS has_parking BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS utilities VARCHAR(20),
ADD COLUMN IF NOT EXISTS furniture VARCHAR(20),
ADD COLUMN IF NOT EXISTS rental_term VARCHAR(20),
ADD COLUMN IF NOT EXISTS all_images TEXT,
ADD COLUMN IF NOT EXISTS timestamp TEXT;

-- Create index on source for filtering
CREATE INDEX IF NOT EXISTS idx_source ON fb_listings(source);

-- Create index on group_id for group-based queries
CREATE INDEX IF NOT EXISTS idx_group_id ON fb_listings(group_id);

-- Create index on bedrooms for filtering
CREATE INDEX IF NOT EXISTS idx_bedrooms ON fb_listings(bedrooms);

-- Create index on kitchen_type for filtering
CREATE INDEX IF NOT EXISTS idx_kitchen_type ON fb_listings(kitchen_type);

-- Update existing records to have source='marketplace'
UPDATE fb_listings SET source = 'marketplace' WHERE source IS NULL;

-- Helpful comment
COMMENT ON COLUMN fb_listings.source IS 'Source of listing: marketplace or group';
COMMENT ON COLUMN fb_listings.bedrooms IS 'Number of bedrooms (extracted from description)';
COMMENT ON COLUMN fb_listings.kitchen_type IS 'Type of kitchen: enclosed, outdoor, shared, kitchenette, none, unknown';
COMMENT ON COLUMN fb_listings.rental_term IS 'Rental term: monthly, yearly, daily, weekly';
