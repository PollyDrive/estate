-- Migration: Add location_extracted and priority columns
-- Date: 2025-11-12
-- Description: Add extracted location and priority for sorting

BEGIN;

-- 1. Add location_extracted column to store parsed location from description
ALTER TABLE listings 
  ADD COLUMN IF NOT EXISTS location_extracted VARCHAR(100);

-- 2. Add priority column for sorting by location preference
-- Higher priority = more preferred location
-- NULL = not yet calculated
ALTER TABLE listings 
  ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT NULL;

-- 3. Create index for location-based queries
CREATE INDEX IF NOT EXISTS idx_listings_location_extracted 
  ON listings(location_extracted);

-- 4. Create index for priority sorting
CREATE INDEX IF NOT EXISTS idx_listings_priority 
  ON listings(priority DESC);

-- 5. Add comments
COMMENT ON COLUMN listings.location_extracted IS 'Extracted location from description (Ubud, Canggu, etc) - filled in Stage 2';
COMMENT ON COLUMN listings.priority IS 'Priority score based on location match with allowed_locations (higher = better)';

COMMIT;
