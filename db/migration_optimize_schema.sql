-- Migration: Optimize database schema
-- Date: 2025-11-12
-- Description: Rename groq_* to llm_* and drop truly unused columns

BEGIN;

-- 1. Rename groq_* columns to llm_* for consistency
ALTER TABLE listings 
  RENAME COLUMN groq_passed TO llm_passed;

ALTER TABLE listings 
  RENAME COLUMN groq_reason TO llm_reason;

ALTER TABLE listings 
  RENAME COLUMN groq_analyzed_at TO llm_analyzed_at;

-- 2. Drop only truly unused columns
-- all_images: stored as text (JSON) but never queried or displayed
-- timestamp: duplicates created_at functionality
ALTER TABLE listings 
  DROP COLUMN IF EXISTS all_images,
  DROP COLUMN IF EXISTS timestamp;

-- 3. Add comments to clarify column usage
COMMENT ON COLUMN listings.bedrooms IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.price_extracted IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.kitchen_type IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.has_ac IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.has_wifi IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.has_pool IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.has_parking IS 'Extracted from description in Stage 2';
COMMENT ON COLUMN listings.utilities IS 'Extracted from description in Stage 2 (included/excluded/not_specified)';
COMMENT ON COLUMN listings.furniture IS 'Extracted from description in Stage 2 (fully_furnished/partially_furnished/unfurnished)';
COMMENT ON COLUMN listings.rental_term IS 'Extracted from description in Stage 2 (monthly/yearly/daily/weekly) - used for filtering';

COMMIT;
