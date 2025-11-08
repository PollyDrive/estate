-- Migration: Create listing_non_relevant table for storing filtered out listings
-- Date: 2025-11-08
-- Purpose: Separate non-relevant listings (price > 16M IDR or bedrooms > 2) from main table
-- Updated: Enhanced filtering with price text parsing and bedroom extraction from title

-- Create table for non-relevant listings
CREATE TABLE IF NOT EXISTS listing_non_relevant (
    id SERIAL PRIMARY KEY,
    fb_id TEXT NOT NULL UNIQUE,
    title TEXT,
    price TEXT,
    location TEXT,
    listing_url TEXT NOT NULL,
    description TEXT,
    phone_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_to_telegram BOOLEAN DEFAULT FALSE,
    source TEXT,
    group_id TEXT,
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
    all_images TEXT,
    timestamp TEXT,
    moved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    move_reason TEXT
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_non_relevant_fb_id ON listing_non_relevant(fb_id);
CREATE INDEX IF NOT EXISTS idx_non_relevant_created_at ON listing_non_relevant(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_non_relevant_source ON listing_non_relevant(source);
CREATE INDEX IF NOT EXISTS idx_non_relevant_moved_at ON listing_non_relevant(moved_at DESC);

-- Function to extract price from text (handles "IDR300,000,000" format)
CREATE OR REPLACE FUNCTION extract_price_from_text(price_text TEXT)
RETURNS NUMERIC AS $$
DECLARE
    price_numeric NUMERIC;
BEGIN
    -- Remove 'IDR', spaces and commas, keep only digits
    price_text := REGEXP_REPLACE(price_text, '[^0-9]', '', 'g');
    
    IF price_text = '' OR price_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    price_numeric := price_text::NUMERIC;
    RETURN price_numeric;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to extract bedrooms from title
-- Handles patterns: '2 Bed', '2 BR', '2BR', '2 Bedroom', '2KT' (kamar tidur - Indonesian)
CREATE OR REPLACE FUNCTION extract_bedrooms_from_title(title_text TEXT)
RETURNS INTEGER AS $$
DECLARE
    bedrooms_count INTEGER;
BEGIN
    IF title_text IS NULL OR title_text = '' THEN
        RETURN NULL;
    END IF;
    
    -- Search for patterns with priority
    
    -- '2 Bedrooms', '2 Bedroom'
    bedrooms_count := (REGEXP_MATCH(title_text, '(\d+)\s*Bedroom', 'i'))[1]::INTEGER;
    IF bedrooms_count IS NOT NULL THEN RETURN bedrooms_count; END IF;
    
    -- '2 Beds', '2 Bed' (but not '2 Beds 1 Bath' to avoid confusion)
    bedrooms_count := (REGEXP_MATCH(title_text, '(\d+)\s*Beds?(?!\s*Bath)', 'i'))[1]::INTEGER;
    IF bedrooms_count IS NOT NULL THEN RETURN bedrooms_count; END IF;
    
    -- '2BR', '2 BR' (but not 'BRC' to avoid false matches)
    bedrooms_count := (REGEXP_MATCH(title_text, '(\d+)\s*BR(?!C)', 'i'))[1]::INTEGER;
    IF bedrooms_count IS NOT NULL THEN RETURN bedrooms_count; END IF;
    
    -- '2KT', '2 KT' (Kamar Tidur - Indonesian)
    bedrooms_count := (REGEXP_MATCH(title_text, '(\d+)\s*KT(?!\s*km)', 'i'))[1]::INTEGER;
    IF bedrooms_count IS NOT NULL THEN RETURN bedrooms_count; END IF;
    
    RETURN NULL;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to move non-relevant listings with enhanced filtering
CREATE OR REPLACE FUNCTION move_non_relevant_listings()
RETURNS TABLE(moved_count INTEGER, deleted_count INTEGER) AS $$
DECLARE
    v_moved_count INTEGER;
    v_deleted_count INTEGER;
BEGIN
    -- Move listings with price > 16M IDR (from text or extracted) OR bedrooms > 2 (from title or field)
    INSERT INTO listing_non_relevant (
        fb_id, title, price, location, listing_url, description, 
        phone_number, created_at, sent_to_telegram, source, group_id,
        bedrooms, price_extracted, kitchen_type, has_ac, has_wifi, 
        has_pool, has_parking, utilities, furniture, rental_term, 
        all_images, timestamp, moved_at, move_reason
    )
    SELECT 
        fb_id, title, price, location, listing_url, description,
        phone_number, created_at, sent_to_telegram, source, group_id,
        bedrooms, price_extracted, kitchen_type, has_ac, has_wifi,
        has_pool, has_parking, utilities, furniture, rental_term,
        all_images, timestamp, NOW(),
        CASE 
            WHEN (extract_price_from_text(price) > 16000000 OR COALESCE(price_extracted, 0) > 16000000)
                AND COALESCE(extract_bedrooms_from_title(title), bedrooms, 0) > 2 
            THEN 'Too expensive (' || 
                 COALESCE(ROUND(GREATEST(extract_price_from_text(price), price_extracted)/1000000, 1)::TEXT, 'N/A') || 
                 'M IDR) and too many bedrooms (' || 
                 COALESCE(extract_bedrooms_from_title(title), bedrooms)::TEXT || ')'
            WHEN extract_price_from_text(price) > 16000000 OR COALESCE(price_extracted, 0) > 16000000 
            THEN 'Too expensive: ' || 
                 COALESCE(ROUND(GREATEST(extract_price_from_text(price), price_extracted)/1000000, 1)::TEXT, 'N/A') || 
                 'M IDR (max 16M)'
            WHEN COALESCE(extract_bedrooms_from_title(title), bedrooms, 0) > 2 
            THEN 'Too many bedrooms: ' || 
                 COALESCE(extract_bedrooms_from_title(title), bedrooms)::TEXT || 
                 ' (max 2)'
            ELSE 'Unknown reason'
        END as move_reason
    FROM fb_listings
    WHERE 
        extract_price_from_text(price) > 16000000 
        OR COALESCE(price_extracted, 0) > 16000000
        OR COALESCE(extract_bedrooms_from_title(title), bedrooms, 0) > 2
    ON CONFLICT (fb_id) DO UPDATE SET
        move_reason = EXCLUDED.move_reason,
        moved_at = NOW();
    
    GET DIAGNOSTICS v_moved_count = ROW_COUNT;
    
    -- Delete from main table
    DELETE FROM fb_listings
    WHERE fb_id IN (
        SELECT fb_id FROM listing_non_relevant
    );
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_moved_count, v_deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON TABLE listing_non_relevant IS 'Stores listings that do not meet criteria: price > 16M IDR (from text or numeric field) or bedrooms > 2 (from title or field)';
COMMENT ON COLUMN listing_non_relevant.moved_at IS 'Timestamp when listing was moved from fb_listings';
COMMENT ON COLUMN listing_non_relevant.move_reason IS 'Reason why listing was moved (too expensive, too many bedrooms, etc.)';
COMMENT ON FUNCTION extract_price_from_text(TEXT) IS 'Extracts numeric price from text format like "IDR300,000,000"';
COMMENT ON FUNCTION extract_bedrooms_from_title(TEXT) IS 'Extracts bedroom count from title using patterns: 2BR, 2 Beds, 2KT, etc.';
COMMENT ON FUNCTION move_non_relevant_listings() IS 'Moves non-relevant listings from fb_listings to listing_non_relevant and deletes them from main table';
