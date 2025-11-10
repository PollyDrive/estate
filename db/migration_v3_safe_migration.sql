-- Migration V3: Safely unify tables and reset all listings for reprocessing.

-- Step 1: Create the new unified 'listings' table (non-destructive)
CREATE TABLE IF NOT EXISTS listings (
    id SERIAL PRIMARY KEY,
    fb_id TEXT NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    title TEXT,
    price TEXT,
    location TEXT,
    listing_url TEXT NOT NULL,
    pass_reason TEXT,
    source VARCHAR(20) DEFAULT 'marketplace',
    group_id VARCHAR(255),
    description TEXT,
    phone_number TEXT,
    bedrooms INTEGER,
    price_extracted NUMERIC,
    kitchen_type VARCHAR(50),
    has_ac BOOLEAN DEFAULT FALSE,
    has_wifi BOOLEAN DEFAULT FALSE,
    has_pool BOOLEAN DEFAULT FALSE,
    has_parking BOOLEAN DEFAULT FALSE,
    utilities VARCHAR(20),
    furniture VARCHAR(20),
    rental_term VARCHAR(20),
    all_images TEXT,
    timestamp TEXT,
    groq_passed BOOLEAN,
    groq_reason TEXT,
    groq_analyzed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    telegram_sent BOOLEAN DEFAULT FALSE,
    telegram_sent_at TIMESTAMP WITH TIME ZONE
);

-- Step 2: Migrate data from old tables, resetting status to 'stage1_new'
INSERT INTO listings (
    fb_id, status, title, price, location, listing_url, pass_reason, source, group_id,
    description, phone_number, bedrooms, price_extracted, kitchen_type, has_ac, has_wifi,
    has_pool, has_parking, utilities, furniture, rental_term, all_images, timestamp,
    groq_passed, groq_reason, groq_analyzed_at, created_at, updated_at, telegram_sent, telegram_sent_at
)
SELECT
    COALESCE(fl.fb_id, fbl.fb_id, s1.fb_id) AS fb_id,
    'stage1_new' AS status, -- Reset all listings to be re-processed
    COALESCE(fl.title, fbl.title, s1.title),
    COALESCE(fl.price, fbl.price, s1.price),
    COALESCE(fl.location, fbl.location, s1.location),
    COALESCE(fl.listing_url, fbl.listing_url, s1.listing_url),
    s1.pass_reason,
    COALESCE(fl.source, fbl.source),
    fbl.group_id,
    COALESCE(fl.description, fbl.description),
    COALESCE(fl.phone_number, fbl.phone_number),
    COALESCE(fl.bedrooms, fbl.bedrooms),
    COALESCE(fl.price_extracted, fbl.price_extracted),
    COALESCE(fl.kitchen_type, fbl.kitchen_type),
    COALESCE(fl.has_ac, fbl.has_ac, FALSE),
    COALESCE(fl.has_wifi, fbl.has_wifi, FALSE),
    COALESCE(fl.has_pool, fbl.has_pool, FALSE),
    COALESCE(fl.has_parking, fbl.has_parking, FALSE),
    COALESCE(fl.utilities, fbl.utilities),
    COALESCE(fl.furniture, fbl.furniture),
    COALESCE(fl.rental_term, fbl.rental_term),
    fbl.all_images,
    fbl.timestamp,
    fl.groq_passed,
    fl.groq_reason,
    fl.groq_analyzed_at,
    COALESCE(fl.created_at, fbl.created_at, s1.scraped_at),
    fl.updated_at,
    COALESCE(fl.sent_to_telegram, fbl.sent_to_telegram, FALSE),
    fl.telegram_sent_at
FROM stage1_candidates_old_backup s1
FULL OUTER JOIN fb_listings_old_backup fbl ON s1.fb_id = fbl.fb_id
FULL OUTER JOIN final_listings_old_backup fl ON COALESCE(s1.fb_id, fbl.fb_id) = fl.fb_id
ON CONFLICT (fb_id) DO NOTHING;

-- Step 3: Rename old tables to keep them as a backup (if they still exist)
ALTER TABLE IF EXISTS stage1_candidates RENAME TO stage1_candidates_old_backup;
ALTER TABLE IF EXISTS fb_listings RENAME TO fb_listings_old_backup;
ALTER TABLE IF EXISTS final_listings RENAME TO final_listings_old_backup;

-- Step 4: Add indexes and triggers to the new table
CREATE INDEX IF NOT EXISTS idx_listings_fb_id ON listings(fb_id);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
-- ... (other indexes and triggers)