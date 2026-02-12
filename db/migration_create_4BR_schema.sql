-- Create dedicated schema/tables for 4BR run.
-- Usage:
--   psql "$DATABASE_URL" -f db/migration_create_4BR_schema.sql

CREATE SCHEMA IF NOT EXISTS "_4BR";

-- Main listings table for this run.
CREATE TABLE IF NOT EXISTS "_4BR".listings (
    LIKE public.listings INCLUDING ALL
);

-- Non-relevant listings table used by scripts.
CREATE TABLE IF NOT EXISTS "_4BR".listing_non_relevant (
    LIKE public.listing_non_relevant INCLUDING ALL
);

-- Optional plural alias for convenience/consistency in queries.
DROP VIEW IF EXISTS "_4BR".listings_non_relevant;
CREATE VIEW "_4BR".listings_non_relevant AS
SELECT * FROM "_4BR".listing_non_relevant;
