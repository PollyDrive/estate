-- Database initialization script for RealtyBot-Bali
-- This script creates the necessary table for storing Facebook Marketplace listings

CREATE TABLE IF NOT EXISTS fb_listings (
    id SERIAL PRIMARY KEY,
    fb_id TEXT NOT NULL UNIQUE,
    title TEXT,
    price TEXT,
    location TEXT,
    listing_url TEXT NOT NULL,
    description TEXT,
    phone_number TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_to_telegram BOOLEAN DEFAULT FALSE
);

-- Create index on fb_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_fb_id ON fb_listings(fb_id);

-- Create index on sent_to_telegram for filtering
CREATE INDEX IF NOT EXISTS idx_sent_to_telegram ON fb_listings(sent_to_telegram);

-- Create index on created_at for sorting
CREATE INDEX IF NOT EXISTS idx_created_at ON fb_listings(created_at DESC);
