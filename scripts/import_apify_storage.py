#!/usr/bin/env python3
"""
Import data from Apify storage (existing dataset) to database.
Does NOT run new scraping, only fetches existing data.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from apify_client import ApifyClient
from database import Database, STATUS_STAGE1_NEW
from apify_scraper import ApifyScraper
import json

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/import_apify_storage.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Import data from Apify storage."""
    
    logger.info("=" * 80)
    logger.info("APIFY STORAGE IMPORT: Fetching existing data from Apify")
    logger.info("=" * 80)
    
    load_dotenv()
    
    # Load config for normalization
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get API key
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        logger.error("Missing APIFY_API_KEY environment variable!")
        sys.exit(1)
    
    client = ApifyClient(api_key)
    
    # Use the latest successful run
    run_id = 'rghWKhi5eK8OoBUNQ'
    dataset_id = 'da5NEzcgis8cmgRwh'
    
    logger.info(f"Fetching data from run {run_id}")
    logger.info(f"Dataset ID: {dataset_id}")
    
    try:
        # Fetch items from dataset
        items = list(client.dataset(dataset_id).iterate_items())
        logger.info(f"Fetched {len(items)} items from Apify storage")
        
        if not items:
            logger.warning("No items found in dataset!")
            sys.exit(0)
        
        # Filter out errors
        valid_items = [item for item in items if 'error' not in item]
        error_items = [item for item in items if 'error' in item]
        
        logger.info(f"Valid items: {len(valid_items)}")
        logger.info(f"Error items: {len(error_items)}")
        
        if not valid_items:
            logger.warning("No valid items to import!")
            sys.exit(0)
        
        # Initialize scraper for normalization
        scraper = ApifyScraper(api_key, config)
        
        # Normalize listings
        normalized = []
        for item in valid_items:
            listing = scraper.normalize_listing(item)
            if listing and listing.get('fb_id'):
                normalized.append(listing)
        
        logger.info(f"Normalized {len(normalized)} listings")
        
        # Save to database
        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        with Database() as db:
            for listing in normalized:
                fb_id = listing.get('fb_id')
                
                try:
                    # Check if listing already exists
                    db.cursor.execute(
                        "SELECT fb_id FROM listings WHERE fb_id = %s",
                        (fb_id,)
                    )
                    existing = db.cursor.fetchone()
                    
                    if existing:
                        logger.debug(f"Listing {fb_id} already exists, skipping")
                        skipped_count += 1
                        continue
                    
                    # Insert new listing
                    db.cursor.execute("""
                        INSERT INTO listings (
                            fb_id, title, price, location, listing_url, 
                            description, status, source, pass_reason
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        fb_id,
                        listing.get('title', ''),
                        listing.get('price', ''),
                        listing.get('location', ''),
                        listing.get('listing_url', ''),
                        listing.get('description', ''),
                        STATUS_STAGE1_NEW,
                        'apify-marketplace',
                        f'From Apify storage (run: {run_id})'
                    ))
                    db.conn.commit()
                    
                    saved_count += 1
                    logger.info(f"✓ Saved: {fb_id} - {listing.get('title', 'N/A')[:60]}")
                    
                except Exception as e:
                    logger.error(f"Error saving listing {fb_id}: {e}")
                    error_count += 1
                    db.conn.rollback()
        
        logger.info("=" * 80)
        logger.info("APIFY STORAGE IMPORT COMPLETE")
        logger.info(f"Total items in storage: {len(items)}")
        logger.info(f"Valid items: {len(valid_items)}")
        logger.info(f"Normalized: {len(normalized)}")
        logger.info(f"Saved to DB: {saved_count}")
        logger.info(f"Skipped (duplicates): {skipped_count}")
        logger.info(f"Errors: {error_count}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error fetching from Apify storage: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
