#!/usr/bin/env python3
"""
Run Apify scraper and save results to database with stage1_new status.
"""

import os
import sys
import logging
import json
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from apify_scraper import ApifyScraper
from database import Database, STATUS_STAGE1_NEW

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/apify_scrape.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Run Apify scraper and save to database."""
    
    logger.info("=" * 80)
    logger.info("APIFY SCRAPER: Fetching listings from Facebook Marketplace")
    logger.info("=" * 80)
    
    load_dotenv()
    
    # Load config
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get API key
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        logger.error("Missing APIFY_API_KEY environment variable!")
        sys.exit(1)
    
    # Check if Apify is enabled
    if not config['apify'].get('enabled', False):
        logger.warning("Apify scraping is DISABLED in config. Enable it in config.json")
        sys.exit(0)
    
    # Initialize scraper
    scraper = ApifyScraper(api_key, config)
    
    # Run scrape
    logger.info("Starting Apify scrape...")
    max_listings = config['apify'].get('max_listings', 100)
    
    try:
        listings = scraper.scrape_titles_only(max_items=max_listings)
        logger.info(f"Scraped {len(listings)} listings from Apify")
        
        if not listings:
            logger.warning("No listings returned from Apify!")
            sys.exit(0)
        
        # Save to database
        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        with Database() as db:
            for listing in listings:
                fb_id = listing.get('fb_id')
                
                if not fb_id:
                    logger.warning(f"Skipping listing without fb_id: {listing.get('title', 'N/A')}")
                    skipped_count += 1
                    continue
                
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
                        'apify',
                        'From Apify scraper'
                    ))
                    db.conn.commit()
                    
                    saved_count += 1
                    logger.info(f"✓ Saved: {fb_id} - {listing.get('title', 'N/A')[:60]}")
                    
                except Exception as e:
                    logger.error(f"Error saving listing {fb_id}: {e}")
                    error_count += 1
                    db.conn.rollback()
        
        logger.info("=" * 80)
        logger.info("APIFY SCRAPE COMPLETE")
        logger.info(f"Total scraped: {len(listings)}")
        logger.info(f"Saved to DB: {saved_count}")
        logger.info(f"Skipped (duplicates): {skipped_count}")
        logger.info(f"Errors: {error_count}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error during Apify scrape: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
