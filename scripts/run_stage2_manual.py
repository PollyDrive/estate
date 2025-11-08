#!/usr/bin/env python3
"""
STAGE 2: Manual full detail scraping with memo23/facebook-marketplace-cheerio
Takes candidates from the main 'listings' table with status 'stage1_new',
scrapes full details, and updates them with the results and a new status.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser
from facebook_marketplace_cheerio_scraper import FacebookMarketplaceCheerioScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage2_manual.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 2: Full detail scraping for candidates"""
    
    logger.info("=" * 80)
    logger.info("STAGE 2: Manual Full Detail Scraping (Refactored)")
    logger.info("=" * 80)
    
    # Load environment
    load_dotenv()
    
    # Load config
    config_path = Path(__file__).parent.parent / 'config' / 'config.json'
    if not config_path.exists():
        config_path = Path('/app/config/config.json')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not all([db_url, apify_key]):
        logger.error("Missing required environment variables (DATABASE_URL, APIFY_API_KEY)!")
        sys.exit(1)
    
    # Initialize components
    parser = PropertyParser()
    cheerio_scraper = FacebookMarketplaceCheerioScraper(apify_key, config)
    
    # Get unprocessed candidates from DB
    with Database(db_url) as db:
        listings_to_process = db.get_listings_for_stage2()
    
    if not listings_to_process:
        logger.warning("No new listings with status 'stage1_new' found.")
        sys.exit(0)
    
    logger.info(f"Found {len(listings_to_process)} listings requiring full details.")
    
    # Extract URLs
    candidate_urls = [c['listing_url'] for c in listings_to_process]
    
    # STAGE 2: Scrape full details
    max_stage2 = config.get('marketplace_cheerio', {}).get('max_stage2_items', 50)
    logger.info(f"Scraping full details (max {max_stage2} items)...")
    
    try:
        stage2_listings = cheerio_scraper.scrape_full_details(candidate_urls, max_stage2_items=max_stage2)
        logger.info(f"✓ Scraped {len(stage2_listings)} full listings")
    except Exception as e:
        logger.error(f"✗ Error scraping: {e}", exc_info=True)
        sys.exit(1)
    
    # Process and save
    logger.info("Processing and updating full listings in the database...")
    
    processed_count = 0
    updated_count = 0
    
    with Database(db_url) as db:
        for listing_details in stage2_listings:
            processed_count += 1
            
            fb_id = listing_details.get('fb_id')
            if not fb_id:
                logger.warning(f"Scraped listing missing fb_id, skipping.")
                continue
            
            # The fb_id from cheerio might have a prefix, let's assume it's clean for now
            # as the new DB schema relies on a consistent fb_id.
            
            # Parse full text
            full_text = f"{listing_details.get('title', '')} {listing_details.get('description', '')}"
            params = parser.parse(full_text)
            
            # Final criteria check
            criterias = config.get('criterias', {})
            passed, reason = parser.matches_criteria(params, criterias)
            
            logger.info(f"Processing {fb_id}: Passed simple filters: {passed}. Reason: {reason}")

            # Prepare details for DB update
            update_details = {
                'description': listing_details.get('description', ''),
                'phone_number': (parser.extract_phone_numbers(full_text) or [None])[0],
                'bedrooms': params.get('bedrooms'),
                'price_extracted': params.get('price'),
                'kitchen_type': params.get('kitchen_type'),
                'has_ac': params.get('has_ac', False),
                'has_wifi': params.get('has_wifi', False),
                'has_pool': params.get('has_pool', False),
                'has_parking': params.get('has_parking', False),
                'utilities': params.get('utilities'),
                'furniture': params.get('furniture'),
                'rental_term': params.get('rental_term'),
                'all_images': json.dumps(listing_details.get('all_images', [])),
                'timestamp': listing_details.get('timestamp')
            }
            
            db.update_listing_after_stage2(fb_id, update_details, passed)
            updated_count += 1
            
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 2 COMPLETE")
    logger.info(f"Processed: {processed_count}")
    logger.info(f"Updated in DB: {updated_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()