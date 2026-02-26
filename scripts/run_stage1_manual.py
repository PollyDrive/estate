#!/usr/bin/env python3
"""
STAGE 1: Manual scraping with apify/facebook-marketplace-scraper
Scrapes 100 listings from today only, filters by title, saves candidates to DB.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser
from apify_scraper import ApifyScraper
from config_loader import load_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage1_manual.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 1: Title-only scraping and filtering"""
    
    logger.info("=" * 80)
    logger.info("STAGE 1: Manual Title Scraping (apify/facebook-marketplace-scraper)")
    logger.info("=" * 80)
    
    # Load environment
    load_dotenv()
    
    # Load config
    config = load_config()
    
    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not all([db_url, apify_key]):
        logger.error("Missing required environment variables!")
        sys.exit(1)
    
    # Initialize components
    parser = PropertyParser(config)  # Pass config to use stop_words from config.json
    scraper = ApifyScraper(apify_key, config)
    
    # STAGE 1: Scrape 100 listings from today
    max_items = config.get('apify', {}).get('max_listings', 100)
    logger.info(f"Scraping up to {max_items} listings from TODAY...")
    
    try:
        stage1_listings = scraper.scrape_titles_only(max_items=max_items)
        logger.info(f"✓ Scraped {len(stage1_listings)} listings")
    except Exception as e:
        logger.error(f"✗ Error scraping: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    
    # Filter by title
    logger.info("Filtering candidates by title criteria...")
    _profiles = config.get('chat_profiles', []) or []
    if not _profiles:
        logger.error("No chat_profiles found in config. Add profiles to config/profiles.json.")
        sys.exit(1)
    criterias = {
        'bedrooms_min': min(p['bedrooms_min'] for p in _profiles),
        'price_max': max(p['price_max'] for p in _profiles),
    }
    candidates = []
    
    for listing in stage1_listings:
        fb_id = listing.get('fb_id')
        title = listing.get('title', '')
        
        # Parse title only
        params = parser.parse(title)
        
        # Check criteria
        passed, reason = parser.matches_criteria(params, criterias)
        
        if passed:
            candidates.append(listing)
            logger.info(f"  ✓ CANDIDATE: {fb_id} - {reason}")
        else:
            logger.debug(f"  ✗ FILTERED: {fb_id} - {reason}")
    
    pass_rate = (len(candidates)/len(stage1_listings)*100) if len(stage1_listings) > 0 else 0
    logger.info(f"✓ {len(candidates)}/{len(stage1_listings)} passed filters ({pass_rate:.1f}%)")
    
    # Save candidates to DB
    saved_count = 0
    if candidates:
        logger.info(f"Saving {len(candidates)} candidates to database...")
        
        with Database() as db:
            for candidate in candidates:
                try:
                    # Use the new method to add listings with 'stage1' status
                    was_added = db.add_listing_from_stage1(
                        fb_id=candidate['fb_id'],
                        title=candidate['title'],
                        price=candidate.get('price', ''),
                        location=candidate.get('location', ''),
                        listing_url=candidate['listing_url'],
                        source='apify-marketplace'
                    )
                    if was_added:
                        saved_count += 1
                except Exception as e:
                    logger.warning(f"Could not save {candidate['fb_id']}: {e}")
            
            logger.info(f"✓ Saved {saved_count} new unique candidates to 'listings' table")
    
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 1 COMPLETE")
    logger.info(f"Scraped: {len(stage1_listings)} listings")
    logger.info(f"Candidates: {len(candidates)}")
    logger.info(f"Saved: {saved_count}")
    logger.info("")
    logger.info("Next step: Run Stage 2 to get full details")
    logger.info("Command: python3 run_stage2_manual.py")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
