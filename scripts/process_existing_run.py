#!/usr/bin/env python3
"""
Process Existing Run: Fetches data from a specified Apify run dataset,
filters it, and saves new candidates to the database.
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
from group_scraper import FacebookGroupScraper

# --- Constants ---
# The ID of the Apify run we want to process
EXISTING_RUN_ID = "yt45zJqAvlE1sU9gG"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/process_existing_run.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Fetches and processes data from a specific Apify run."""
    
    logger.info("=" * 80)
    logger.info(f"Processing Existing Apify Run: {EXISTING_RUN_ID}")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not all([db_url, apify_key]):
        logger.error("Missing required environment variables (DATABASE_URL, APIFY_API_KEY)!")
        sys.exit(1)

    # --- Fetching Logic ---
    parser = PropertyParser(config)
    scraper = FacebookGroupScraper(apify_key, config)
    
    try:
        all_posts = scraper.fetch_results_from_run(EXISTING_RUN_ID)
        if not all_posts:
            logger.warning("No posts were fetched from the specified run. Exiting.")
            sys.exit(0)
        logger.info(f"✓ Fetched {len(all_posts)} total posts from run {EXISTING_RUN_ID}.")
    except Exception as e:
        logger.error(f"✗ Error during fetching: {e}", exc_info=True)
        sys.exit(1)
    
    # --- Filtering Logic ---
    logger.info("Filtering candidates by title/text criteria...")
    criterias = config.get('criterias', {})
    stop_words = config.get('filters', {}).get('stop_words', [])
    stop_locations = config.get('filters', {}).get('stop_locations', [])
    candidates = []
    
    for post in all_posts:
        params = parser.parse(post['title'])
        passed, reason = parser.matches_criteria(params, criterias)
        
        # Additional stop-word filtering in title
        if passed:
            title_lower = post['title'].lower()
            for stop_word in stop_words:
                if stop_word.lower() in title_lower:
                    passed = False
                    reason = f"Stop word in title: {stop_word}"
                    break
        
        # Additional stop-location filtering in location field
        if passed and post.get('location'):
            location_lower = post['location'].lower()
            for stop_loc in stop_locations:
                if stop_loc.lower() in location_lower:
                    passed = False
                    reason = f"Stop location found: {stop_loc}"
                    break
        
        if passed:
            candidates.append(post)
            logger.info(f"  ✓ CANDIDATE: Post {post.get('fb_id', 'N/A')} from group {post.get('group_id', 'N/A')} - {reason}")
        else:
            logger.debug(f"  ✗ FILTERED: Post {post.get('fb_id', 'N/A')} - {reason}")
            
    pass_rate = (len(candidates) / len(all_posts) * 100) if all_posts else 0
    logger.info(f"✓ {len(candidates)}/{len(all_posts)} posts passed filters ({pass_rate:.1f}%)")
    
    # --- Database Logic ---
    saved_count = 0
    if candidates:
        logger.info(f"Saving {len(candidates)} candidates to database...")
        with Database() as db:
            for candidate in candidates:
                try:
                    was_added = db.add_listing_from_stage1(
                        fb_id=candidate['fb_id'],
                        title=candidate['title'],
                        price=candidate.get('price', ''),
                        location=candidate.get('location', ''),
                        listing_url=candidate['listing_url'],
                        source='facebook_group',
                        group_id=str(candidate.get('group_id', ''))
                    )
                    if was_added:
                        saved_count += 1
                except Exception as e:
                    logger.warning(f"Could not save candidate {candidate.get('fb_id', 'N/A')}: {e}")
        
        logger.info(f"✓ Saved {saved_count} new unique candidates to 'listings' table.")

    # --- Summary ---
    logger.info("=" * 80)
    logger.info("EXISTING RUN PROCESSING COMPLETE")
    logger.info(f"Total posts fetched: {len(all_posts)}")
    logger.info(f"Candidates found: {len(candidates)}")
    logger.info(f"New candidates saved: {saved_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
