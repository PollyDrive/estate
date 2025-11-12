#!/usr/bin/env python3
"""
Import results from a specific Apify Group run into the database.
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Import results from specific Apify run"""
    
    if len(sys.argv) < 2:
        logger.error("Usage: python import_last_group_run.py <RUN_ID>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    
    logger.info("=" * 80)
    logger.info(f"IMPORTING APIFY RUN: {run_id}")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not apify_key:
        logger.error("Missing APIFY_API_KEY!")
        sys.exit(1)
    
    # Fetch results from run
    scraper = FacebookGroupScraper(apify_key, config)
    
    try:
        all_posts = scraper.fetch_results_from_run(run_id)
        logger.info(f"✓ Fetched {len(all_posts)} posts from run {run_id}")
    except Exception as e:
        logger.error(f"✗ Error fetching results: {e}", exc_info=True)
        sys.exit(1)
    
    if not all_posts:
        logger.warning("No posts found in run!")
        sys.exit(0)
    
    # Filter candidates
    logger.info("Filtering candidates by title/text criteria...")
    parser = PropertyParser(config)
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
            logger.info(f"  ✓ CANDIDATE: Post {post.get('fb_id', 'N/A')} from group {post.get('group_id', 'N/A')}")
        else:
            logger.debug(f"  ✗ FILTERED: Post {post.get('fb_id', 'N/A')} - {reason}")
    
    pass_rate = (len(candidates) / len(all_posts) * 100) if all_posts else 0
    logger.info(f"✓ {len(candidates)}/{len(all_posts)} posts passed filters ({pass_rate:.1f}%)")
    
    # Save to database
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
                        group_id=str(candidate.get('group_id', '')),
                        description=candidate.get('description', '')
                    )
                    if was_added:
                        saved_count += 1
                except Exception as e:
                    logger.warning(f"Could not save candidate {candidate.get('fb_id', 'N/A')}: {e}")
        
        logger.info(f"✓ Saved {saved_count} new unique candidates to 'listings' table.")
    
    # Summary
    logger.info("=" * 80)
    logger.info("IMPORT COMPLETE")
    logger.info(f"Total posts fetched: {len(all_posts)}")
    logger.info(f"Candidates found: {len(candidates)}")
    logger.info(f"New candidates saved: {saved_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
