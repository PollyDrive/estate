#!/usr/bin/env python3
"""
STAGE 1 (Groups): Manual scraping of Facebook Groups with rotation.
Scrapes N posts from a subset of configured groups based on last-scraped time,
filters them, and saves candidates to DB.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import json
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser
from group_scraper import FacebookGroupScraper

# --- Constants ---
STATE_FILE = 'config/scraper_state.json'
GROUPS_PER_RUN = 6 # How many groups to scrape in a single run

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage1_groups_manual.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def load_state():
    """Loads the scraper state file."""
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(state):
    """Saves the scraper state file."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def get_groups_to_scrape(all_groups, state):
    """Selects the least recently scraped groups."""
    # Sort groups by last scraped time (oldest first). Groups not in state are considered oldest.
    sorted_groups = sorted(
        all_groups,
        key=lambda g: datetime.fromisoformat(state.get(g, "1970-01-01T00:00:00Z"))
    )
    return sorted_groups[:GROUPS_PER_RUN]

# --- Main Logic ---
def main():
    """Run Stage 1 for a subset of Facebook Groups."""
    
    logger.info("=" * 80)
    logger.info("STAGE 1: Rotational Group Scraping")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not all([db_url, apify_key]):
        logger.error("Missing required environment variables (DATABASE_URL, APIFY_API_KEY)!")
        sys.exit(1)
    
    groups_config = config.get('facebook_groups', {})
    if not groups_config.get('enabled', False):
        logger.warning("Facebook Groups scraping is disabled in config.json. Exiting.")
        sys.exit(0)
        
    all_group_ids = groups_config.get('groups', [])
    if not all_group_ids:
        logger.warning("No group IDs found in config.json. Exiting.")
        sys.exit(0)

    # --- Scheduling Logic ---
    state = load_state()
    groups_to_scrape = get_groups_to_scrape(all_group_ids, state)
    logger.info(f"Total groups in config: {len(all_group_ids)}")
    logger.info(f"This run will scrape {len(groups_to_scrape)} groups (least recent): {', '.join(groups_to_scrape)}")

    # --- Scraping Logic ---
    parser = PropertyParser(config)
    scraper = FacebookGroupScraper(apify_key, config)
    
    try:
        all_posts, successful_groups = scraper.scrape_posts(groups_to_scrape)
        logger.info(f"✓ Scraped {len(all_posts)} total posts from {len(successful_groups)}/{len(groups_to_scrape)} groups.")
        
        if len(successful_groups) < len(groups_to_scrape):
            failed_groups = set(groups_to_scrape) - set(successful_groups)
            logger.warning(f"⚠ {len(failed_groups)} groups failed to scrape: {', '.join(failed_groups)}")
            logger.info(f"These groups will be retried in the next run")
    except Exception as e:
        logger.error(f"✗ Error during scraping: {e}", exc_info=True)
        logger.warning("Continuing with empty results...")
        all_posts = []
        successful_groups = []
    
    # --- Filtering Logic ---
    logger.info("Filtering candidates by description/text criteria...")
    criterias = config.get('criterias', {})
    stop_words = config.get('filters', {}).get('stop_words', [])
    stop_locations = config.get('filters', {}).get('stop_locations', [])
    candidates = []
    
    for post in all_posts:
        # For groups, parse description (not title which is empty)
        description = post.get('description', '')
        params = parser.parse(description)
        passed, reason = parser.matches_criteria(params, criterias, stage=1)
        
        # Additional stop-word filtering in description text
        if passed and description:
            description_lower = description.lower()
            for stop_word in stop_words:
                if stop_word.lower() in description_lower:
                    passed = False
                    reason = f"Stop word in description: {stop_word}"
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
                        group_id=str(candidate.get('group_id', '')),
                        description=candidate.get('description', '')
                    )
                    if was_added:
                        saved_count += 1
                except Exception as e:
                    logger.warning(f"Could not save candidate {candidate.get('fb_id', 'N/A')}: {e}")
        
        logger.info(f"✓ Saved {saved_count} new unique candidates to 'listings' table.")

    # --- Update State Logic ---
    # Only update state for successfully scraped groups
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    if successful_groups:
        logger.info(f"Updating state for successful groups: {', '.join(successful_groups)}")
        for group_id in successful_groups:
            state[group_id] = now_iso
        save_state(state)
        logger.info(f"✓ Updated scraper state for {len(successful_groups)} successfully scraped groups.")
    else:
        logger.warning("No successful groups to update in state (all groups may have failed or returned no valid posts)")
    
    if len(successful_groups) < len(groups_to_scrape):
        failed_count = len(groups_to_scrape) - len(successful_groups)
        failed_groups = set(groups_to_scrape) - set(successful_groups)
        logger.info(f"⚠ {failed_count} groups were not updated and will be retried in the next run: {', '.join(failed_groups)}")

    # --- Summary ---
    logger.info("=" * 80)
    logger.info("GROUP SCRAPING (STAGE 1) COMPLETE")
    logger.info(f"Total posts scraped: {len(all_posts)}")
    logger.info(f"Candidates found: {len(candidates)}")
    logger.info(f"New candidates saved: {saved_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()