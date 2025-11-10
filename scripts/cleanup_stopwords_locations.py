#!/usr/bin/env python3
"""
CLEANUP SCRIPT: Check stop_words in title and stop_locations in location.
Marks listings that fail as 'rejected_by_cleanup'.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database, STATUS_STAGE1_NEW

# --- Constants ---
STATUS_REJECTED_BY_CLEANUP = 'rejected_by_cleanup'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/cleanup_stopwords_locations.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def check_stop_words(text, stop_words):
    """Check if text contains any stop words."""
    if not text:
        return None
    
    text_lower = text.lower()
    for stop_word in stop_words:
        if stop_word.lower() in text_lower:
            return stop_word
    return None


def check_stop_locations(location, stop_locations):
    """Check if location contains any stop locations."""
    if not location:
        return None
    
    location_lower = location.lower()
    for stop_loc in stop_locations:
        if stop_loc.lower() in location_lower:
            return stop_loc
    return None


# --- Main Logic ---
def main():
    """Runs the cleanup process for stop words and locations."""
    
    logger.info("=" * 80)
    logger.info("CLEANUP SCRIPT: Checking stop_words in title and stop_locations in location")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    db_url = os.getenv('DATABASE_URL')
    
    if not db_url:
        logger.error("Missing DATABASE_URL environment variable!")
        sys.exit(1)

    # Load stop words and locations from config
    stop_words = config['filters']['stop_words']
    stop_locations = config['filters']['stop_locations']
    
    logger.info(f"Loaded {len(stop_words)} stop words")
    logger.info(f"Loaded {len(stop_locations)} stop locations")
    
    processed_count = 0
    rejected_count = 0
    
    with Database() as db:
        # Fetch all listings that are currently in stage1_new
        query = "SELECT fb_id, title, location FROM listings WHERE status = %s"
        db.cursor.execute(query, (STATUS_STAGE1_NEW,))
        columns = [desc[0] for desc in db.cursor.description]
        listings_to_cleanup = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
        
        if not listings_to_cleanup:
            logger.info("No listings found with status 'stage1_new' for cleanup. Exiting.")
            sys.exit(0)
            
        logger.info(f"Found {len(listings_to_cleanup)} listings to check.")

        for listing in listings_to_cleanup:
            processed_count += 1
            fb_id = listing['fb_id']
            title = listing.get('title', '')
            location = listing.get('location', '')
            
            # Check stop words in title
            found_stop_word = check_stop_words(title, stop_words)
            if found_stop_word:
                rejected_count += 1
                reason = f"Stop word in title: '{found_stop_word}'"
                logger.info(f"  ✗ REJECTED: {fb_id} - {reason}")
                db.cursor.execute(
                    "UPDATE listings SET status = %s WHERE fb_id = %s",
                    (STATUS_REJECTED_BY_CLEANUP, fb_id)
                )
                db.conn.commit()
                continue
            
            # Check stop locations in location
            found_stop_location = check_stop_locations(location, stop_locations)
            if found_stop_location:
                rejected_count += 1
                reason = f"Stop location: '{found_stop_location}'"
                logger.info(f"  ✗ REJECTED: {fb_id} - {reason}")
                db.cursor.execute(
                    "UPDATE listings SET status = %s WHERE fb_id = %s",
                    (STATUS_REJECTED_BY_CLEANUP, fb_id)
                )
                db.conn.commit()
                continue
            
            # Passed all checks
            logger.debug(f"  ✓ PASSED: {fb_id}")
    
    logger.info("=" * 80)
    logger.info("CLEANUP SCRIPT COMPLETE")
    logger.info(f"Total listings checked: {processed_count}")
    logger.info(f"Rejected by cleanup: {rejected_count}")
    logger.info(f"Remaining for Stage 2: {processed_count - rejected_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
