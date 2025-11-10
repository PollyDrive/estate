#!/usr/bin/env python3
"""
CLEANUP SCRIPT: Iterates through existing 'stage1_new' listings,
re-applies Stage 1 filters, and marks those that fail as 'rejected_by_cleanup'.
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
from property_parser import PropertyParser

# --- Constants ---
STATUS_REJECTED_BY_CLEANUP = 'rejected_by_cleanup'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/cleanup_old_listings.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Main Logic ---
def main():
    """Runs the cleanup process for old listings."""
    
    logger.info("=" * 80)
    logger.info("CLEANUP SCRIPT: Re-applying Stage 1 filters to old listings")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    db_url = os.getenv('DATABASE_URL') # Not directly used, but for Database() init
    
    if not db_url:
        logger.error("Missing DATABASE_URL environment variable!")
        sys.exit(1)

    parser = PropertyParser(config)
    
    processed_count = 0
    rejected_count = 0
    
    with Database() as db:
        # Fetch all listings that are currently in stage1_new
        query = "SELECT fb_id, title, description FROM listings WHERE status = %s"
        db.cursor.execute(query, (STATUS_STAGE1_NEW,))
        columns = [desc[0] for desc in db.cursor.description]
        listings_to_cleanup = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
        
        if not listings_to_cleanup:
            logger.info("No listings found with status 'stage1_new' for cleanup. Exiting.")
            sys.exit(0)
            
        logger.info(f"Found {len(listings_to_cleanup)} listings to re-evaluate.")

        for listing in listings_to_cleanup:
            processed_count += 1
            fb_id = listing['fb_id']
            
            # Use title for Stage 1 filtering, as description might be incomplete/corrupted
            text_to_parse = listing.get('title', '')
            if not text_to_parse: # Fallback to description if title is empty
                text_to_parse = listing.get('description', '')

            params = parser.parse(text_to_parse)
            passed, reason = parser.matches_criteria(params, config.get('criterias', {}))
            
            if not passed:
                rejected_count += 1
                logger.info(f"  ✗ REJECTED: {fb_id} - {reason}. Marking as '{STATUS_REJECTED_BY_CLEANUP}'.")
                db.cursor.execute(
                    "UPDATE listings SET status = %s WHERE fb_id = %s",
                    (STATUS_REJECTED_BY_CLEANUP, fb_id)
                )
                db.conn.commit()
            else:
                logger.debug(f"  ✓ PASSED: {fb_id} - {reason}. Retaining '{STATUS_STAGE1_NEW}'.")
    
    logger.info("=" * 80)
    logger.info("CLEANUP SCRIPT COMPLETE")
    logger.info(f"Total listings re-evaluated: {processed_count}")
    logger.info(f"Rejected by cleanup: {rejected_count}")
    logger.info(f"Remaining for Stage 2: {processed_count - rejected_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
