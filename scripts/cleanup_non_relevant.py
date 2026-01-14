#!/usr/bin/env python3
"""
Universal cleanup script - moves non-relevant listings to listing_non_relevant table.

Moves:
- Empty listings (no description AND no title)
- stage2_failed (failed Stage 2 filters)
- stage4_duplicate (duplicates found in Stage 4)
- stage3_failed (failed LLM analysis) - optional
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import argparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/cleanup_non_relevant.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Move reasons by status
MOVE_REASONS = {
    'empty': 'Empty description and title',
    'stage2_failed': 'Failed Stage 2 filters (no kitchen, wrong bedrooms, price too high, etc)',
    'stage4_duplicate': 'Duplicate listing (Stage 4 deduplication)',
    'stage3_failed': 'Failed LLM analysis (Stage 3)',
    'no_description': 'No description available'
}


def move_listings(db, condition, move_reason, description):
    """Move listings matching condition to listing_non_relevant."""
    
    logger.info(f"\n{description}...")
    
    # Count
    count_query = f"SELECT COUNT(*) FROM listings WHERE {condition}"
    db.cursor.execute(count_query)
    count = db.cursor.fetchone()[0]
    
    if count == 0:
        logger.info(f"  No listings found.")
        return 0
    
    logger.info(f"  Found {count} listings to move")
    
    # Get listings
    select_query = f"""
        SELECT fb_id, title, price, location, listing_url, description, 
               phone_number, source, group_id, bedrooms, price_extracted, 
               kitchen_type, has_ac, has_wifi, has_pool, has_parking, 
               utilities, furniture, rental_term, created_at, location_extracted
        FROM listings 
        WHERE {condition}
    """
    db.cursor.execute(select_query)
    listings = db.cursor.fetchall()
    
    moved = 0
    errors = 0
    
    for listing in listings:
        fb_id = listing[0]
        
        try:
            # Insert into non_relevant
            insert_query = """
                INSERT INTO listing_non_relevant 
                (fb_id, title, price, location, listing_url, description, 
                 phone_number, source, group_id, bedrooms, price_extracted, 
                 kitchen_type, has_ac, has_wifi, has_pool, has_parking, 
                 utilities, furniture, rental_term, created_at, 
                 moved_at, move_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (fb_id) DO NOTHING
            """
            
            values = list(listing[:20]) + [move_reason]
            db.cursor.execute(insert_query, values)
            
            # Delete from listings
            db.cursor.execute("DELETE FROM listings WHERE fb_id = %s", (fb_id,))
            db.conn.commit()
            
            moved += 1
            
        except Exception as e:
            logger.error(f"  Error moving {fb_id}: {e}")
            db.conn.rollback()
            errors += 1
    
    logger.info(f"  ✓ Moved: {moved}, Errors: {errors}")
    return moved


def main():
    parser = argparse.ArgumentParser(description='Move non-relevant listings')
    parser.add_argument('--include-stage3', action='store_true',
                      help='Also move stage3_failed listings')
    parser.add_argument('--include-no-desc', action='store_true',
                      help='Also move no_description listings')
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("CLEANUP: Moving non-relevant listings")
    logger.info("=" * 80)
    
    load_dotenv()
    
    total_moved = 0
    
    with Database() as db:
        # 1. Empty listings
        total_moved += move_listings(
            db,
            "(description IS NULL OR description = '') AND (title IS NULL OR title = '')",
            MOVE_REASONS['empty'],
            "Moving empty listings (no description AND no title)"
        )
        
        # 2. Stage 2 failed
        total_moved += move_listings(
            db,
            "status = 'stage2_failed'",
            MOVE_REASONS['stage2_failed'],
            "Moving Stage 2 failed listings"
        )
        
        # 3. Stage 4 duplicates
        total_moved += move_listings(
            db,
            "status = 'stage4_duplicate'",
            MOVE_REASONS['stage4_duplicate'],
            "Moving Stage 4 duplicates"
        )
        
        # 4. Stage 3 failed (optional)
        if args.include_stage3:
            total_moved += move_listings(
                db,
                "status = 'stage3_failed'",
                MOVE_REASONS['stage3_failed'],
                "Moving Stage 3 failed listings"
            )
        
        # 5. No description (optional)
        if args.include_no_desc:
            total_moved += move_listings(
                db,
                "status = 'no_description'",
                MOVE_REASONS['no_description'],
                "Moving no description listings"
            )
    
    logger.info("\n" + "=" * 80)
    logger.info(f"CLEANUP COMPLETED: {total_moved} listings moved to listing_non_relevant")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
