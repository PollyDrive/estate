#!/usr/bin/env python3
"""
Move stage2_failed and stage4_duplicate listings to listing_non_relevant table.
These are listings that failed filters or are duplicates.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/move_failed_duplicates.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Move failed and duplicate listings to non-relevant table."""
    
    logger.info("=" * 80)
    logger.info("MOVING FAILED & DUPLICATE LISTINGS TO listing_non_relevant")
    logger.info("=" * 80)
    
    load_dotenv()
    
    statuses_to_move = ['stage2_failed', 'stage4_duplicate']
    
    with Database() as db:
        # First, check how many we have
        check_query = """
            SELECT status, COUNT(*) as count
            FROM listings 
            WHERE status = ANY(%s)
            GROUP BY status
        """
        db.cursor.execute(check_query, (statuses_to_move,))
        status_counts = {row[0]: row[1] for row in db.cursor.fetchall()}
        
        total_count = sum(status_counts.values())
        logger.info(f"Found {total_count} listings to move:")
        for status, count in status_counts.items():
            logger.info(f"  - {status}: {count}")
        
        if total_count == 0:
            logger.info("Nothing to move. Exiting.")
            return
        
        # Get the listings to move
        select_query = """
            SELECT fb_id, title, price, location, listing_url, description, 
                   phone_number, source, group_id, bedrooms, price_extracted, 
                   kitchen_type, has_ac, has_wifi, has_pool, has_parking, 
                   utilities, furniture, rental_term, created_at, status,
                   location_extracted
            FROM listings 
            WHERE status = ANY(%s)
            ORDER BY status, created_at
        """
        db.cursor.execute(select_query, (statuses_to_move,))
        listings_to_move = db.cursor.fetchall()
        
        logger.info(f"Moving {len(listings_to_move)} listings...")
        
        moved_count = 0
        error_count = 0
        
        move_reasons = {
            'stage2_failed': 'Failed Stage 2 filters (no kitchen, wrong bedrooms, etc)',
            'stage4_duplicate': 'Duplicate listing (Stage 4 deduplication)'
        }
        
        for listing in listings_to_move:
            fb_id = listing[0]
            old_status = listing[20]  # status is at index 20
            move_reason = move_reasons.get(old_status, f'Status: {old_status}')
            
            try:
                # Insert into listing_non_relevant
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
                
                values = list(listing[:20]) + [move_reason]  # First 20 fields + move_reason
                db.cursor.execute(insert_query, values)
                
                # Delete from listings
                delete_query = "DELETE FROM listings WHERE fb_id = %s"
                db.cursor.execute(delete_query, (fb_id,))
                
                db.conn.commit()
                moved_count += 1
                
                if moved_count % 10 == 0:
                    logger.info(f"  Moved {moved_count}/{len(listings_to_move)}...")
                    
            except Exception as e:
                logger.error(f"Error moving listing {fb_id}: {e}")
                db.conn.rollback()
                error_count += 1
        
        logger.info("=" * 80)
        logger.info(f"COMPLETED")
        logger.info(f"Successfully moved: {moved_count}")
        logger.info(f"Errors: {error_count}")
        
        # Show final counts
        logger.info("\nFinal counts by status:")
        for status, count in status_counts.items():
            logger.info(f"  - {status}: {count} moved")
        
        logger.info("=" * 80)


if __name__ == '__main__':
    main()
