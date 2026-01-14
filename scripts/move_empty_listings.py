#!/usr/bin/env python3
"""
Move listings with empty description AND title to listing_non_relevant table.
These are incomplete listings that cannot be processed.
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
        logging.FileHandler('logs/move_empty_listings.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Move empty listings to non-relevant table."""
    
    logger.info("=" * 80)
    logger.info("MOVING EMPTY LISTINGS TO listing_non_relevant")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with Database() as db:
        # First, check how many we have
        check_query = """
            SELECT COUNT(*) as count
            FROM listings 
            WHERE (description IS NULL OR description = '') 
              AND (title IS NULL OR title = '')
        """
        db.cursor.execute(check_query)
        count = db.cursor.fetchone()[0]
        
        logger.info(f"Found {count} listings with empty description AND title")
        
        if count == 0:
            logger.info("Nothing to move. Exiting.")
            return
        
        # Get the listings to move
        select_query = """
            SELECT fb_id, title, price, location, listing_url, description, 
                   phone_number, source, group_id, bedrooms, price_extracted, 
                   kitchen_type, has_ac, has_wifi, has_pool, has_parking, 
                   utilities, furniture, rental_term, created_at
            FROM listings 
            WHERE (description IS NULL OR description = '') 
              AND (title IS NULL OR title = '')
        """
        db.cursor.execute(select_query)
        listings_to_move = db.cursor.fetchall()
        
        logger.info(f"Moving {len(listings_to_move)} listings...")
        
        moved_count = 0
        error_count = 0
        
        for listing in listings_to_move:
            fb_id = listing[0]
            
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
                
                values = list(listing) + ['Empty description and title']
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
        logger.info("=" * 80)


if __name__ == '__main__':
    main()
