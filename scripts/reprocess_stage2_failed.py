"""
Reprocess stage2_failed listings using ONLY description (without title).
Many failed due to incorrect parsing from title + description.
"""

import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import Database
from property_parser import PropertyParser
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration"""
    config_path = os.path.join('config', 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    logger.info("=" * 80)
    logger.info("Reprocess stage2_failed listings (parse from description only)")
    logger.info("=" * 80)
    
    # Load config
    config = load_config()
    
    # Initialize parser
    parser = PropertyParser(config)
    
    # Connect to database
    db = Database()
    db.connect()
    cur = db.conn.cursor()
    
    # Get all stage2_failed listings with descriptions
    cur.execute("""
        SELECT fb_id, title, description, source
        FROM listings
        WHERE status = 'stage2_failed'
          AND description IS NOT NULL
          AND description != ''
        ORDER BY source, fb_id
    """)
    
    failed_listings = cur.fetchall()
    logger.info(f"\nFound {len(failed_listings)} stage2_failed listings with descriptions")
    
    if not failed_listings:
        logger.info("✅ No listings to reprocess")
        cur.close()
        db.conn.close()
        return
    
    # Group by source
    by_source = {}
    for row in failed_listings:
        source = row[3]
        if source not in by_source:
            by_source[source] = []
        by_source[source].append(row)
    
    for source, count in [(s, len(items)) for s, items in by_source.items()]:
        logger.info(f"  {source}: {count}")
    
    print()
    response = input(f"Reprocess {len(failed_listings)} listings? (y/N): ").strip().lower()
    
    if response != 'y':
        logger.info("❌ Cancelled")
        cur.close()
        db.conn.close()
        return
    
    # Load detailed stop words
    stop_words_detailed = config.get('filters', {}).get('stop_words_detailed', [])
    stop_words_detailed_lower = [w.lower() for w in stop_words_detailed]
    
    # Process each listing
    stats = {
        'total': len(failed_listings),
        'now_stage2': 0,
        'still_failed': 0,
        'bedrooms_changed': 0,
        'price_changed': 0
    }
    
    changes_log = []
    
    logger.info("\n" + "=" * 80)
    logger.info("Processing...")
    logger.info("=" * 80)
    
    for fb_id, title, description, source in failed_listings:
        # Get current bedrooms/price
        cur.execute("""
            SELECT bedrooms, price_extracted
            FROM listings
            WHERE fb_id = %s
        """, (fb_id,))
        
        old_data = cur.fetchone()
        old_bedrooms = old_data[0] if old_data else None
        old_price = old_data[1] if old_data else None
        
        # Check for detailed stop words in description
        found_detailed_stop_word = None
        if description and stop_words_detailed_lower:
            description_lower = description.lower()
            for stop_word in stop_words_detailed_lower:
                if stop_word in description_lower:
                    found_detailed_stop_word = stop_word
                    break
        
        # Parse ONLY from description (ignore title completely)
        params = parser.parse(description)
        
        # Check criteria
        criterias = config.get('criterias', {})
        passed, reason = parser.matches_criteria(params, criterias)
        
        # Determine new status
        if found_detailed_stop_word:
            new_status = 'stage2_failed'
            logger.info(f"  ✗ {fb_id} ({source}): Still FAILED - stop word '{found_detailed_stop_word}'")
            stats['still_failed'] += 1
        elif passed:
            new_status = 'stage2'
            logger.info(f"  ✓ {fb_id} ({source}): NOW PASSED - {reason}")
            stats['now_stage2'] += 1
            
            # Track changes
            change_info = {'fb_id': fb_id, 'source': source, 'changes': []}
            
            new_bedrooms = params.get('bedrooms')
            new_price = params.get('price')
            
            if old_bedrooms != new_bedrooms:
                change_info['changes'].append(f"bedrooms: {old_bedrooms} → {new_bedrooms}")
                stats['bedrooms_changed'] += 1
            
            if old_price != new_price:
                try:
                    old_price_fmt = f'{float(old_price):,.0f}' if old_price else 'None'
                except (ValueError, TypeError):
                    old_price_fmt = str(old_price)
                new_price_fmt = f'{float(new_price):,.0f}' if new_price else 'None'
                change_info['changes'].append(f"price: {old_price_fmt} → {new_price_fmt}")
                stats['price_changed'] += 1
            
            if change_info['changes']:
                changes_log.append(change_info)
        else:
            new_status = 'stage2_failed'
            logger.info(f"  ✗ {fb_id} ({source}): Still FAILED - {reason}")
            stats['still_failed'] += 1
        
        # Extract phone number from description
        phone_numbers = parser.extract_phone_numbers(description)
        phone_number = phone_numbers[0] if phone_numbers else None
        
        # Update database
        cur.execute("""
            UPDATE listings
            SET status = %s,
                bedrooms = %s,
                price_extracted = %s,
                phone_number = %s,
                has_ac = %s,
                has_wifi = %s,
                updated_at = NOW()
            WHERE fb_id = %s
        """, (
            new_status,
            params.get('bedrooms'),
            params.get('price'),
            phone_number,
            params.get('has_ac', False),
            params.get('has_wifi', False),
            fb_id
        ))
    
    db.conn.commit()
    
    # Print statistics
    logger.info("\n" + "=" * 80)
    logger.info("REPROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total processed: {stats['total']}")
    logger.info(f"  ✓ Now stage2 (RESCUED): {stats['now_stage2']}")
    logger.info(f"  ✗ Still failed: {stats['still_failed']}")
    logger.info(f"\nChanges:")
    logger.info(f"  - Bedrooms changed: {stats['bedrooms_changed']}")
    logger.info(f"  - Price changed: {stats['price_changed']}")
    
    # Show sample changes
    if changes_log:
        logger.info("\n" + "=" * 80)
        logger.info("Sample rescued listings (first 10):")
        logger.info("=" * 80)
        for change in changes_log[:10]:
            logger.info(f"\n{change['fb_id']} ({change['source']}):")
            for detail in change['changes']:
                logger.info(f"  • {detail}")
    
    # Show final stats
    logger.info("\n" + "=" * 80)
    logger.info("Final database statistics:")
    logger.info("=" * 80)
    
    for source_name in ['apify-marketplace', 'facebook_group']:
        logger.info(f"\n{source_name}:")
        cur.execute("""
            SELECT status, COUNT(*)
            FROM listings
            WHERE source = %s
            GROUP BY status
            ORDER BY status
        """, (source_name,))
        
        for row in cur.fetchall():
            logger.info(f"  {row[0]}: {row[1]}")
    
    cur.close()
    db.conn.close()
    
    logger.info("\n✅ Reprocessing completed successfully!")

if __name__ == '__main__':
    main()
