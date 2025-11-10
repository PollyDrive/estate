#!/usr/bin/env python3
"""
Process no_description listings:
- Marketplace → move to stage1 (need Stage 2 scraping)
- Facebook Groups → parse title as description, apply filters
"""

import os
import sys
import logging
import json
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/process_no_description.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Process no_description listings based on source."""
    
    logger.info("=" * 80)
    logger.info("Processing no_description listings")
    logger.info("=" * 80)
    
    load_dotenv()
    
    # Load config
    config_path = 'config/config.json'
    if not os.path.exists(config_path):
        config_path = '/app/config/config.json'
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    parser = PropertyParser(config)
    criterias = config.get('criterias', {})
    
    marketplace_count = 0
    group_processed = 0
    group_passed = 0
    group_failed = 0
    
    with Database() as db:
        # Get all no_description listings
        query = """
            SELECT fb_id, title, source
            FROM listings
            WHERE status = 'no_description'
            ORDER BY source, created_at DESC
        """
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
        
        logger.info(f"Found {len(listings)} no_description listings\n")
        
        for listing in listings:
            fb_id = listing['fb_id']
            title = listing['title'] or ''
            source = listing['source']
            
            logger.info(f"Processing {fb_id} ({source})")
            logger.info(f"  Title: {title[:80]}...")
            
            if source in ['apify-marketplace', 'marketplace']:
                # Marketplace → move to stage1 (need full scraping)
                logger.info(f"  → Moving to stage1 (marketplace needs Stage 2)")
                
                db.cursor.execute(
                    "UPDATE listings SET status = 'stage1' WHERE fb_id = %s",
                    (fb_id,)
                )
                db.conn.commit()
                marketplace_count += 1
                
            elif source == 'facebook_group':
                # Facebook Groups → parse title as description
                logger.info(f"  → Parsing title as description")
                
                # Parse title
                params = parser.parse(title)
                passed, reason = parser.matches_criteria(params, criterias)
                
                logger.info(f"     Bedrooms: {params.get('bedrooms')}")
                logger.info(f"     Price: {params.get('price')}")
                logger.info(f"     Kitchen: {params.get('kitchen_type')}")
                
                if passed:
                    # Copy title to description and move to stage2
                    logger.info(f"  ✓ PASSED: {reason} → stage2")
                    
                    db.cursor.execute(
                        """UPDATE listings 
                           SET description = %s,
                               status = 'stage2',
                               bedrooms = %s,
                               price_extracted = %s,
                               kitchen_type = %s,
                               has_ac = %s,
                               has_wifi = %s
                           WHERE fb_id = %s""",
                        (
                            title,  # Use title as description
                            params.get('bedrooms'),
                            params.get('price'),
                            params.get('kitchen_type'),
                            params.get('has_ac', False),
                            params.get('has_wifi', False),
                            fb_id
                        )
                    )
                    db.conn.commit()
                    group_passed += 1
                else:
                    # Failed filters → stage2_failed
                    logger.info(f"  ✗ FAILED: {reason} → stage2_failed")
                    
                    db.cursor.execute(
                        """UPDATE listings 
                           SET description = %s,
                               status = 'stage2_failed'
                           WHERE fb_id = %s""",
                        (title, fb_id)
                    )
                    db.conn.commit()
                    group_failed += 1
                
                group_processed += 1
            
            logger.info("")
    
    # Summary
    logger.info("=" * 80)
    logger.info("PROCESSING COMPLETE")
    logger.info(f"Marketplace → stage1: {marketplace_count}")
    logger.info(f"Groups processed: {group_processed}")
    logger.info(f"  ✓ Groups passed → stage2: {group_passed}")
    logger.info(f"  ✗ Groups failed → stage2_failed: {group_failed}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
