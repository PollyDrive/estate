#!/usr/bin/env python3
"""
STAGE 3: Zhipu LLM Analysis for saved listings
Analyzes descriptions with Zhipu GLM-4 to check if listing matches criteria.
Updates fb_listings with analysis results.
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
from llm_filters import ZhipuFilter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage3_zhipu.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 3: Zhipu LLM analysis for unprocessed listings"""
    
    logger.info("=" * 80)
    logger.info("STAGE 3: Zhipu GLM-4 Analysis")
    logger.info("=" * 80)
    
    # Load environment
    load_dotenv()
    
    # Load config
    config_path = 'config/config.json'
    if not os.path.exists(config_path):
        config_path = '/app/config.json'
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    zhipu_api_key = os.getenv('ZHIPU_API_KEY')
    
    if not all([db_url, zhipu_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, ZHIPU_API_KEY)!")
        sys.exit(1)
    
    # Initialize Zhipu filter
    try:
        zhipu_filter = ZhipuFilter(config, zhipu_api_key)
        logger.info("✓ Zhipu filter initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize Zhipu filter: {e}")
        sys.exit(1)
    
    # Get listings with status 'stage2' ready for Groq analysis
    with Database() as db:
        query = """
            SELECT fb_id, title, description, location, price,
                   phone_number, bedrooms, price_extracted, kitchen_type,
                   has_ac, has_wifi, has_pool, has_parking,
                   utilities, furniture, rental_term, listing_url, source
            FROM listings
            WHERE status = 'stage2'
            AND description IS NOT NULL 
            AND description != ''
            ORDER BY created_at DESC
        """
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
    
    if not listings:
        logger.warning("No unprocessed listings found")
        logger.info("All listings have been analyzed or don't have descriptions")
        sys.exit(0)
    
    logger.info(f"Found {len(listings)} listings to analyze")
    
    # Process each listing
    passed_count = 0
    failed_count = 0
    error_count = 0
    
    with Database() as db:
        for listing in listings:
            fb_id = listing['fb_id']
            description = listing['description']
            title = listing.get('title') or ''
            location = listing.get('location') or ''
            
            logger.info(f"\nAnalyzing {fb_id}: {title[:50] if title else 'No title'}...")
            logger.info(f"  Location: {location}")
            
            # Run Zhipu analysis
            try:
                passed, reason = zhipu_filter.filter(description)
                
                # Update status based on Zhipu result
                new_status = 'stage3' if passed else 'stage3_failed'
                
                # Save Zhipu analysis result to groq_reason field (keep column name for compatibility)
                db.cursor.execute(
                    "UPDATE listings SET status = %s, groq_reason = %s WHERE fb_id = %s",
                    (new_status, reason, fb_id)
                )
                db.conn.commit()
                
                if passed:
                    logger.info(f"  ✓ PASSED: {reason} → status: stage3")
                    passed_count += 1
                else:
                    logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"  ✗ ERROR: {e}")
                import traceback
                logger.error(traceback.format_exc())
                error_count += 1
    
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 3 COMPLETE")
    logger.info(f"Total analyzed: {len(listings)}")
    logger.info(f"Passed: {passed_count}")
    logger.info(f"Filtered: {failed_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("")
    logger.info("Listings with status 'stage3' are ready for Stage 4 (deduplication)")
    logger.info("Command: python3 scripts/run_stage4.py")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
