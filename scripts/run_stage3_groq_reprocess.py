#!/usr/bin/env python3
"""
STAGE 3 REPROCESS: Re-run Groq LLM Analysis for ALL listings
Updates existing final_listings with new Groq analysis using updated prompt.
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
from llm_filters import Level1Filter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage3_groq_reprocess.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Re-run Stage 3: Groq LLM analysis for ALL listings"""
    
    logger.info("=" * 80)
    logger.info("STAGE 3 REPROCESS: Re-run Groq LLM Analysis for ALL listings")
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
    groq_api_key = os.getenv('GROQ_API_KEY')
    
    if not all([db_url, groq_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, GROQ_API_KEY)!")
        sys.exit(1)
    
    # Initialize Groq filter
    try:
        groq_filter = Level1Filter(config, groq_api_key)
        logger.info("✓ Groq filter initialized with NEW prompt")
    except Exception as e:
        logger.error(f"✗ Failed to initialize Groq filter: {e}")
        sys.exit(1)
    
    # Get ALL marketplace listings with descriptions
    with Database(db_url) as db:
        query = """
            SELECT fb_id, title, description, location, price,
                   phone_number, bedrooms, price_extracted, kitchen_type,
                   has_ac, has_wifi, has_pool, has_parking,
                   utilities, furniture, rental_term, listing_url, source
            FROM fb_listings
            WHERE source = 'marketplace'
            AND description IS NOT NULL 
            AND description != ''
            ORDER BY created_at DESC
        """
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
    
    if not listings:
        logger.warning("No listings found")
        sys.exit(0)
    
    logger.info(f"Found {len(listings)} listings to re-analyze")
    logger.info("=" * 80)
    
    # Process each listing
    passed_count = 0
    failed_count = 0
    error_count = 0
    
    with Database(db_url) as db:
        for idx, listing in enumerate(listings, 1):
            fb_id = listing['fb_id']
            description = listing['description']
            title = listing.get('title') or ''
            
            logger.info(f"[{idx}/{len(listings)}] {fb_id}: {title[:40] if title else 'No title'}...")
            
            # Run Groq analysis
            try:
                passed, reason = groq_filter.filter(description)
                
                # Save to final_listings table (will update if exists)
                db.save_to_final_listings(
                    listing_data=listing,
                    groq_passed=passed,
                    groq_reason=reason
                )
                
                if passed:
                    logger.info(f"  ✓ PASSED: {reason}")
                    passed_count += 1
                else:
                    logger.info(f"  ✗ FILTERED: {reason}")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"  ✗ ERROR: {e}")
                error_count += 1
    
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 3 REPROCESS COMPLETE")
    logger.info(f"Total analyzed: {len(listings)}")
    logger.info(f"Passed: {passed_count} ({100*passed_count/len(listings):.1f}%)")
    logger.info(f"Filtered: {failed_count} ({100*failed_count/len(listings):.1f}%)")
    logger.info(f"Errors: {error_count}")
    logger.info("")
    logger.info("All listings in final_listings have been updated with new Groq analysis")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
