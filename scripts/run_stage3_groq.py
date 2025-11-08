#!/usr/bin/env python3
"""
STAGE 3: Groq LLM Analysis for saved listings
Analyzes descriptions with Groq (free) to check if listing matches criteria.
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
from llm_filters import Level1Filter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage3_groq.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 3: Groq LLM analysis for unprocessed listings"""
    
    logger.info("=" * 80)
    logger.info("STAGE 3: Groq LLM Analysis")
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
        logger.info("✓ Groq filter initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize Groq filter: {e}")
        sys.exit(1)
    
    # Get unprocessed listings (with description but not in final_listings yet)
    with Database(db_url) as db:
        query = """
            SELECT fl.fb_id, fl.title, fl.description, fl.location, fl.price,
                   fl.phone_number, fl.bedrooms, fl.price_extracted, fl.kitchen_type,
                   fl.has_ac, fl.has_wifi, fl.has_pool, fl.has_parking,
                   fl.utilities, fl.furniture, fl.rental_term, fl.listing_url, fl.source
            FROM fb_listings fl
            LEFT JOIN final_listings fin ON fl.fb_id = fin.fb_id
            WHERE fl.source = 'marketplace'
            AND fl.description IS NOT NULL 
            AND fl.description != ''
            AND fin.fb_id IS NULL
            ORDER BY fl.created_at DESC
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
    
    with Database(db_url) as db:
        for listing in listings:
            fb_id = listing['fb_id']
            description = listing['description']
            title = listing.get('title') or ''
            location = listing.get('location') or ''
            
            logger.info(f"\nAnalyzing {fb_id}: {title[:50] if title else 'No title'}...")
            logger.info(f"  Location: {location}")
            
            # Run Groq analysis
            try:
                passed, reason = groq_filter.filter(description)
                
                # Save to final_listings table
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
    logger.info("Listings that passed Groq filter are ready for review/telegram")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
