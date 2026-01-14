#!/usr/bin/env python3
"""
STAGE 2: Manual full detail scraping with memo23/facebook-marketplace-cheerio
Takes candidates from the main 'listings' table with status 'stage1_new',
scrapes full details, and updates them with the results and a new status.
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
from property_parser import PropertyParser
from facebook_marketplace_cheerio_scraper import FacebookMarketplaceCheerioScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage2_manual.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 2: Full detail scraping for candidates"""
    
    logger.info("=" * 80)
    logger.info("STAGE 2: Manual Full Detail Scraping (Refactored)")
    logger.info("=" * 80)
    
    # Load environment
    load_dotenv()
    
    # Load config
    config_path = Path(__file__).parent.parent / 'config' / 'config.json'
    if not config_path.exists():
        config_path = Path('/app/config/config.json')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not all([db_url, apify_key]):
        logger.error("Missing required environment variables (DATABASE_URL, APIFY_API_KEY)!")
        sys.exit(1)
    
    # Initialize components
    parser = PropertyParser()
    cheerio_scraper = FacebookMarketplaceCheerioScraper(apify_key, config)
    
    # Get unprocessed candidates from DB
    with Database() as db:
        # Get all stage1 listings
        query = "SELECT fb_id, listing_url, source, description FROM listings WHERE status IN ('stage1', 'stage1_new') ORDER BY created_at DESC"
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings_to_process = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
    
    if not listings_to_process:
        logger.warning("No new listings with status 'stage1' or 'stage1_new' found.")
        sys.exit(0)
    
    logger.info(f"Found {len(listings_to_process)} listings to process.")
    
    # Separate Groups with description vs. those needing scraping
    groups_with_description = [l for l in listings_to_process if l['source'] == 'facebook_group' and l.get('description')]
    groups_no_description = [l for l in listings_to_process if l['source'] == 'facebook_group' and not l.get('description')]
    marketplace_listings = [l for l in listings_to_process if l['source'] in ('marketplace', 'apify', 'apify-marketplace', None, '')]
    
    logger.info(f"  Groups with description: {len(groups_with_description)}")
    logger.info(f"  Groups without description: {len(groups_no_description)}")
    logger.info(f"  Marketplace listings: {len(marketplace_listings)}")
    
    # Extract URLs for marketplace scraping
    candidate_urls = [c['listing_url'] for c in marketplace_listings]
    
    # STAGE 2: Scrape full details for marketplace
    stage2_listings = []
    if candidate_urls:
        max_stage2 = config.get('marketplace_cheerio', {}).get('max_stage2_items', 50)
        logger.info(f"Scraping full details for marketplace (max {max_stage2} items)...")
        
        try:
            stage2_listings = cheerio_scraper.scrape_full_details(candidate_urls, max_stage2_items=max_stage2)
            logger.info(f"✓ Scraped {len(stage2_listings)} full listings")
        except Exception as e:
            logger.error(f"✗ Error scraping: {e}", exc_info=True)
            # Don't exit, continue processing groups
    
    # Load detailed stop words
    stop_words_detailed = config.get('filters', {}).get('stop_words_detailed', [])
    stop_words_detailed_lower = [word.lower() for word in stop_words_detailed]
    
    logger.info(f"Loaded {len(stop_words_detailed)} detailed stop words for Stage 2")
    
    # Process and save
    logger.info("Processing and updating full listings in the database...")
    
    processed_count = 0
    updated_count = 0
    
    with Database() as db:
        for listing_details in stage2_listings:
            processed_count += 1
            
            fb_id = listing_details.get('fb_id')
            if not fb_id:
                logger.warning(f"Scraped listing missing fb_id, skipping.")
                continue
            
            description = listing_details.get('description', '')
            
            # Check for detailed stop words in description
            found_detailed_stop_word = None
            if description and stop_words_detailed_lower:
                description_lower = description.lower()
                for stop_word in stop_words_detailed_lower:
                    if stop_word in description_lower:
                        found_detailed_stop_word = stop_word
                        break
            
            # Determine status
            if found_detailed_stop_word:
                new_status = 'stage2_failed'
                logger.info(f"  ✗ REJECTED {fb_id}: Detailed stop word '{found_detailed_stop_word}' found")
            else:
                # Parse ONLY from description (title can be incorrect/outdated)
                params = parser.parse(description)
                
                # Final criteria check with Stage 2 filters (kitchen required, bedrooms >= 2)
                criterias = config.get('criterias', {})
                passed, reason = parser.matches_criteria(params, criterias, stage=2)
                
                new_status = 'stage2' if passed else 'stage2_failed'
                logger.info(f"Processing {fb_id}: Status '{new_status}'. Reason: {reason}")

            # Extract location from description
            location_extracted = parser.extract_location(description) if description else None
            
            # Prepare details for DB update
            update_details = {
                'description': description,
                'phone_number': (parser.extract_phone_numbers(description) or [None])[0],
                'bedrooms': params.get('bedrooms') if 'params' in locals() else None,
                'price_extracted': params.get('price') if 'params' in locals() else None,
                'has_ac': params.get('has_ac', False) if 'params' in locals() else False,
                'has_wifi': params.get('has_wifi', False) if 'params' in locals() else False,
                'has_pool': params.get('has_pool', False) if 'params' in locals() else False,
                'has_parking': params.get('has_parking', False) if 'params' in locals() else False,
                'utilities': params.get('utilities') if 'params' in locals() else None,
                'furniture': params.get('furniture') if 'params' in locals() else None,
                'rental_term': params.get('rental_term') if 'params' in locals() else None,
                'location_extracted': location_extracted,
                'status': new_status
            }
            
            # Update in database
            set_clause = ", ".join([f"{key} = %s" for key in update_details.keys()])
            query = f"UPDATE listings SET {set_clause} WHERE fb_id = %s"
            values = list(update_details.values())
            values.append(fb_id)
            
            db.cursor.execute(query, tuple(values))
            db.conn.commit()
            updated_count += 1
            
        # Process Groups with description (already have it, just apply filters)
        logger.info(f"\nProcessing {len(groups_with_description)} Groups with existing description...")
        for listing in groups_with_description:
            processed_count += 1
            fb_id = listing['fb_id']
            description = listing['description']
            
            # Extract title and location from description for groups
            extracted_title = parser.extract_title_from_description(description, max_length=150)
            location_extracted = parser.extract_location(description) if description else None
            
            # Check for detailed stop words in description
            found_detailed_stop_word = None
            if description and stop_words_detailed_lower:
                description_lower = description.lower()
                for stop_word in stop_words_detailed_lower:
                    if stop_word in description_lower:
                        found_detailed_stop_word = stop_word
                        break
            
            # Determine status
            if found_detailed_stop_word:
                new_status = 'stage2_failed'
                logger.info(f"  ✗ REJECTED {fb_id}: Detailed stop word '{found_detailed_stop_word}' found")
            else:
                # Parse ONLY from description (title can be incorrect/outdated)
                params = parser.parse(description)
                
                # Final criteria check with Stage 2 filters (kitchen required, bedrooms >= 2)
                criterias = config.get('criterias', {})
                passed, reason = parser.matches_criteria(params, criterias, stage=2)
                
                new_status = 'stage2' if passed else 'stage2_failed'
                logger.info(f"  Processing {fb_id}: Status '{new_status}'. Reason: {reason}")
            
            # Prepare details for DB update
            update_details = {
                'title': extracted_title,  # Extract title from description
                'phone_number': (parser.extract_phone_numbers(description) or [None])[0],
                'bedrooms': params.get('bedrooms') if 'params' in locals() else None,
                'price_extracted': params.get('price') if 'params' in locals() else None,
                'has_ac': params.get('has_ac', False) if 'params' in locals() else False,
                'has_wifi': params.get('has_wifi', False) if 'params' in locals() else False,
                'location_extracted': location_extracted,
                'status': new_status
            }
            
            # Update in database
            set_clause = ", ".join([f"{key} = %s" for key in update_details.keys()])
            query = f"UPDATE listings SET {set_clause} WHERE fb_id = %s"
            values = list(update_details.values())
            values.append(fb_id)
            
            db.cursor.execute(query, tuple(values))
            db.conn.commit()
            updated_count += 1
        
        # Process Groups without description
        logger.info(f"\nProcessing {len(groups_no_description)} Groups without description...")
        for listing in groups_no_description:
            processed_count += 1
            fb_id = listing['fb_id']
            new_status = 'no_description'
            logger.info(f"  ⚠ NO_DESC {fb_id}: Marked as 'no_description'")
            
            db.cursor.execute("UPDATE listings SET status = %s WHERE fb_id = %s", (new_status, fb_id))
            db.conn.commit()
            updated_count += 1
    
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 2 COMPLETE")
    logger.info(f"Processed: {processed_count}")
    logger.info(f"Updated in DB: {updated_count}")
    logger.info(f"  - Marketplace scraped: {len(stage2_listings)}")
    logger.info(f"  - Groups with description: {len(groups_with_description)}")
    logger.info(f"  - Groups without description (no_description): {len(groups_no_description)}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()