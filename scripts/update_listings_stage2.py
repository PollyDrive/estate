#!/usr/bin/env python3
"""
Update existing fb_listings with Stage 2 data (full details including description).
Takes 20 listings from fb_listings, runs Stage 2, and UPDATES them (not insert new).
"""

import os
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser
from facebook_marketplace_cheerio_scraper import FacebookMarketplaceCheerioScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def main():
    """Run Stage 2 on existing listings and UPDATE them."""
    
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not db_url or not apify_key:
        logger.error("Missing DATABASE_URL or APIFY_API_KEY")
        sys.exit(1)
    
    config_path = Path(__file__).parent.parent / 'config' / 'config.json'
    if not config_path.exists():
        config_path = Path('/app/config/config.json')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info("=" * 80)
    logger.info("Stage 2: Updating 20 existing listings with full details")
    logger.info("=" * 80)
    
    # Get 20 listings with 2BR from database (most likely to be relevant)
    with Database(db_url) as db:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT fb_id, listing_url, title
            FROM fb_listings 
            WHERE listing_url IS NOT NULL 
            AND listing_url != ''
            AND listing_url LIKE '%facebook.com/marketplace/item/%'
            AND (bedrooms = 2 OR title ~* '2\s*(bed|BR|KT)')
            AND (description IS NULL OR description = '')
            ORDER BY created_at DESC
            LIMIT 20
        """)
        listings = cursor.fetchall()
        cursor.close()
    
    if not listings:
        logger.error("No suitable listings found!")
        return
    
    logger.info(f"Found {len(listings)} listings to update")
    
    # Extract fb_id -> URL mapping
    fb_id_to_url = {}
    urls = []
    for row in listings:
        fb_id, url, title = row
        fb_id_to_url[url] = fb_id
        urls.append(url)
        logger.info(f"  [{len(urls)}] {fb_id}: {title[:40]}...")
    
    # Initialize scraper
    scraper = FacebookMarketplaceCheerioScraper(apify_key, config)
    parser = PropertyParser()
    criterias = config.get('criterias', {})
    
    # Run Stage 2
    logger.info("\n" + "=" * 80)
    logger.info(f"Running Stage 2 for {len(urls)} URLs")
    logger.info("=" * 80)
    
    try:
        stage2_listings = scraper.scrape_full_details(urls, max_stage2_items=20)
        logger.info(f"\nStage 2 completed: {len(stage2_listings)} listings extracted")
        
        if not stage2_listings:
            logger.warning("No listings returned from actor!")
            return
        
        # Update existing listings in database
        updated = 0
        with Database(db_url) as db:
            cursor = db.conn.cursor()
            
            for listing in stage2_listings:
                listing_url = listing.get('listing_url')
                original_fb_id = fb_id_to_url.get(listing_url)
                
                if not original_fb_id:
                    logger.warning(f"Could not find original fb_id for URL: {listing_url}")
                    continue
                
                title = listing.get('title', '')
                description = listing.get('description', '')
                
                logger.info(f"\n[{updated+1}/{len(stage2_listings)}] Updating: {original_fb_id}")
                logger.info(f"  Title: {title[:50]}...")
                logger.info(f"  Description: {len(description)} chars")
                
                # Parse full text for updated parameters
                full_text = f"{title} {description}"
                params = parser.parse(full_text)
                phones = parser.extract_phone_numbers(full_text)
                phone = phones[0] if phones else None
                
                logger.info(f"  BR={params.get('bedrooms')}, "
                          f"AC={params.get('has_ac')}, WiFi={params.get('has_wifi')}, "
                          f"Kitchen={params.get('kitchen_type')}")
                
                # Update the listing (keeping original fb_id, just updating fields)
                cursor.execute("""
                    UPDATE fb_listings SET
                        title = %s,
                        description = %s,
                        price = %s,
                        location = %s,
                        phone_number = COALESCE(%s, phone_number),
                        bedrooms = COALESCE(%s, bedrooms),
                        price_extracted = COALESCE(%s, price_extracted),
                        kitchen_type = COALESCE(%s, kitchen_type),
                        has_ac = %s,
                        has_wifi = %s,
                        has_pool = %s,
                        has_parking = %s,
                        utilities = COALESCE(%s, utilities),
                        furniture = COALESCE(%s, furniture),
                        rental_term = COALESCE(%s, rental_term),
                        all_images = %s
                    WHERE fb_id = %s
                """, (
                    title[:500] if title else None,
                    description,
                    listing.get('price', ''),
                    listing.get('location', ''),
                    phone,
                    params.get('bedrooms'),
                    params.get('price'),
                    params.get('kitchen_type'),
                    params.get('has_ac', False),
                    params.get('has_wifi', False),
                    params.get('has_pool', False),
                    params.get('has_parking', False),
                    params.get('utilities'),
                    params.get('furniture'),
                    params.get('rental_term'),
                    json.dumps(listing.get('all_images', [])),
                    original_fb_id
                ))
                
                db.conn.commit()
                updated += 1
                logger.info(f"  ✓ UPDATED")
            
            cursor.close()
        
        logger.info("\n" + "=" * 80)
        logger.info(f"COMPLETE: Updated {updated}/{len(stage2_listings)} listings")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)


if __name__ == '__main__':
    main()
