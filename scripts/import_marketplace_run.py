#!/usr/bin/env python3
"""
Import results from a specific Apify Marketplace run into the database.
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
from apify_client import ApifyClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Import results from specific Apify marketplace run"""
    
    if len(sys.argv) < 2:
        logger.error("Usage: python import_marketplace_run.py <RUN_ID>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    
    logger.info("=" * 80)
    logger.info(f"IMPORTING APIFY MARKETPLACE RUN: {run_id}")
    logger.info("=" * 80)
    
    load_dotenv()
    
    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    apify_key = os.getenv('APIFY_API_KEY')
    
    if not apify_key:
        logger.error("Missing APIFY_API_KEY!")
        sys.exit(1)
    
    # Fetch results from run using ApifyClient directly
    client = ApifyClient(apify_key)
    
    try:
        run = client.run(run_id).get()
        if not run:
            logger.error(f"Run {run_id} not found.")
            sys.exit(1)

        dataset_id = run['defaultDatasetId']
        logger.info(f"Found dataset {dataset_id} for run {run_id}")

        items = list(client.dataset(dataset_id).iterate_items())
        logger.info(f"✓ Fetched {len(items)} listings from dataset")
        
        # Normalize to our format
        all_listings = []
        for item in items:
            # Extract price
            price = ''
            if item.get('listing_price'):
                price = item['listing_price'].get('formatted_amount', '')
            
            # Extract location
            location = ''
            if item.get('location'):
                if isinstance(item['location'], dict):
                    location = item['location'].get('reverse_geocode', {}).get('city', '')
                else:
                    location = str(item['location'])
            
            normalized = {
                'fb_id': item.get('id', ''),
                'title': item.get('marketplace_listing_title', ''),
                'price': price,
                'location': location,
                'listing_url': item.get('listingUrl', ''),
                'description': ''  # Marketplace doesn't have description at stage1
            }
            if normalized['fb_id'] and normalized['listing_url']:
                all_listings.append(normalized)
        
        logger.info(f"✓ Normalized {len(all_listings)} listings")
        
    except Exception as e:
        logger.error(f"✗ Error fetching results: {e}", exc_info=True)
        sys.exit(1)
    
    if not all_listings:
        logger.warning("No listings found in run!")
        sys.exit(0)
    
    # Filter candidates
    logger.info("Filtering candidates by title criteria...")
    parser = PropertyParser(config)
    criterias = config.get('criterias', {})
    stop_words = config.get('filters', {}).get('stop_words', [])
    stop_locations = config.get('filters', {}).get('stop_locations', [])
    candidates = []
    
    for listing in all_listings:
        params = parser.parse(listing['title'])
        passed, reason = parser.matches_criteria(params, criterias)
        
        # Additional stop-word filtering in title
        if passed:
            title_lower = listing['title'].lower()
            for stop_word in stop_words:
                if stop_word.lower() in title_lower:
                    passed = False
                    reason = f"Stop word in title: {stop_word}"
                    break
        
        # Additional stop-location filtering in location field
        if passed and listing.get('location'):
            location_lower = listing['location'].lower()
            for stop_loc in stop_locations:
                if stop_loc.lower() in location_lower:
                    passed = False
                    reason = f"Stop location found: {stop_loc}"
                    break
        
        if passed:
            candidates.append(listing)
            logger.info(f"  ✓ CANDIDATE: {listing.get('fb_id', 'N/A')}")
        else:
            logger.debug(f"  ✗ FILTERED: {listing.get('fb_id', 'N/A')} - {reason}")
    
    pass_rate = (len(candidates) / len(all_listings) * 100) if all_listings else 0
    logger.info(f"✓ {len(candidates)}/{len(all_listings)} listings passed filters ({pass_rate:.1f}%)")
    
    # Save to database
    saved_count = 0
    if candidates:
        logger.info(f"Saving {len(candidates)} candidates to database...")
        with Database() as db:
            for candidate in candidates:
                try:
                    was_added = db.add_listing_from_stage1(
                        fb_id=candidate['fb_id'],
                        title=candidate['title'],
                        price=candidate.get('price', ''),
                        location=candidate.get('location', ''),
                        listing_url=candidate['listing_url'],
                        source='apify-marketplace',
                        description=candidate.get('description', '')
                    )
                    if was_added:
                        saved_count += 1
                except Exception as e:
                    logger.warning(f"Could not save candidate {candidate.get('fb_id', 'N/A')}: {e}")
        
        logger.info(f"✓ Saved {saved_count} new unique candidates to 'listings' table.")
    
    # Summary
    logger.info("=" * 80)
    logger.info("IMPORT COMPLETE")
    logger.info(f"Total listings fetched: {len(all_listings)}")
    logger.info(f"Candidates found: {len(candidates)}")
    logger.info(f"New candidates saved: {saved_count}")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()
