#!/usr/bin/env python3
"""
Analyze why listings were filtered out.
Shows title-level filtering decisions for database listings.
"""

import json
import os
import sys
from dotenv import load_dotenv
from src.property_parser import PropertyParser
from src.database import Database

# Load environment
load_dotenv()

def analyze_filtered_listings(limit=30):
    """Analyze filtered listings from database."""
    
    parser = PropertyParser()
    
    # Load criteria
    with open('config/config.json', 'r') as f:
        config = json.load(f)
    criterias = config.get('criterias', {})
    
    # Connect to database
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL not set in environment")
        sys.exit(1)
    
    with Database(db_url) as db:
        # Get filtered listings
        query = """
            SELECT id, title, bedrooms, price_extracted, has_ac, has_wifi, rental_term
            FROM fb_listings 
            WHERE sent_to_telegram = false 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        db.cursor.execute(query, (limit,))
        listings = db.cursor.fetchall()
    
    print("=" * 100)
    print(f"АНАЛИЗ ОТФИЛЬТРОВАННЫХ ОБЪЯВЛЕНИЙ (последние {limit})")
    print("=" * 100)
    
    # Count reasons
    reasons_count = {}
    
    for i, listing in enumerate(listings, 1):
        listing_id, title, db_bedrooms, db_price, db_ac, db_wifi, db_term = listing
        
        print(f"\n[{i}/{len(listings)}] ID: {listing_id}")
        print(f"Заголовок: {title}")
        
        # Parse title
        params = parser.parse(title)
        
        print(f"  Распознано:")
        print(f"    - bedrooms: {params.get('bedrooms')}")
        print(f"    - price: {params.get('price')}")
        print(f"    - has_kitchen: {params.get('has_kitchen')}")
        print(f"    - has_ac: {params.get('has_ac')}")
        print(f"    - has_wifi: {params.get('has_wifi')}")
        print(f"    - rental_term: {params.get('rental_term')}")
        print(f"    - has_stop_word: {params.get('has_stop_word')}")
        
        # Check criteria
        passed, reason = parser.matches_criteria(params, criterias)
        
        if passed:
            print(f"  ⚠️  СТРАННО: Должен был пройти! Причина: {reason}")
        else:
            print(f"  ✗ ОТФИЛЬТРОВАН: {reason}")
            
            # Count reasons
            reasons_count[reason] = reasons_count.get(reason, 0) + 1
    
    # Summary
    print("\n" + "=" * 100)
    print("СТАТИСТИКА ПО ПРИЧИНАМ ФИЛЬТРАЦИИ:")
    print("=" * 100)
    
    for reason, count in sorted(reasons_count.items(), key=lambda x: x[1], reverse=True):
        print(f"  {count:2d}x - {reason}")
    
    print("\n" + "=" * 100)
    print(f"Всего проанализировано: {len(listings)}")
    print("=" * 100)


if __name__ == '__main__':
    analyze_filtered_listings(30)
