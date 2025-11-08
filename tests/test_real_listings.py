#!/usr/bin/env python3
"""
Test PropertyParser with REAL listings from database.
Fetches listings that were NOT sent to Telegram (filtered out).
"""

import os
import sys
import json
from dotenv import load_dotenv
from src.database import Database
from src.property_parser import PropertyParser

# Load environment
load_dotenv()

# Get DB connection
db_url = os.getenv('DATABASE_URL')
if not db_url:
    print("ERROR: DATABASE_URL not found in .env")
    sys.exit(1)

# Load criteria
with open('config/config.json', 'r') as f:
    config = json.load(f)
    criterias = config['criterias']

# Initialize parser
parser = PropertyParser()

print("=" * 80)
print("TESTING REAL LISTINGS FROM DATABASE")
print("=" * 80)
print()

try:
    with Database(db_url) as db:
        # Get 5 listings that were NOT sent to Telegram (were filtered out)
        query = """
            SELECT fb_id, title, description, price, location, sent_to_telegram, source
            FROM listings
            WHERE sent_to_telegram = FALSE
            ORDER BY created_at DESC
            LIMIT 5
        """
        
        cursor = db.conn.cursor()
        cursor.execute(query)
        listings = cursor.fetchall()
        
        if not listings:
            print("⚠️  No filtered listings found in database.")
            print()
            print("Options:")
            print("1. Run scraper first: python src/main.py")
            print("2. Or use test data: python test_filters.py")
            sys.exit(0)
        
        print(f"Found {len(listings)} filtered listings. Analyzing why they were rejected:")
        print()
        
        for idx, listing in enumerate(listings, 1):
            fb_id, title, description, price, location, sent, source = listing
            
            print(f"LISTING #{idx}")
            print("-" * 80)
            print(f"Source: {source}")
            print(f"Title: {title[:100]}")
            print(f"Location: {location}")
            print(f"Listed Price: {price}")
            print()
            
            # Parse full text
            full_text = f"{title} {description}"
            params = parser.parse(full_text)
            phones = parser.extract_phone_numbers(full_text)
            
            print("PARSED PARAMETERS:")
            print(f"  Bedrooms: {params.get('bedrooms')}")
            print(f"  Price: {params.get('price'):,.0f} IDR" if params.get('price') else "  Price: Not extracted")
            print(f"  Kitchen: {params.get('kitchen_type')}")
            print(f"  AC: {params.get('has_ac')}")
            print(f"  WiFi: {params.get('has_wifi')}")
            print(f"  Term: {params.get('rental_term')}")
            print(f"  Phone: {phones[0] if phones else 'None'}")
            print()
            
            # Check why it was filtered
            passed, reason = parser.matches_criteria(params, criterias)
            
            if passed:
                print("⚠️  UNEXPECTED: This listing PASSED filters!")
                print("    But it was marked as sent_to_telegram=FALSE in DB")
                print("    Possible reasons:")
                print("    - Telegram send failed")
                print("    - Criteria changed since then")
            else:
                print(f"✅ CORRECTLY FILTERED OUT")
                print(f"   Reason: {reason}")
            
            print()
            print(f"Description preview:")
            print(f"{description[:200]}...")
            print()
            print("=" * 80)
            print()
        
        # Also get 3 listings that PASSED (were sent to Telegram)
        print()
        print("=" * 80)
        print("LISTINGS THAT PASSED FILTERS (sent to Telegram):")
        print("=" * 80)
        print()
        
        query_passed = """
            SELECT fb_id, title, description, price, location, source
            FROM listings
            WHERE sent_to_telegram = TRUE
            ORDER BY created_at DESC
            LIMIT 3
        """
        
        cursor.execute(query_passed)
        passed_listings = cursor.fetchall()
        
        if not passed_listings:
            print("⚠️  No passed listings found in database.")
            print()
        else:
            for idx, listing in enumerate(passed_listings, 1):
                fb_id, title, description, price, location, source = listing
                
                print(f"PASSED LISTING #{idx}")
                print("-" * 80)
                print(f"Source: {source}")
                print(f"Title: {title[:100]}")
                print()
                
                full_text = f"{title} {description}"
                params = parser.parse(full_text)
                
                print("PARSED PARAMETERS:")
                print(f"  Bedrooms: {params.get('bedrooms')}")
                print(f"  Price: {params.get('price'):,.0f} IDR" if params.get('price') else "  Price: Not extracted")
                print(f"  Kitchen: {params.get('kitchen_type')}")
                print(f"  AC: {params.get('has_ac')}")
                print(f"  WiFi: {params.get('has_wifi')}")
                print()
                
                passed, reason = parser.matches_criteria(params, criterias)
                
                if passed:
                    print(f"✅ PASSED: {reason}")
                else:
                    print(f"⚠️  UNEXPECTED: Should have passed but got: {reason}")
                
                print()
                print("=" * 80)
                print()
        
        cursor.close()

except Exception as e:
    print(f"ERROR: {e}")
    print()
    print("Make sure:")
    print("1. Database is running")
    print("2. DATABASE_URL in .env is correct")
    print("3. Listings table exists (run migration)")
    sys.exit(1)

print()
print("SUMMARY")
print("=" * 80)
print("This shows real examples from your database and explains")
print("why each listing was accepted or rejected by filters.")
print()
