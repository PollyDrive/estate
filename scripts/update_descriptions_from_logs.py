"""
Update listings descriptions from parsed Apify logs.
Re-parses bedrooms and price using ONLY description (without title).
"""

import json
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import Database
from property_parser import PropertyParser

def load_parsed_logs(filename='parsed_apify_logs.json'):
    """Load parsed logs and deduplicate by fb_id."""
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Deduplicate: take first occurrence of each fb_id
    fb_id_to_desc = {}
    for log_file, items in data.items():
        for item in items:
            fb_id = item['fb_id']
            description = item['description']
            
            # Take first occurrence (don't overwrite)
            if fb_id not in fb_id_to_desc:
                fb_id_to_desc[fb_id] = description
    
    return fb_id_to_desc

def update_database(fb_id_to_desc, dry_run=False):
    """Update database with new descriptions and re-parse properties."""
    
    db = Database()
    db.connect()
    conn = db.conn
    cur = conn.cursor()
    
    parser = PropertyParser()
    
    stats = {
        'total': len(fb_id_to_desc),
        'found_in_db': 0,
        'updated': 0,
        'bedrooms_changed': 0,
        'price_changed': 0,
        'not_found': 0,
    }
    
    changes_log = []
    
    print(f"Processing {stats['total']} unique fb_ids from logs...")
    print(f"Dry run: {dry_run}\n")
    
    for fb_id, new_description in fb_id_to_desc.items():
        # Get current data from DB
        cur.execute('''
            SELECT id, description, bedrooms, price_extracted, source
            FROM listings
            WHERE fb_id = %s
        ''', (fb_id,))
        
        result = cur.fetchone()
        
        if not result:
            stats['not_found'] += 1
            continue
        
        listing_id, old_description, old_bedrooms, old_price, source = result
        stats['found_in_db'] += 1
        
        # Parse new description to extract bedrooms and price
        parsed = parser.parse(new_description)
        new_bedrooms = parsed.get('bedrooms')
        new_price = parsed.get('price')
        
        # Track changes
        changed = False
        change_details = {
            'fb_id': fb_id,
            'source': source,
            'changes': []
        }
        
        if old_description != new_description:
            change_details['changes'].append('description updated')
            changed = True
        
        if new_bedrooms is not None and old_bedrooms != new_bedrooms:
            change_details['changes'].append(f'bedrooms: {old_bedrooms} → {new_bedrooms}')
            stats['bedrooms_changed'] += 1
            changed = True
        
        if new_price is not None and old_price != new_price:
            # Format old price safely (might be string like "IDR5,500,000")
            try:
                old_price_fmt = f'{float(old_price):,.0f}' if old_price else 'None'
            except (ValueError, TypeError):
                old_price_fmt = str(old_price)
            
            new_price_fmt = f'{float(new_price):,.0f}'
            change_details['changes'].append(f'price: {old_price_fmt} → {new_price_fmt}')
            stats['price_changed'] += 1
            changed = True
        
        if changed:
            changes_log.append(change_details)
            
            if not dry_run:
                # Update database
                cur.execute('''
                    UPDATE listings
                    SET description = %s,
                        bedrooms = %s,
                        price_extracted = %s,
                        updated_at = NOW()
                    WHERE id = %s
                ''', (new_description, new_bedrooms, new_price, listing_id))
                
                stats['updated'] += 1
    
    if not dry_run:
        conn.commit()
        print("✅ Changes committed to database")
    else:
        print("🔍 DRY RUN - No changes made to database")
    
    cur.close()
    conn.close()
    
    return stats, changes_log

def main():
    print("=" * 60)
    print("Update Descriptions from Apify Logs")
    print("=" * 60)
    print()
    
    # Check if parsed logs exist
    if not os.path.exists('parsed_apify_logs.json'):
        print("❌ Error: parsed_apify_logs.json not found")
        print("Run parse_apify_logs.py first!")
        return
    
    # Load data
    print("Loading parsed logs...")
    fb_id_to_desc = load_parsed_logs()
    print(f"Loaded {len(fb_id_to_desc)} unique fb_ids\n")
    
    # Ask for confirmation
    print("This will:")
    print("  1. Update 'description' field for matching fb_ids")
    print("  2. Re-parse 'bedrooms' and 'price_extracted' from description only")
    print("  3. Update these fields in the database")
    print()
    
    response = input("Do you want to see a DRY RUN first? (Y/n): ").strip().lower()
    
    if response != 'n':
        print("\n" + "=" * 60)
        print("DRY RUN - Analyzing changes")
        print("=" * 60 + "\n")
        
        stats, changes_log = update_database(fb_id_to_desc, dry_run=True)
        
        # Print statistics
        print("\n" + "=" * 60)
        print("Statistics")
        print("=" * 60)
        print(f"Total fb_ids from logs: {stats['total']}")
        print(f"Found in database: {stats['found_in_db']}")
        print(f"Not found in database: {stats['not_found']}")
        print(f"Would be updated: {stats['found_in_db']}")
        print(f"  - Bedrooms changed: {stats['bedrooms_changed']}")
        print(f"  - Price changed: {stats['price_changed']}")
        
        # Show sample changes
        if changes_log:
            print("\n" + "=" * 60)
            print("Sample Changes (first 10)")
            print("=" * 60)
            for change in changes_log[:10]:
                print(f"\nfb_id: {change['fb_id']} ({change['source']})")
                for detail in change['changes']:
                    print(f"  • {detail}")
        
        print("\n" + "=" * 60)
        response = input("\nProceed with actual update? (y/N): ").strip().lower()
        
        if response != 'y':
            print("❌ Cancelled by user")
            return
    
    # Actual update
    print("\n" + "=" * 60)
    print("UPDATING DATABASE")
    print("=" * 60 + "\n")
    
    stats, changes_log = update_database(fb_id_to_desc, dry_run=False)
    
    # Print final statistics
    print("\n" + "=" * 60)
    print("Final Statistics")
    print("=" * 60)
    print(f"Total fb_ids from logs: {stats['total']}")
    print(f"Found in database: {stats['found_in_db']}")
    print(f"Not found in database: {stats['not_found']}")
    print(f"Updated: {stats['updated']}")
    print(f"  - Bedrooms changed: {stats['bedrooms_changed']}")
    print(f"  - Price changed: {stats['price_changed']}")
    
    print("\n✅ Update completed successfully!")

if __name__ == '__main__':
    main()
