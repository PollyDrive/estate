"""
Mark listings with old descriptions as stage1_new for reprocessing.
These are listings that have descriptions but were NOT updated from Apify logs.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import Database

def main():
    print("=" * 60)
    print("Mark listings with old descriptions as stage1_new")
    print("=" * 60)
    print()
    
    # Load fb_ids that were updated from logs
    if not os.path.exists('parsed_apify_logs.json'):
        print("❌ parsed_apify_logs.json not found")
        return
    
    with open('parsed_apify_logs.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Get unique fb_ids that were updated
    updated_fb_ids = set()
    for items in data.values():
        for item in items:
            updated_fb_ids.add(item['fb_id'])
    
    print(f"FB IDs updated from logs: {len(updated_fb_ids)}")
    
    # Connect to database
    db = Database()
    db.connect()
    cur = db.conn.cursor()
    
    # Find listings with descriptions that were NOT updated
    cur.execute('''
        SELECT fb_id, status, title, LEFT(description, 80) as desc_preview
        FROM listings
        WHERE description IS NOT NULL 
          AND description != ''
          AND status IN ('stage1', 'stage2', 'stage2_failed')
          AND source = 'apify-marketplace'
    ''')
    
    all_with_desc = cur.fetchall()
    print(f"Total listings with descriptions: {len(all_with_desc)}")
    
    # Filter out those that were updated
    to_mark = []
    for row in all_with_desc:
        fb_id = row[0]
        if fb_id not in updated_fb_ids:
            to_mark.append(row)
    
    print(f"Listings with OLD descriptions (not updated): {len(to_mark)}")
    print()
    
    if not to_mark:
        print("✅ No listings to mark - all are up to date!")
        cur.close()
        db.conn.close()
        return
    
    # Show samples
    print("Sample listings to be marked as stage1_new:")
    for row in to_mark[:5]:
        fb_id, status, title, desc_preview = row
        print(f"  {fb_id} ({status}): {title}")
        print(f"    desc: {desc_preview}...")
    
    if len(to_mark) > 5:
        print(f"  ... and {len(to_mark) - 5} more")
    
    print()
    response = input(f"Mark {len(to_mark)} listings as stage1_new for reprocessing? (y/N): ").strip().lower()
    
    if response != 'y':
        print("❌ Cancelled")
        cur.close()
        db.conn.close()
        return
    
    # Update status to stage1_new
    fb_ids_to_update = [row[0] for row in to_mark]
    
    cur.execute('''
        UPDATE listings
        SET status = 'stage1_new'
        WHERE fb_id = ANY(%s)
    ''', (fb_ids_to_update,))
    
    db.conn.commit()
    updated_count = cur.rowcount
    
    print(f"\n✅ Updated {updated_count} listings to stage1_new")
    
    # Show final stats by status
    print("\nFinal statistics (marketplace):")
    cur.execute('''
        SELECT status, COUNT(*)
        FROM listings
        WHERE source = 'apify-marketplace'
        GROUP BY status
        ORDER BY status
    ''')
    
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    
    cur.close()
    db.conn.close()

if __name__ == '__main__':
    main()
