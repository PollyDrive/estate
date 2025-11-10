#!/usr/bin/env python3
"""
Import Facebook Groups data from JSON file to database.
"""

import os
import sys
import logging
import json
import re
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database, STATUS_STAGE1_NEW

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/import_facebook_groups.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def extract_fb_id_from_link(link):
    """Extract Facebook marketplace item ID from link."""
    if not link:
        return None
    
    # Pattern: /item/1234567890/
    match = re.search(r'/item/(\d+)', link)
    if match:
        return match.group(1)
    
    return None


def extract_fb_id_from_url(url):
    """Extract Facebook post ID from group post URL."""
    if not url:
        return None
    
    # Pattern: /permalink/1234567890/
    match = re.search(r'/permalink/(\d+)', url)
    if match:
        return f"group_post_{match.group(1)}"
    
    return None


def main():
    """Import Facebook Groups data from JSON file."""
    
    logger.info("=" * 80)
    logger.info("FACEBOOK GROUPS IMPORT: Loading data from JSON file")
    logger.info("=" * 80)
    
    load_dotenv()
    
    json_file = 'Facebook Groups Scraper Nov 9 2025.json'
    
    if not os.path.exists(json_file):
        logger.error(f"File not found: {json_file}")
        sys.exit(1)
    
    # Load JSON data
    logger.info(f"Loading data from {json_file}...")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    logger.info(f"Loaded {len(data)} items from JSON")
    
    # Filter out errors
    valid_items = [item for item in data if 'error' not in item]
    error_items = [item for item in data if 'error' in item]
    
    logger.info(f"Valid items: {len(valid_items)}")
    logger.info(f"Error items: {len(error_items)}")
    
    if not valid_items:
        logger.warning("No valid items to import!")
        sys.exit(0)
    
    # Import to database
    saved_count = 0
    skipped_count = 0
    error_count = 0
    
    with Database() as db:
        for item in valid_items:
            try:
                # Extract data
                title = item.get('title', '')
                text = item.get('text', '')
                link = item.get('link', '')
                url = item.get('url', '')
                group_title = item.get('groupTitle', '')
                user = item.get('user', {})
                user_name = user.get('name', '') if isinstance(user, dict) else ''
                
                # Try to get fb_id from marketplace link
                fb_id = extract_fb_id_from_link(link)
                
                # If no marketplace link, use post ID
                if not fb_id:
                    fb_id = extract_fb_id_from_url(url)
                
                if not fb_id:
                    logger.warning(f"Could not extract fb_id from: {url}")
                    skipped_count += 1
                    continue
                
                # Use marketplace link as listing_url, fallback to group post URL
                listing_url = link if link else url
                
                # Combine title and text as description
                description_parts = []
                if title:
                    description_parts.append(title)
                if text:
                    description_parts.append(text)
                description = '\n'.join(description_parts)
                
                # Check if listing already exists
                db.cursor.execute(
                    "SELECT fb_id FROM listings WHERE fb_id = %s",
                    (fb_id,)
                )
                existing = db.cursor.fetchone()
                
                if existing:
                    logger.debug(f"Listing {fb_id} already exists, skipping")
                    skipped_count += 1
                    continue
                
                # Insert new listing
                db.cursor.execute("""
                    INSERT INTO listings (
                        fb_id, title, listing_url, description, 
                        status, source, group_id, pass_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    fb_id,
                    title,
                    listing_url,
                    description,
                    STATUS_STAGE1_NEW,
                    'facebook_group',
                    group_title,
                    f'From Facebook Group: {group_title} (Posted by: {user_name})'
                ))
                db.conn.commit()
                
                saved_count += 1
                logger.info(f"✓ Saved: {fb_id} - {title[:60] if title else 'No title'}")
                
            except Exception as e:
                logger.error(f"Error importing item: {e}")
                logger.error(f"Item: {item.get('url', 'N/A')}")
                error_count += 1
                db.conn.rollback()
    
    logger.info("=" * 80)
    logger.info("FACEBOOK GROUPS IMPORT COMPLETE")
    logger.info(f"Total items in file: {len(data)}")
    logger.info(f"Valid items: {len(valid_items)}")
    logger.info(f"Saved to DB: {saved_count}")
    logger.info(f"Skipped (duplicates): {skipped_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
