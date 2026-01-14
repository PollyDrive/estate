#!/usr/bin/env python3
"""
Fetch data from last N Apify Facebook Groups Scraper runs and import to database.
"""

import os
import sys
import logging
import re
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from apify_client import ApifyClient
from database import Database, STATUS_STAGE1_NEW

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fetch_apify_groups_runs.log'),
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
    
    # Pattern: /posts/1234567890
    match = re.search(r'/posts/(\d+)', url)
    if match:
        return f"group_post_{match.group(1)}"
    
    return None


def main():
    """Fetch and import Facebook Groups data from last N Apify runs."""
    
    logger.info("=" * 80)
    logger.info("FETCH APIFY GROUPS RUNS: Fetching last 2 runs from Apify")
    logger.info("=" * 80)
    
    load_dotenv()
    
    # Get API key
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        logger.error("Missing APIFY_API_KEY environment variable!")
        sys.exit(1)
    
    client = ApifyClient(api_key)
    
    # Facebook Groups Scraper actor ID
    actor_id = 'msq85~facebook-groups-scraper'
    
    try:
        # Get last N runs
        num_runs = 2
        logger.info(f"Fetching last {num_runs} runs for actor: {actor_id}")
        
        runs = client.actor(actor_id).last_run()
        
        # Get list of runs
        runs_list = list(client.actor(actor_id).runs().list(limit=num_runs).items)
        
        if not runs_list:
            logger.warning("No runs found!")
            sys.exit(0)
        
        logger.info(f"Found {len(runs_list)} runs")
        
        total_saved = 0
        total_skipped = 0
        total_errors = 0
        
        # Process each run
        for idx, run in enumerate(runs_list, 1):
            run_id = run.get('id')
            status = run.get('status')
            started_at = run.get('startedAt')
            finished_at = run.get('finishedAt')
            default_dataset_id = run.get('defaultDatasetId')
            
            logger.info("-" * 80)
            logger.info(f"RUN {idx}/{len(runs_list)}")
            logger.info(f"Run ID: {run_id}")
            logger.info(f"Status: {status}")
            logger.info(f"Started: {started_at}")
            logger.info(f"Finished: {finished_at}")
            logger.info(f"Dataset ID: {default_dataset_id}")
            logger.info("-" * 80)
            
            if not default_dataset_id:
                logger.warning(f"No dataset found for run {run_id}, skipping")
                continue
            
            # Fetch items from dataset
            logger.info(f"Fetching items from dataset {default_dataset_id}...")
            items = list(client.dataset(default_dataset_id).iterate_items())
            logger.info(f"Fetched {len(items)} items from run {run_id}")
            
            if not items:
                logger.warning("No items found in dataset!")
                continue
            
            # Filter out errors
            valid_items = [item for item in items if 'error' not in item]
            error_items = [item for item in items if 'error' in item]
            
            logger.info(f"Valid items: {len(valid_items)}")
            logger.info(f"Error items: {len(error_items)}")
            
            if not valid_items:
                logger.warning("No valid items to import!")
                continue
            
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
                            title if title else '',
                            listing_url,
                            description,
                            STATUS_STAGE1_NEW,
                            'facebook_group',
                            group_title,
                            f'From Apify run {run_id} - Group: {group_title}'
                        ))
                        db.conn.commit()
                        
                        saved_count += 1
                        logger.info(f"✓ Saved: {fb_id} - {title[:60] if title else 'No title'}")
                        
                    except Exception as e:
                        logger.error(f"Error importing item: {e}")
                        logger.error(f"Item URL: {item.get('url', 'N/A')}")
                        error_count += 1
                        db.conn.rollback()
            
            logger.info(f"Run {run_id} complete:")
            logger.info(f"  Saved: {saved_count}")
            logger.info(f"  Skipped: {skipped_count}")
            logger.info(f"  Errors: {error_count}")
            
            total_saved += saved_count
            total_skipped += skipped_count
            total_errors += error_count
        
        logger.info("=" * 80)
        logger.info("ALL RUNS IMPORT COMPLETE")
        logger.info(f"Processed {len(runs_list)} runs")
        logger.info(f"Total saved to DB: {total_saved}")
        logger.info(f"Total skipped (duplicates): {total_skipped}")
        logger.info(f"Total errors: {total_errors}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error fetching from Apify: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
