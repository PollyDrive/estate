#!/usr/bin/env python3
"""
RealtyBot-Bali: Automated Facebook Marketplace scraper with multi-level filtering.
"""

import os
import json
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

from database import Database
from filters import Level0Filter
from llm_filters import Level1Filter, Level2Filter
from telegram_notifier import TelegramNotifier
from apify_scraper import ApifyScraper


def setup_logging(config):
    """Setup logging configuration."""
    log_config = config['logging']
    log_dir = Path(log_config['file']).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_config['level']),
        format=log_config['format'],
        handlers=[
            logging.FileHandler(log_config['file']),
            logging.StreamHandler(sys.stdout)
        ]
    )


def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    """Main execution function."""
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config()
    setup_logging(config)
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("RealtyBot-Bali started")
    logger.info("=" * 80)
    
    # Get credentials from environment
    db_url = os.getenv('DATABASE_URL')
    apify_key = os.getenv('APIFY_API_KEY')
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    groq_key = os.getenv('GROQ_API_KEY')
    anthropic_key = os.getenv('ANTHROPIC_API_KEY')
    
    # Validate credentials
    if not all([db_url, apify_key, telegram_token, telegram_chat_id, groq_key, anthropic_key]):
        logger.error("Missing required environment variables!")
        sys.exit(1)
    
    # Initialize components
    logger.info("Initializing components...")
    scraper = ApifyScraper(apify_key, config)
    level0_filter = Level0Filter(config)
    level1_filter = Level1Filter(config, groq_key)
    level2_filter = Level2Filter(config, anthropic_key)
    telegram = TelegramNotifier(telegram_token, telegram_chat_id, config)
    
    # Scrape listings
    logger.info("Starting scraping process...")
    raw_listings = scraper.scrape_listings()
    logger.info(f"Scraped {len(raw_listings)} raw listings")
    
    if not raw_listings:
        logger.warning("No listings found, exiting")
        return
    
    # Process listings
    processed_count = 0
    new_count = 0
    sent_count = 0
    
    with Database(db_url) as db:
        for raw_listing in raw_listings:
            processed_count += 1
            
            # Normalize listing
            listing = scraper.normalize_listing(raw_listing)
            fb_id = listing['fb_id']
            
            if not fb_id:
                logger.warning(f"Listing has no ID, skipping")
                continue
            
            logger.info(f"Processing listing {processed_count}/{len(raw_listings)}: {fb_id}")
            
            # Check if listing already exists
            if db.listing_exists(fb_id):
                logger.info(f"Listing {fb_id} already exists, skipping")
                continue
            
            new_count += 1
            logger.info(f"New listing found: {fb_id}")
            
            # Level 0: Hard filters
            passed_l0, phone_number, reason = level0_filter.filter(
                listing['title'],
                listing['price'],
                listing['description']
            )
            
            if not passed_l0:
                logger.info(f"Level 0 FAILED: {reason}")
                db.insert_listing(
                    fb_id=fb_id,
                    title=listing['title'],
                    price=listing['price'],
                    location=listing['location'],
                    listing_url=listing['listing_url'],
                    description=listing['description'],
                    phone_number=phone_number,
                    sent_to_telegram=False
                )
                continue
            
            logger.info(f"Level 0 PASSED")
            
            # Level 1: Groq kitchen check
            passed_l1, reason = level1_filter.filter(listing['description'])
            
            if not passed_l1:
                logger.info(f"Level 1 FAILED: {reason}")
                db.insert_listing(
                    fb_id=fb_id,
                    title=listing['title'],
                    price=listing['price'],
                    location=listing['location'],
                    listing_url=listing['listing_url'],
                    description=listing['description'],
                    phone_number=phone_number,
                    sent_to_telegram=False
                )
                continue
            
            logger.info(f"Level 1 PASSED")
            
            # Level 2: Claude analysis
            passed_l2, analysis_data, reason = level2_filter.filter(
                listing['title'],
                listing['price'],
                listing['description']
            )
            
            if not passed_l2:
                logger.info(f"Level 2 FAILED: {reason}")
                db.insert_listing(
                    fb_id=fb_id,
                    title=listing['title'],
                    price=listing['price'],
                    location=listing['location'],
                    listing_url=listing['listing_url'],
                    description=listing['description'],
                    phone_number=phone_number,
                    sent_to_telegram=False
                )
                continue
            
            logger.info(f"Level 2 PASSED")
            
            # Send Telegram notification
            success = telegram.send_notification(
                summary_ru=analysis_data['summary_ru'],
                price=listing['price'],
                phone=phone_number,
                url=listing['listing_url'],
                msg_en=analysis_data['msg_en'],
                msg_id=analysis_data['msg_id']
            )
            
            if success:
                sent_count += 1
                logger.info(f"Telegram notification sent successfully")
            else:
                logger.error(f"Failed to send Telegram notification")
            
            # Save to database
            db.insert_listing(
                fb_id=fb_id,
                title=listing['title'],
                price=listing['price'],
                location=listing['location'],
                listing_url=listing['listing_url'],
                description=listing['description'],
                phone_number=phone_number,
                sent_to_telegram=success
            )
    
    # Summary
    logger.info("=" * 80)
    logger.info(f"Processing complete!")
    logger.info(f"Total listings processed: {processed_count}")
    logger.info(f"New listings: {new_count}")
    logger.info(f"Notifications sent: {sent_count}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
