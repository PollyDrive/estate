#!/usr/bin/env python3
"""
RealtyBot-Bali: Main orchestrator for the multi-stage scraping and filtering process.
"""

import os
import json
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

from database import Database, STATUS_STAGE2_FILTERED, STATUS_STAGE2_REJECTED
from property_parser import PropertyParser
from telegram_notifier import TelegramNotifier
from apify_scraper import ApifyScraper
from facebook_marketplace_cheerio_scraper import FacebookMarketplaceCheerioScraper
from llm_filters import get_llm_filters


def setup_logging(config):
    """Setup logging configuration."""
    log_config = config.get('logging', {})
    log_file = log_config.get('file', 'logs/realty_bot.log')
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_config(config_path='config/config.json'):
    """Load configuration from JSON file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def run_stage1_scrape(config: dict, db: Database):
    """
    Stage 1: Scrape title-only listings from sources and save new ones to DB.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("PHASE 1: Starting Stage 1 (Initial Scrape)")
    logger.info("=" * 80)

    cheerio_enabled = config.get('marketplace_cheerio', {}).get('enabled', False)
    if not cheerio_enabled:
        logger.warning("Marketplace Cheerio scraping disabled, skipping Stage 1.")
        return

    apify_key = os.getenv('APIFY_API_KEY')
    if not apify_key:
        logger.error("APIFY_API_KEY not found, cannot run scraper.")
        return

    try:
        cheerio_scraper = FacebookMarketplaceCheerioScraper(apify_key, config)
        max_items = config.get('marketplace_cheerio', {}).get('max_items', 100)
        
        stage1_listings = cheerio_scraper.scrape_titles_only(max_items=max_items)
        logger.info(f"[STAGE 1] Scraped {len(stage1_listings)} raw listings.")

        new_listings_added = 0
        for listing in stage1_listings:
            # Here we can apply very basic title-only filters if needed before DB insert
            # For now, we add all unique listings to the DB for processing.
            # The user confirmed Apify/Cheerio does the initial title filtering.
            was_added = db.add_listing_from_stage1(
                fb_id=listing['fb_id'],
                title=listing['title'],
                price=listing.get('price', ''),
                location=listing.get('location', ''),
                listing_url=listing['listing_url']
            )
            if was_added:
                new_listings_added += 1
        
        logger.info(f"[STAGE 1] Added {new_listings_added} new unique listings to the database for processing.")

    except Exception as e:
        logger.error(f"Error during Stage 1 scraping: {e}", exc_info=True)


def run_stage2_details_scrape(config: dict, db: Database):
    """
    Stage 2: Scrape full details for new listings and apply simple filters.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("PHASE 2: Starting Stage 2 (Detailed Scrape & Simple Filters)")
    logger.info("=" * 80)

    listings_to_process = db.get_listings_for_stage2()
    if not listings_to_process:
        logger.info("[STAGE 2] No new listings to process from Stage 1.")
        return

    logger.info(f"[STAGE 2] Found {len(listings_to_process)} listings requiring full details.")
    
    apify_key = os.getenv('APIFY_API_KEY')
    if not apify_key:
        logger.error("APIFY_API_KEY not found, cannot run scraper for Stage 2.")
        return

    parser = PropertyParser()
    cheerio_scraper = FacebookMarketplaceCheerioScraper(apify_key, config)
    
    candidate_urls = [listing['listing_url'] for listing in listings_to_process]
    max_stage2 = config.get('marketplace_cheerio', {}).get('max_stage2_items', 50)

    try:
        full_detail_listings = cheerio_scraper.scrape_full_details(candidate_urls, max_stage2_items=max_stage2)
        logger.info(f"[STAGE 2] Scraped {len(full_detail_listings)} full-detail listings.")

        for listing_details in full_detail_listings:
            fb_id = listing_details.get('fb_id')
            if not fb_id:
                continue

            # Parse ONLY from description (title can be incorrect/outdated)
            description = listing_details.get('description', '')
            params = parser.parse(description)
            
            criterias = config.get('criterias', {})
            passed, reason = parser.matches_criteria(params, criterias, stage=2)
            
            logger.info(f"[STAGE 2] Processing {fb_id}: Passed Stage 2 filters: {passed}. Reason: {reason}")

            # Extract location
            location_extracted = parser.extract_location(description) if description else None
            
            # Prepare details for DB update
            update_details = {
                'description': description,
                'phone_number': (parser.extract_phone_numbers(description) or [None])[0],
                'bedrooms': params.get('bedrooms'),
                'price_extracted': params.get('price'),
                'kitchen_type': params.get('kitchen_type'),
                'has_ac': params.get('has_ac', False),
                'has_wifi': params.get('has_wifi', False),
                'has_pool': params.get('has_pool', False),
                'has_parking': params.get('has_parking', False),
                'utilities': params.get('utilities'),
                'furniture': params.get('furniture'),
                'rental_term': params.get('rental_term'),
                'location_extracted': location_extracted
            }
            
            db.update_listing_after_stage2(fb_id, update_details, passed)

    except Exception as e:
        logger.error(f"Error during Stage 2 processing: {e}", exc_info=True)


def run_stage3_llm_analysis(config: dict, db: Database):
    """
    Stage 3: Run LLM analysis on listings that passed simple filters.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("PHASE 3: Starting Stage 3 (LLM Analysis)")
    logger.info("=" * 80)

    level1_filter, _ = get_llm_filters(config) # We only use Groq for now
    if not level1_filter:
        logger.warning("LLM filters are not enabled or configured. Skipping Stage 3.")
        return

    listings_to_analyze = db.get_listings_for_stage3()
    if not listings_to_analyze:
        logger.info("[STAGE 3] No new listings to analyze from Stage 2.")
        return
        
    logger.info(f"[STAGE 3] Found {len(listings_to_analyze)} listings for LLM analysis.")

    for listing in listings_to_analyze:
        fb_id = listing['fb_id']
        description = listing.get('description', '')
        
        if not description:
            logger.warning(f"[STAGE 3] Listing {fb_id} has no description, cannot analyze. Marking as failed.")
            db.update_listing_after_stage3(fb_id, False, "Missing description")
            continue

        logger.info(f"[STAGE 3] Analyzing {fb_id} with Groq...")
        passed, reason = level1_filter.filter(description)
        
        logger.info(f"[STAGE 3] Analysis for {fb_id}: Passed: {passed}. Reason: {reason}")
        db.update_listing_after_stage3(fb_id, passed, reason)


def run_telegram_notifications(config: dict, db: Database, telegram: TelegramNotifier):
    """
    Final Phase: Send notifications for listings that passed all filters.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("PHASE 4: Sending Telegram Notifications")
    logger.info("=" * 80)

    listings_to_send = db.get_listings_for_telegram()
    if not listings_to_send:
        logger.info("No new listings to notify about.")
        return

    logger.info(f"Found {len(listings_to_send)} new listings to send to Telegram.")
    sent_count = 0
    for listing in listings_to_send:
        fb_id = listing['fb_id']
        
        # Re-create a human-readable summary for the message
        summary_ru = listing.get('llm_reason', 'Подходящий вариант') # Use LLM reason as a summary
        price_display = listing.get('price', '')
        if listing.get('price_extracted'):
            price_display = f"Rp {listing['price_extracted']:,.0f}"

        success = telegram.send_notification(
            summary_ru=summary_ru,
            price=price_display,
            phone=listing.get('phone_number') or 'N/A',
            url=listing.get('listing_url', '')
        )
        
        if success:
            logger.info(f"Successfully sent notification for {fb_id}.")
            db.mark_listing_sent(fb_id)
            sent_count += 1
        else:
            logger.error(f"Failed to send notification for {fb_id}.")
    
    logger.info(f"Sent {sent_count}/{len(listings_to_send)} notifications.")


def main():
    """Main execution function to run all stages."""
    load_dotenv()
    config = load_config()
    setup_logging(config)
    
    logger = logging.getLogger(__name__)
    logger.info("RealtyBot-Bali orchestrator started")
    
    db_url = os.getenv('DATABASE_URL')
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not all([db_url, telegram_token, telegram_chat_id]):
        logger.error("Missing required environment variables (DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)!")
        sys.exit(1)

    # Initialize notifier once
    telegram = TelegramNotifier(telegram_token, telegram_chat_id, config)

    # Use a single DB connection for the whole run
    with Database(db_url) as db:
        run_stage1_scrape(config, db)
        run_stage2_details_scrape(config, db)
        run_stage3_llm_analysis(config, db)
        run_telegram_notifications(config, db, telegram)

    logger.info("RealtyBot-Bali run finished.")


if __name__ == '__main__':
    main()