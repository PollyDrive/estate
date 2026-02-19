#!/usr/bin/env python3
"""
STAGE 2 (QFR, search-driven):
- Accepts a Marketplace search URL from CLI
- Scrapes detailed listings via actor qFR6mjgdwPouKLDvE
- Upserts found listings into DB with status='stage2'
"""

import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import Database
from facebook_marketplace_qfr_scraper import FacebookMarketplaceQfrScraper
from property_parser import PropertyParser
from telegram_notifier import TelegramNotifier


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/stage2_manual_qfr.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _load_config() -> dict:
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    if not config_path.exists():
        config_path = Path("/app/config/config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    logger.info("=" * 80)
    logger.info("STAGE 2 (QFR): Manual Full Detail Scraping")
    logger.info("=" * 80)

    load_dotenv()
    config = _load_config()

    db_url = os.getenv("DATABASE_URL")
    apify_key = os.getenv("APIFY_API_KEY")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    admin_id = os.getenv("TELEGRAM_ADMIN_ID")

    notifier = None
    if bot_token and (chat_id or admin_id):
        notifier = TelegramNotifier(bot_token, chat_id, config, admin_chat_id=admin_id)

    if not all([db_url, apify_key]):
        msg = "Missing required env vars: DATABASE_URL, APIFY_API_KEY"
        logger.error(msg)
        if notifier:
            notifier.send_error("Stage 2 QFR", msg)
        sys.exit(1)

    try:
        parser = PropertyParser()
        qfr_scraper = FacebookMarketplaceQfrScraper(apify_key, config)
        qfr_cfg = config.get("marketplace_qfr", {}) or {}

        # QFR actor is search-oriented in practice.
        # Read all runtime settings from marketplace_qfr config.
        start_urls = list(qfr_cfg.get("start_urls") or [])
        stage2_listings = []
        if start_urls:
            max_stage2 = int(qfr_cfg.get("max_stage2_items", 20))
            logger.info(
                "Scraping marketplace via QFR actor (maxListings=%s, startUrls=%s)...",
                max_stage2,
                len(start_urls),
            )
            stage2_listings = qfr_scraper.scrape_marketplace(start_urls, max_listings=max_stage2)
            logger.info("✓ Scraped %s marketplace listings", len(stage2_listings))
        else:
            logger.warning("No marketplace_qfr.start_urls configured")

        stop_words_detailed = config.get("filters", {}).get("stop_words_detailed", [])
        stop_words_detailed_lower = [w.lower() for w in stop_words_detailed]

        # Get stop locations for filtering
        stop_locations = config.get("filters", {}).get("stop_locations", [])
        stop_locations_lower = [loc.lower() for loc in stop_locations]

        processed_count = 0
        upserted_count = 0
        skipped_location_count = 0

        with Database() as db:
            # Upsert all listings found from this search URL as stage2.
            for listing in stage2_listings:
                fb_id = listing.get("fb_id")
                listing_url = listing.get("listing_url", "") or ""
                if not fb_id:
                    if "/marketplace/item/" in listing_url:
                        fb_id = listing_url.split("/marketplace/item/")[-1].split("?")[0].split("/")[0]
                        listing["fb_id"] = fb_id
                    if not listing_url:
                        logger.warning("Scraped listing missing fb_id and listing_url, skip")
                        continue
                if not fb_id:
                    logger.warning("Could not resolve fb_id for listing URL: %s", listing_url)
                    continue

                processed_count += 1

                # Check location against stop_locations FIRST (before saving to DB)
                location = listing.get("location", "") or ""
                title = listing.get("title", "") or ""
                description = listing.get("description", "") or ""

                # Skip if location matches stop_locations (e.g., USA, other countries)
                # Use word boundary matching to avoid false positives (e.g., "ny" in "Gianyar")
                # Empty locations and foreign languages will be handled by LLM filter
                location_rejected = False
                if location and stop_locations_lower:
                    import re
                    location_lower = location.lower()
                    for stop_loc in stop_locations_lower:
                        # For short codes (2-3 chars), require word boundaries or comma/space separation
                        if len(stop_loc) <= 3:
                            # Match "ny" in "Brooklyn, NY" but not in "Gianyar"
                            pattern = r'\b' + re.escape(stop_loc) + r'\b'
                            if re.search(pattern, location_lower):
                                logger.info(f"SKIP {fb_id}: Stop location '{stop_loc}' found in location: {location}")
                                skipped_location_count += 1
                                location_rejected = True
                                break
                        else:
                            # For longer names, simple substring match is OK
                            if stop_loc in location_lower:
                                logger.info(f"SKIP {fb_id}: Stop location '{stop_loc}' found in location: {location}")
                                skipped_location_count += 1
                                location_rejected = True
                                break

                if location_rejected:
                    continue  # Skip this listing entirely - don't save to DB

                found_stop = None
                if description and stop_words_detailed_lower:
                    low = description.lower()
                    for sw in stop_words_detailed_lower:
                        if sw in low:
                            found_stop = sw
                            break

                if found_stop:
                    reason = f"Detailed stop word: {found_stop}"
                    params = {}
                else:
                    params = parser.parse(description)
                    reason = "QFR detailed import from search URL"

                location_extracted = parser.extract_location(description) if description else None
                phones = parser.extract_phone_numbers(description) if description else []
                phone = phones[0] if phones else None

                # Do not overwrite listings that already progressed beyond stage2.
                upsert_query = """
                    INSERT INTO listings (
                        fb_id, title, description, price, location, listing_url, source,
                        phone_number, bedrooms, price_extracted, has_ac, has_wifi, has_pool, has_parking,
                        utilities, furniture, rental_term, location_extracted, status, pass_reason, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (fb_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        price = EXCLUDED.price,
                        location = EXCLUDED.location,
                        listing_url = EXCLUDED.listing_url,
                        source = EXCLUDED.source,
                        phone_number = EXCLUDED.phone_number,
                        bedrooms = EXCLUDED.bedrooms,
                        price_extracted = EXCLUDED.price_extracted,
                        has_ac = EXCLUDED.has_ac,
                        has_wifi = EXCLUDED.has_wifi,
                        has_pool = EXCLUDED.has_pool,
                        has_parking = EXCLUDED.has_parking,
                        utilities = EXCLUDED.utilities,
                        furniture = EXCLUDED.furniture,
                        rental_term = EXCLUDED.rental_term,
                        location_extracted = EXCLUDED.location_extracted,
                        status = EXCLUDED.status,
                        pass_reason = EXCLUDED.pass_reason,
                        updated_at = NOW()
                    WHERE listings.status IN ('stage1', 'stage1_new', 'stage2', 'stage2_failed', 'no_description')
                """
                db.cursor.execute(
                    upsert_query,
                    (
                        fb_id,
                        listing.get("title", ""),
                        description,
                        listing.get("price", ""),
                        listing.get("location", ""),
                        listing_url,
                        "apify-marketplace",
                        phone,
                        params.get("bedrooms"),
                        params.get("price"),
                        params.get("has_ac", False),
                        params.get("has_wifi", False),
                        params.get("has_pool", False),
                        params.get("has_parking", False),
                        params.get("utilities"),
                        params.get("furniture"),
                        params.get("rental_term"),
                        location_extracted,
                        "stage2",
                        reason,
                    ),
                )
                db.conn.commit()
                upserted_count += 1

        logger.info("=" * 80)
        logger.info("STAGE 2 (QFR) COMPLETE")
        logger.info("Processed: %s", processed_count)
        logger.info("Upserted in DB: %s", upserted_count)
        logger.info("Skipped (stop_locations): %s", skipped_location_count)
        logger.info("  - Marketplace scraped: %s", len(stage2_listings))
        logger.info("  - Search URLs: %s", start_urls)
        logger.info("=" * 80)

    except Exception as e:
        error_msg = f"Unexpected error in Stage 2 QFR: {e}"
        logger.error(error_msg, exc_info=True)
        if notifier:
            import traceback
            full_error = f"{error_msg}\n\n{traceback.format_exc()[-500:]}"
            notifier.send_error("Stage 2 QFR", full_error)
        sys.exit(1)


if __name__ == "__main__":
    main()

