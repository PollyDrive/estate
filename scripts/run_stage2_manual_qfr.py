#!/usr/bin/env python3
"""
STAGE 2 (QFR, search-driven):
- Scrapes detailed listings via actor qFR6mjgdwPouKLDvE
- Upserts found listings into DB with status='stage2'

Usage:
  python scripts/run_stage2_manual_qfr.py --chat <chat_id>
    → scrape using qfr_start_urls for the given chat profile

  python scripts/run_stage2_manual_qfr.py
    → run for ALL enabled chat profiles sequentially
"""

import argparse
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
from config_loader import load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/stage2_manual_qfr.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _scrape_and_upsert(
    start_urls: list,
    profile_name: str,
    qfr_scraper,
    parser,
    db: Database,
    config: dict,
    qfr_cfg: dict,
) -> tuple:
    """Scrape start_urls and upsert into DB. Returns (processed, upserted, skipped_location)."""
    import re as _re

    stop_words          = config.get("filters", {}).get("stop_words", [])
    stop_words_lower    = [w.lower() for w in stop_words]
    stop_words_detailed = config.get("filters", {}).get("stop_words_detailed", [])
    stop_words_detailed_lower = [w.lower() for w in stop_words_detailed]
    stop_locations      = config.get("filters", {}).get("stop_locations", [])  # keep original case for _re

    max_stage2 = int(qfr_cfg.get("max_stage2_items", 20))
    logger.info(
        "[%s] Scraping via QFR actor (maxListings=%s, startUrls=%s)...",
        profile_name, max_stage2, len(start_urls),
    )
    stage2_listings = qfr_scraper.scrape_marketplace(start_urls, max_listings=max_stage2)
    logger.info("[%s] ✓ Scraped %s marketplace listings", profile_name, len(stage2_listings))

    processed_count = 0
    upserted_count = 0
    skipped_location_count = 0

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

        location = listing.get("location", "") or ""
        description = listing.get("description", "") or ""

        # Skip if stop_location found in location field OR description text.
        # Short tokens (≤3 chars: NJ, CA) use word boundaries to avoid false
        # positives (e.g. "CA" inside "Canggu").
        location_rejected = False
        search_for_loc = f"{location} {description}".lower()
        for stop_loc in stop_locations:
            stop_loc_lower = stop_loc.lower()
            if len(stop_loc_lower) <= 3:
                pattern = r'\b' + _re.escape(stop_loc_lower) + r'\b'
                if _re.search(pattern, search_for_loc):
                    logger.info(f"SKIP {fb_id}: Stop location '{stop_loc}' in text")
                    skipped_location_count += 1
                    location_rejected = True
                    break
            else:
                if stop_loc_lower in search_for_loc:
                    logger.info(f"SKIP {fb_id}: Stop location '{stop_loc}' in text")
                    skipped_location_count += 1
                    location_rejected = True
                    break

        if location_rejected:
            continue

        # Global stop_words check (title + description)
        title_text = listing.get("title", "") or ""
        full_text_lower = f"{title_text} {description}".lower()
        found_stop = None
        for sw in stop_words_lower:
            if sw in full_text_lower:
                found_stop = sw
                break
        if not found_stop:
            for sw in stop_words_detailed_lower:
                if sw in full_text_lower:
                    found_stop = sw
                    break

        if found_stop:
            reason = f"stop_word: {found_stop}"
            params = {}
        else:
            # Parse title + description combined to capture bedroom counts in title
            parse_text = f"{title_text} {description}".strip()
            params = parser.parse(parse_text)
            reason = f"QFR detailed import (profile: {profile_name})"

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

    return processed_count, upserted_count, skipped_location_count


def main():
    logger.info("=" * 80)
    logger.info("STAGE 2 (QFR): Manual Full Detail Scraping")
    logger.info("=" * 80)

    parser_cli = argparse.ArgumentParser(description="Stage 2 QFR scraper")
    parser_cli.add_argument(
        "--chat", type=int, default=None,
        help="chat_id of the profile to scrape. Without this, runs all enabled profiles."
    )
    args = parser_cli.parse_args()

    load_dotenv()
    config = load_config()

    db_url = os.getenv("DATABASE_URL")
    apify_key = os.getenv("APIFY_API_KEY")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    admin_chat_id_env = os.getenv("TELEGRAM_CHAT_ID")
    admin_id = os.getenv("TELEGRAM_ADMIN_ID")

    notifier = None
    if bot_token and (admin_chat_id_env or admin_id):
        notifier = TelegramNotifier(bot_token, admin_chat_id_env, config, admin_chat_id=admin_id)

    if not all([db_url, apify_key]):
        msg = "Missing required env vars: DATABASE_URL, APIFY_API_KEY"
        logger.error(msg)
        if notifier:
            notifier.send_error("Stage 2 QFR", msg)
        sys.exit(1)

    try:
        # Sync profiles to DB and load enabled ones
        chat_profiles_cfg = config.get('chat_profiles', []) or []
        with Database() as db_sync:
            if chat_profiles_cfg:
                db_sync.sync_chat_profiles(chat_profiles_cfg)
            all_enabled_profiles = db_sync.get_enabled_chat_profiles()

        if not all_enabled_profiles:
            logger.warning("No enabled chat profiles found in DB")
            sys.exit(0)

        # Determine which profiles to run
        if args.chat is not None:
            profiles_to_run = [p for p in all_enabled_profiles if p['chat_id'] == args.chat]
            if not profiles_to_run:
                logger.error(f"No enabled profile found for chat_id={args.chat}")
                sys.exit(1)
        else:
            profiles_to_run = all_enabled_profiles
            logger.info(f"No --chat specified → running all {len(profiles_to_run)} enabled profiles")

        prop_parser = PropertyParser()
        qfr_scraper = FacebookMarketplaceQfrScraper(apify_key, config)
        qfr_cfg = config.get("marketplace_qfr", {}) or {}

        total_processed = 0
        total_upserted = 0
        total_skipped = 0

        with Database() as db:
            for profile in profiles_to_run:
                profile_name = profile.get('name', str(profile['chat_id']))
                start_urls = list(profile.get('qfr_start_urls') or [])

                if not start_urls:
                    logger.warning("[%s] No qfr_start_urls configured — skipping", profile_name)
                    continue

                logger.info("\n" + "-" * 60)
                logger.info(f"Profile: {profile_name} (chat_id={profile['chat_id']})")
                logger.info(f"URLs: {start_urls}")

                processed, upserted, skipped = _scrape_and_upsert(
                    start_urls=start_urls,
                    profile_name=profile_name,
                    qfr_scraper=qfr_scraper,
                    parser=prop_parser,
                    db=db,
                    config=config,
                    qfr_cfg=qfr_cfg,
                )
                total_processed += processed
                total_upserted += upserted
                total_skipped += skipped

                logger.info(
                    "[%s] Processed: %s | Upserted: %s | Skipped (location): %s",
                    profile_name, processed, upserted, skipped,
                )

        logger.info("=" * 80)
        logger.info("STAGE 2 (QFR) COMPLETE")
        logger.info("Total processed: %s", total_processed)
        logger.info("Total upserted in DB: %s", total_upserted)
        logger.info("Total skipped (stop_locations): %s", total_skipped)
        logger.info("Profiles run: %s", [p['name'] for p in profiles_to_run])
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