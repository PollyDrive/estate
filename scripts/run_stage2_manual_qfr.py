#!/usr/bin/env python3
"""
STAGE 2 (alternative): Full detail scraping with actor qFR6mjgdwPouKLDvE.
Keeps legacy Cheerio stage2 script untouched.
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
    if not all([db_url, apify_key]):
        logger.error("Missing required env vars: DATABASE_URL, APIFY_API_KEY")
        sys.exit(1)

    parser = PropertyParser()
    qfr_scraper = FacebookMarketplaceQfrScraper(apify_key, config)

    with Database() as db:
        db.cursor.execute(
            """
            SELECT fb_id, listing_url, source, description
            FROM listings
            WHERE status IN ('stage1', 'stage1_new')
            ORDER BY created_at DESC
            """
        )
        cols = [d[0] for d in db.cursor.description]
        listings_to_process = [dict(zip(cols, row)) for row in db.cursor.fetchall()]

    if not listings_to_process:
        logger.warning("No listings with status stage1/stage1_new")
        return

    logger.info("Found %s listings to process", len(listings_to_process))

    groups_with_description = [
        l for l in listings_to_process if l["source"] == "facebook_group" and l.get("description")
    ]
    groups_no_description = [
        l for l in listings_to_process if l["source"] == "facebook_group" and not l.get("description")
    ]
    marketplace_listings = [
        l for l in listings_to_process if l["source"] in ("marketplace", "apify", "apify-marketplace", None, "")
    ]

    logger.info("  Groups with description: %s", len(groups_with_description))
    logger.info("  Groups without description: %s", len(groups_no_description))
    logger.info("  Marketplace listings: %s", len(marketplace_listings))

    # Stage 2 should only process concrete item URLs, not search/unavailable pages.
    raw_urls = [c["listing_url"] for c in marketplace_listings if c.get("listing_url")]
    candidate_urls = [u for u in raw_urls if "/marketplace/item/" in u]
    skipped_bad_urls = len(raw_urls) - len(candidate_urls)
    if skipped_bad_urls:
        logger.warning(
            "Skipped %s marketplace URLs that are not item links (e.g. unavailable/search pages).",
            skipped_bad_urls,
        )
    stage2_listings = []
    if candidate_urls:
        max_stage2 = int(config.get("marketplace_qfr", {}).get("max_stage2_items", 50))
        logger.info("Scraping full details via QFR actor (max %s items)...", max_stage2)
        try:
            stage2_listings = qfr_scraper.scrape_full_details(candidate_urls, max_stage2_items=max_stage2)
            logger.info("✓ Scraped %s full listings", len(stage2_listings))
        except Exception as e:
            logger.error("✗ Error scraping with QFR actor: %s", e, exc_info=True)

    stop_words_detailed = config.get("filters", {}).get("stop_words_detailed", [])
    stop_words_detailed_lower = [w.lower() for w in stop_words_detailed]
    criterias = config.get("criterias", {})

    processed_count = 0
    updated_count = 0

    with Database() as db:
        # Marketplace rows from actor result
        for listing in stage2_listings:
            processed_count += 1
            fb_id = listing.get("fb_id")
            if not fb_id:
                logger.warning("Scraped listing missing fb_id, skip")
                continue

            description = listing.get("description", "") or ""

            found_stop = None
            if description and stop_words_detailed_lower:
                low = description.lower()
                for sw in stop_words_detailed_lower:
                    if sw in low:
                        found_stop = sw
                        break

            if found_stop:
                new_status = "stage2_failed"
                reason = f"Detailed stop word: {found_stop}"
                params = {}
            else:
                params = parser.parse(description)
                passed, reason = parser.matches_criteria(params, criterias, stage=2)
                new_status = "stage2" if passed else "stage2_failed"

            location_extracted = parser.extract_location(description) if description else None
            phones = parser.extract_phone_numbers(description) if description else []
            phone = phones[0] if phones else None

            update = {
                "title": listing.get("title", ""),
                "description": description,
                "price": listing.get("price", ""),
                "location": listing.get("location", ""),
                "listing_url": listing.get("listing_url", ""),
                "source": "apify-marketplace",
                "phone_number": phone,
                "bedrooms": params.get("bedrooms"),
                "price_extracted": params.get("price"),
                "has_ac": params.get("has_ac", False),
                "has_wifi": params.get("has_wifi", False),
                "has_pool": params.get("has_pool", False),
                "has_parking": params.get("has_parking", False),
                "utilities": params.get("utilities"),
                "furniture": params.get("furniture"),
                "rental_term": params.get("rental_term"),
                "location_extracted": location_extracted,
                "status": new_status,
                "pass_reason": reason,
            }
            set_clause = ", ".join([f"{k} = %s" for k in update.keys()])
            values = list(update.values()) + [fb_id]
            db.cursor.execute(f"UPDATE listings SET {set_clause}, updated_at = NOW() WHERE fb_id = %s", tuple(values))
            db.conn.commit()
            updated_count += 1

        # Existing group flow remains the same as old stage2_manual
        logger.info("\nProcessing %s Groups with existing description...", len(groups_with_description))
        for listing in groups_with_description:
            processed_count += 1
            fb_id = listing["fb_id"]
            description = listing["description"] or ""
            extracted_title = parser.extract_title_from_description(description, max_length=150)
            location_extracted = parser.extract_location(description) if description else None

            found_stop = None
            if description and stop_words_detailed_lower:
                low = description.lower()
                for sw in stop_words_detailed_lower:
                    if sw in low:
                        found_stop = sw
                        break

            if found_stop:
                new_status = "stage2_failed"
                reason = f"Detailed stop word: {found_stop}"
                params = {}
            else:
                params = parser.parse(description)
                passed, reason = parser.matches_criteria(params, criterias, stage=2)
                new_status = "stage2" if passed else "stage2_failed"

            phones = parser.extract_phone_numbers(description) if description else []
            phone = phones[0] if phones else None
            update = {
                "title": extracted_title,
                "phone_number": phone,
                "bedrooms": params.get("bedrooms"),
                "price_extracted": params.get("price"),
                "has_ac": params.get("has_ac", False),
                "has_wifi": params.get("has_wifi", False),
                "location_extracted": location_extracted,
                "status": new_status,
                "pass_reason": reason,
            }
            set_clause = ", ".join([f"{k} = %s" for k in update.keys()])
            values = list(update.values()) + [fb_id]
            db.cursor.execute(f"UPDATE listings SET {set_clause}, updated_at = NOW() WHERE fb_id = %s", tuple(values))
            db.conn.commit()
            updated_count += 1

        logger.info("\nProcessing %s Groups without description...", len(groups_no_description))
        for listing in groups_no_description:
            processed_count += 1
            fb_id = listing["fb_id"]
            db.cursor.execute(
                "UPDATE listings SET status = %s, updated_at = NOW() WHERE fb_id = %s",
                ("no_description", fb_id),
            )
            db.conn.commit()
            updated_count += 1

    logger.info("=" * 80)
    logger.info("STAGE 2 (QFR) COMPLETE")
    logger.info("Processed: %s", processed_count)
    logger.info("Updated in DB: %s", updated_count)
    logger.info("  - Marketplace scraped: %s", len(stage2_listings))
    logger.info("  - Groups with description: %s", len(groups_with_description))
    logger.info("  - Groups without description: %s", len(groups_no_description))
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

