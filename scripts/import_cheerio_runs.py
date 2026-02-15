#!/usr/bin/env python3
"""
Import processed items from Apify Cheerio actor runs into local DB.

What it does:
- Reads recent runs from `memo23/facebook-marketplace-cheerio`
- Fetches every run dataset item
- Normalizes to our listing format
- Inserts missing listings
- Updates statuses for existing `stage1/stage1_new` listings that were actually processed
"""

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List

from apify_client import ApifyClient
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import Database
from facebook_marketplace_cheerio_scraper import FacebookMarketplaceCheerioScraper
from property_parser import PropertyParser


logger = logging.getLogger(__name__)
ACTOR_IDS = [
    "memo23/facebook-marketplace-cheerio",
    "memo23~facebook-marketplace-cheerio",
]


def _determine_stage2_status(
    parser: PropertyParser,
    criterias: Dict,
    stop_words_detailed_lower: List[str],
    description: str,
) -> tuple[str, str, Dict]:
    """Return (new_status, reason, parsed_params)."""
    if not description:
        return "stage2_failed", "NO_DESCRIPTION_FROM_CHEERIO", {}

    description_lower = description.lower()
    for sw in stop_words_detailed_lower:
        if sw in description_lower:
            return "stage2_failed", f"Stop word in description: {sw}", {}

    params = parser.parse(description)
    passed, reason = parser.matches_criteria(params, criterias, stage=2)
    return ("stage2" if passed else "stage2_failed"), reason, params


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    load_dotenv()

    argp = argparse.ArgumentParser(description="Import processed items from Cheerio actor runs")
    argp.add_argument("--runs-limit", type=int, default=30, help="How many latest runs to scan")
    argp.add_argument("--include-non-succeeded", action="store_true", help="Also process non-SUCCEEDED runs")
    argp.add_argument(
        "--run-ids-log",
        default="logs/stage2_manual.log",
        help="Fallback: parse run IDs from this log file if actor runs listing is unavailable",
    )
    args = argp.parse_args()

    apify_key = os.getenv("APIFY_API_KEY")
    if not apify_key:
        raise RuntimeError("Missing APIFY_API_KEY")

    config_path = Path("config/config.json")
    if not config_path.exists():
        config_path = Path("/app/config/config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    parser = PropertyParser()
    cheerio = FacebookMarketplaceCheerioScraper(apify_key, config)
    client = ApifyClient(apify_key)

    criterias = config.get("criterias", {}) or {}
    stop_words_detailed = config.get("filters", {}).get("stop_words_detailed", []) or []
    stop_words_detailed_lower = [w.lower() for w in stop_words_detailed]

    selected_actor_id = None
    runs = []
    for actor_id in ACTOR_IDS:
        try:
            runs = list(client.actor(actor_id).runs().list(limit=args.runs_limit).items)
            if runs:
                selected_actor_id = actor_id
                break
        except Exception as e:
            logger.warning("Could not list runs for actor %s: %s", actor_id, e)

    if not runs:
        logger.warning("No runs found for any actor ids: %s", ", ".join(ACTOR_IDS))
        log_path = Path(args.run_ids_log)
        if not log_path.exists():
            logger.warning("Fallback log not found: %s", log_path)
            return
        txt = log_path.read_text(encoding="utf-8", errors="ignore")
        run_ids = list(dict.fromkeys(re.findall(r"Actor run ID:\s*([A-Za-z0-9]+)", txt)))
        if not run_ids:
            logger.warning("No run IDs found in fallback log: %s", log_path)
            return
        logger.info("Using %s run IDs parsed from %s", len(run_ids), log_path)
        runs = []
        for rid in run_ids[-args.runs_limit:]:
            try:
                run_obj = client.run(rid).get()
                if run_obj:
                    runs.append(run_obj)
            except Exception as e:
                logger.warning("Could not fetch run %s: %s", rid, e)
        selected_actor_id = "fallback_from_logs"
        if not runs:
            logger.warning("No valid runs fetched from parsed run IDs.")
            return

    logger.info("Found %s runs for actor %s", len(runs), selected_actor_id)

    total_items = 0
    normalized_ok = 0
    inserted = 0
    updated_stage1 = 0
    skipped_existing_non_stage1 = 0
    skipped_unusable = 0
    errors = 0

    with Database() as db:
        for run in runs:
            run_id = run.get("id")
            run_status = run.get("status")
            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                continue
            if not args.include_non_succeeded and run_status != "SUCCEEDED":
                continue

            logger.info("Run %s status=%s dataset=%s", run_id, run_status, dataset_id)

            try:
                items = list(client.dataset(dataset_id).iterate_items())
            except Exception as e:
                logger.warning("Failed reading dataset %s: %s", dataset_id, e)
                errors += 1
                continue

            logger.info("Dataset %s: %s items", dataset_id, len(items))

            for raw in items:
                total_items += 1
                if not isinstance(raw, dict) or ("error" in raw):
                    skipped_unusable += 1
                    continue

                listing = cheerio.normalize_listing(raw)
                if not listing:
                    skipped_unusable += 1
                    continue

                normalized_ok += 1
                fb_id = listing.get("fb_id")
                listing_url = listing.get("listing_url", "") or ""
                if not fb_id:
                    skipped_unusable += 1
                    continue

                description = listing.get("description", "") or ""
                new_status, reason, params = _determine_stage2_status(
                    parser, criterias, stop_words_detailed_lower, description
                )
                location_extracted = parser.extract_location(description) if description else None
                phones = parser.extract_phone_numbers(description) if description else []
                phone_number = phones[0] if phones else None

                try:
                    db.cursor.execute("SELECT fb_id, status FROM listings WHERE fb_id = %s", (fb_id,))
                    existing = db.cursor.fetchone()

                    # Fallback match by URL for cases when actor did not provide stable item ID.
                    matched_fb_id = fb_id
                    if not existing and listing_url:
                        db.cursor.execute(
                            "SELECT fb_id, status FROM listings WHERE listing_url = %s ORDER BY created_at DESC LIMIT 1",
                            (listing_url,),
                        )
                        existing = db.cursor.fetchone()
                        if existing:
                            matched_fb_id = existing[0]

                    if not existing:
                        db.cursor.execute(
                            """
                            INSERT INTO listings (
                                fb_id, title, price, location, listing_url, description, source,
                                status, pass_reason, phone_number, bedrooms, price_extracted,
                                has_ac, has_wifi, has_pool, has_parking,
                                utilities, furniture, rental_term, location_extracted
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                fb_id,
                                listing.get("title", "") or "",
                                listing.get("price", "") or "",
                                listing.get("location", "") or "",
                                listing_url,
                                description,
                                "apify-marketplace",
                                new_status,
                                f"Imported from Cheerio run {run_id}: {reason}",
                                phone_number,
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
                            ),
                        )
                        inserted += 1
                    else:
                        current_status = existing[1]
                        if current_status in ("stage1", "stage1_new"):
                            db.cursor.execute(
                                """
                                UPDATE listings
                                   SET title = %s,
                                       price = %s,
                                       location = %s,
                                       listing_url = %s,
                                       description = %s,
                                       source = %s,
                                       status = %s,
                                       pass_reason = %s,
                                       phone_number = %s,
                                       bedrooms = %s,
                                       price_extracted = %s,
                                       has_ac = %s,
                                       has_wifi = %s,
                                       has_pool = %s,
                                       has_parking = %s,
                                       utilities = %s,
                                       furniture = %s,
                                       rental_term = %s,
                                       location_extracted = %s,
                                       updated_at = NOW()
                                 WHERE fb_id = %s
                                """,
                                (
                                    listing.get("title", "") or "",
                                    listing.get("price", "") or "",
                                    listing.get("location", "") or "",
                                    listing_url,
                                    description,
                                    "apify-marketplace",
                                    new_status,
                                    f"Processed from Cheerio run {run_id}: {reason}",
                                    phone_number,
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
                                    matched_fb_id,
                                ),
                            )
                            updated_stage1 += 1
                        else:
                            skipped_existing_non_stage1 += 1

                    db.conn.commit()
                except Exception as e:
                    db.conn.rollback()
                    errors += 1
                    logger.warning("DB error on fb_id=%s: %s", fb_id, e)

    logger.info("IMPORT COMPLETE")
    logger.info("Total dataset items: %s", total_items)
    logger.info("Normalized OK: %s", normalized_ok)
    logger.info("Inserted new rows: %s", inserted)
    logger.info("Updated stage1/stage1_new rows: %s", updated_stage1)
    logger.info("Skipped existing non-stage1 rows: %s", skipped_existing_non_stage1)
    logger.info("Skipped unusable items: %s", skipped_unusable)
    logger.info("Errors: %s", errors)


if __name__ == "__main__":
    main()

