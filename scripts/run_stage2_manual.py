#!/usr/bin/env python3
"""
STAGE 2: Filter and enrich group-scraped listings.

Takes listings with status 'stage1' / 'stage1_new':
  - facebook_group with description  → apply GLOBAL filters, advance to 'stage2' or 'stage2_failed'
  - facebook_group without description → mark 'no_description'
  - marketplace / apify sources      → return to 'stage1' (handled by stage2_qfr, not here)

Filtering logic (GLOBAL only — no per-profile criteria here):
  - stop_words / stop_words_detailed (land, kos, room-only, construction, US markers…)
  - stop_locations checked against BOTH location field AND description text
  Profile-specific filtering (bedrooms, price, allowed_locations) happens in stage4 --chat.
"""

import os
import re
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from property_parser import PropertyParser
from config_loader import load_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage2_manual.log'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _check_stop_locations(text: str, stop_locations: list[str]) -> str | None:
    """
    Return the first stop_location found in text, or None.
    Short tokens (≤3 chars: NJ, CA, FL) require word boundaries to avoid
    false positives (e.g. 'CA' inside 'Canggu').
    """
    text_lower = text.lower()
    for loc in stop_locations:
        loc_lower = loc.lower()
        if len(loc_lower) <= 3:
            if re.search(r'\b' + re.escape(loc_lower) + r'\b', text_lower):
                return loc
        else:
            if loc_lower in text_lower:
                return loc
    return None


def main():
    """Run Stage 2: Filter group listings, return marketplace to stage1."""

    logger.info("=" * 80)
    logger.info("STAGE 2: Group Listing Filter")
    logger.info("=" * 80)

    load_dotenv()
    config = load_config()

    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.error("Missing DATABASE_URL!")
        sys.exit(1)

    parser = PropertyParser(config)

    # ── Global filters (no profiles needed here) ─────────────────────────────
    filters_cfg = config.get('filters', {})
    global_stop_words          = [w.lower() for w in (filters_cfg.get('stop_words') or [])]
    global_stop_words_detailed = [w.lower() for w in (filters_cfg.get('stop_words_detailed') or [])]
    global_stop_locations      = [loc for loc in (filters_cfg.get('stop_locations') or [])]
    logger.info(f"Global filters: {len(global_stop_words)} stop_words, "
                f"{len(global_stop_words_detailed)} stop_words_detailed, "
                f"{len(global_stop_locations)} stop_locations")

    # ── Fetch stage1 listings ────────────────────────────────────────────────
    with Database() as db:
        db.cursor.execute(
            "SELECT fb_id, title, listing_url, source, description "
            "FROM listings WHERE status IN ('stage1', 'stage1_new') ORDER BY created_at DESC"
        )
        columns = [d[0] for d in db.cursor.description]
        all_listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]

    if not all_listings:
        logger.warning("No listings with status 'stage1' or 'stage1_new'.")
        sys.exit(0)

    logger.info(f"Found {len(all_listings)} listings to process.")

    # Bucket by source
    groups_with_desc    = [l for l in all_listings
                           if l['source'] == 'facebook_group' and l.get('description')]
    groups_no_desc      = [l for l in all_listings
                           if l['source'] == 'facebook_group' and not l.get('description')]
    marketplace_listings = [l for l in all_listings
                            if l['source'] not in ('facebook_group',)]

    logger.info(f"  Groups with description:    {len(groups_with_desc)}")
    logger.info(f"  Groups without description: {len(groups_no_desc)}")
    logger.info(f"  Marketplace (return→stage1): {len(marketplace_listings)}")

    processed = passed_count = failed_count = no_desc_count = returned_count = 0

    with Database() as db:

        # ── Marketplace: return to stage1 so stage2_qfr picks them up ──────
        if marketplace_listings:
            logger.info(f"\nReturning {len(marketplace_listings)} marketplace listings to 'stage1'...")
            for listing in marketplace_listings:
                fb_id = listing['fb_id']
                db.cursor.execute(
                    "UPDATE listings SET status = 'stage1', pass_reason = NULL, updated_at = NOW() "
                    "WHERE fb_id = %s",
                    (fb_id,)
                )
                returned_count += 1
            db.conn.commit()
            logger.info(f"  ✓ Returned {returned_count} marketplace listings to 'stage1'")

        # ── Groups without description ───────────────────────────────────────
        if groups_no_desc:
            logger.info(f"\nMarking {len(groups_no_desc)} groups without description...")
            for listing in groups_no_desc:
                db.cursor.execute(
                    "UPDATE listings SET status = 'no_description', updated_at = NOW() WHERE fb_id = %s",
                    (listing['fb_id'],)
                )
                no_desc_count += 1
            db.conn.commit()

        # ── Groups with description: full filter pipeline ────────────────────
        logger.info(f"\nProcessing {len(groups_with_desc)} groups with description...")

        for listing in groups_with_desc:
            processed += 1
            fb_id = listing['fb_id']
            description = listing.get('description') or ''

            # Parse structured fields from title + description combined so that
            # bedroom counts embedded in the title ("3 Beds 3 Baths House",
            # "Four bedroom villa") are captured even when description is vague.
            title_text = listing.get('title') or ''
            parse_text = f"{title_text} {description}".strip()
            params = parser.parse(parse_text)
            bedrooms      = params.get('bedrooms')
            price_extracted = params.get('price')
            location      = parser.extract_location(description) or ''
            extracted_title = parser.extract_title_from_description(description, max_length=150)
            phone         = (parser.extract_phone_numbers(description) or [None])[0]

            new_status = 'stage2'
            reason     = None
            full_text  = f"{title_text} {description}".lower()

            # 1. Global stop_words (title+description combined).
            #    stop_words are also checked at stage1 on title alone — here we
            #    catch patterns that only appear in the full description.
            if new_status == 'stage2' and global_stop_words:
                hit = next((w for w in global_stop_words if w in full_text), None)
                if hit:
                    new_status = 'stage2_failed'
                    reason = f"stop_word: '{hit}'"
                    logger.info(f"  ✗ {fb_id}: {reason}")

            # 2. Global stop_words_detailed (longer lease-term patterns)
            if new_status == 'stage2' and global_stop_words_detailed:
                hit = next((w for w in global_stop_words_detailed if w in full_text), None)
                if hit:
                    new_status = 'stage2_failed'
                    reason = f"stop_word_detailed: '{hit}'"
                    logger.info(f"  ✗ {fb_id}: {reason}")

            # 3. Global stop_locations — checked against BOTH location field AND
            #    full description so listings like "apartment in New York" with
            #    empty location field are still caught.
            if new_status == 'stage2' and global_stop_locations:
                search_for_loc = f"{location} {description}"
                hit = _check_stop_locations(search_for_loc, global_stop_locations)
                if hit:
                    new_status = 'stage2_failed'
                    reason = f"stop_location: '{hit}'"
                    logger.info(f"  ✗ {fb_id}: {reason}")

            if new_status == 'stage2':
                logger.info(f"  ✓ {fb_id}: passed global filters")

            if new_status == 'stage2':
                passed_count += 1
            else:
                failed_count += 1

            # Update listing with parsed fields + new status
            db.cursor.execute(
                """
                UPDATE listings SET
                    title            = COALESCE(NULLIF(%s, ''), title),
                    phone_number     = COALESCE(%s, phone_number),
                    bedrooms         = COALESCE(%s, bedrooms),
                    price_extracted  = COALESCE(%s, price_extracted),
                    has_ac           = %s,
                    has_wifi         = %s,
                    location         = COALESCE(NULLIF(%s, ''), location),
                    status           = %s,
                    pass_reason      = %s,
                    updated_at       = NOW()
                WHERE fb_id = %s
                """,
                (
                    extracted_title,
                    phone,
                    bedrooms,
                    price_extracted,
                    params.get('has_ac', False),
                    params.get('has_wifi', False),
                    location,
                    new_status,
                    reason if new_status == 'stage2_failed' else None,
                    fb_id,
                )
            )
            db.conn.commit()

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("=" * 80)
    logger.info("STAGE 2 COMPLETE")
    logger.info(f"  Groups processed:          {processed}")
    logger.info(f"    → stage2 (passed):       {passed_count}")
    logger.info(f"    → stage2_failed:         {failed_count}")
    logger.info(f"    → no_description:        {no_desc_count}")
    logger.info(f"  Marketplace → stage1:      {returned_count}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
