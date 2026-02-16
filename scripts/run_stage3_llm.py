#!/usr/bin/env python3
"""
STAGE 3: LLM Analysis for saved listings
Analyzes descriptions with OpenRouter to check if listing matches criteria.
Updates listings with analysis results.
"""

import os
import sys
import logging
import re
from pathlib import Path
from dotenv import load_dotenv
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from llm_filters import OpenRouterFilter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage3_llm.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def _extract_reject_type(reason: str) -> str:
    text = (reason or "").strip()
    m = re.search(r"\b(REJECT_[A-Z_]+)\b", text)
    return m.group(1) if m else "REJECT_UNKNOWN"


def main():
    """Run Stage 3: LLM analysis for unprocessed listings"""

    logger.info("=" * 80)
    logger.info("STAGE 3: LLM Analysis (OpenRouter)")
    logger.info("=" * 80)

    # Load environment
    load_dotenv()

    # Load config
    config_path = 'config/config.json'
    if not os.path.exists(config_path):
        config_path = '/app/config.json'

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    openrouter_api_key = os.getenv('OPENROUTER_API_KEY')

    if not all([db_url, openrouter_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, OPENROUTER_API_KEY)!")
        sys.exit(1)

    # Initialize OpenRouter filter
    try:
        llm_filter = OpenRouterFilter(config, openrouter_api_key)
        logger.info("✓ OpenRouter filter initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize OpenRouter filter: {e}")
        sys.exit(1)

    # Get listings with status 'stage2' ready for LLM analysis.
    # Description is preferred, but we also support title-only fallback.
    with Database() as db:
        query = """
            SELECT fb_id, title, description, location, price, price_extracted,
                   phone_number, bedrooms, kitchen_type,
                   has_ac, has_wifi, has_pool, has_parking,
                   utilities, furniture, rental_term, listing_url, source
            FROM listings
            WHERE status = 'stage2'
            ORDER BY created_at DESC
        """
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]

    if not listings:
        logger.warning("No unprocessed listings found")
        logger.info("All listings have been analyzed or don't have descriptions")
        sys.exit(0)

    logger.info(f"Found {len(listings)} listings to analyze")

    # Process each listing
    passed_count = 0
    failed_count = 0
    error_count = 0
    criterias = config.get('criterias', {}) or {}
    max_price = float(criterias.get('price_max', 40000000))
    enforce_stop_locations = bool(config.get('filters', {}).get('enforce_stop_locations', False))

    with Database() as db:
        for listing in listings:
            fb_id = listing['fb_id']
            description = listing.get('description') or ''
            title = listing.get('title') or ''
            location = listing.get('location') or ''
            llm_text = description.strip() if description and description.strip() else title.strip()
            text_mode = "description" if description and description.strip() else "title-only"

            logger.info(f"\nAnalyzing {fb_id}: {title[:50] if title else 'No title'}...")
            logger.info(f"  Location: {location}")
            logger.info(f"  Mode: {text_mode}")

            if not llm_text:
                reason = "REJECT_NO_TEXT (both description and title are empty)"
                db.cursor.execute(
                    "UPDATE listings SET status = 'stage3_failed', llm_reason = %s, llm_passed = %s, llm_analyzed_at = NOW() WHERE fb_id = %s",
                    (reason, False, fb_id)
                )
                db.conn.commit()
                failed_count += 1
                logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                continue

            # Check location against stop_locations FIRST
            stop_locations = config.get('filters', {}).get('stop_locations', [])
            location_lower = location.lower() if location else ''
            llm_text_lower = llm_text.lower() if llm_text else ''

            passed = True
            reason = None

            # Hard gate: reject only explicit 1/2/3 bedroom listings.
            bedrooms = listing.get('bedrooms')
            if bedrooms is not None and bedrooms < 4:
                passed = False
                reason = f"REJECT_BEDROOMS (need 4+, got: {bedrooms})"
                logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")

            for stop_loc in stop_locations:
                if not passed:
                    break
                # Remove "in " prefix for checking (e.g., "in Ubud" -> "ubud")
                stop_loc_clean = stop_loc.lower().replace('in ', '').strip()

                # Check both location field and currently used LLM text.
                if stop_loc_clean in location_lower or stop_loc_clean in llm_text_lower:
                    if enforce_stop_locations:
                        passed = False
                        reason = f"REJECT_LOCATION (stop location: {stop_loc})"
                        logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                    else:
                        logger.info(
                            f"  ⚠ stop location matched ({stop_loc}) but enforce_stop_locations=false, continue to LLM"
                        )
                    break

            # Suspicious short prices are no longer hard-rejected here.
            # Let the LLM decide these cases to avoid false negatives.

            # If location check passed, run OpenRouter analysis
            if passed:
                try:
                    passed, reason = llm_filter.filter(llm_text)
                    reject_type = _extract_reject_type(reason)

                    # Deterministic overrides for known false negatives.
                    if not passed and reject_type == "REJECT_BEDROOMS":
                        bedrooms = listing.get('bedrooms')
                        if bedrooms is not None and bedrooms >= 4:
                            passed = True
                            reason = f"PASS_OVERRIDE_BEDROOMS (structured bedrooms={bedrooms})"
                            reject_type = ""

                    if not passed and reject_type == "REJECT_PRICE":
                        extracted_price = listing.get('price_extracted')
                        try:
                            extracted_price_f = float(extracted_price) if extracted_price is not None else None
                        except Exception:
                            extracted_price_f = None
                        if extracted_price_f is not None and extracted_price_f <= max_price:
                            passed = True
                            reason = (
                                f"PASS_OVERRIDE_PRICE (structured price={extracted_price_f:.0f} <= {max_price:.0f})"
                            )
                            reject_type = ""

                    # Update status based on LLM result
                    new_status = 'stage3' if passed else 'stage3_failed'
                    pass_reason = reason if passed else f"{_extract_reject_type(reason)} | {reason}"

                    # Save LLM analysis result
                    db.cursor.execute(
                        "UPDATE listings SET status = %s, llm_reason = %s, pass_reason = %s, llm_passed = %s, llm_analyzed_at = NOW() WHERE fb_id = %s",
                        (new_status, reason, pass_reason, passed, fb_id)
                    )
                    db.conn.commit()

                    if passed:
                        logger.info(f"  ✓ PASSED: {reason} → status: stage3")
                        passed_count += 1
                    else:
                        logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                        failed_count += 1

                except Exception as e:
                    logger.error(f"  ✗ ERROR: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # Stop processing immediately on ANY LLM error.
                    # Do NOT mark listing as failed; keep it in stage2 for retry after fixing the issue.
                    try:
                        db.conn.rollback()
                    except Exception:
                        pass
                    logger.error(f"Stopping Stage 3 due to LLM error (fb_id={fb_id}). Exiting with code 1.")
                    sys.exit(1)
            else:
                # Location/bedrooms/price filtered, update status
                pass_reason = f"{_extract_reject_type(reason)} | {reason}"
                db.cursor.execute(
                    "UPDATE listings SET status = 'stage3_failed', llm_reason = %s, pass_reason = %s, llm_passed = %s, llm_analyzed_at = NOW() WHERE fb_id = %s",
                    (reason, pass_reason, False, fb_id)
                )
                db.conn.commit()
                failed_count += 1

    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 3 COMPLETE")
    logger.info(f"Total analyzed: {len(listings)}")
    logger.info(f"Passed: {passed_count}")
    logger.info(f"Filtered: {failed_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("")
    logger.info("Listings with status 'stage3' are ready for Stage 4 (deduplication)")
    logger.info("Command: python3 scripts/run_stage4.py")
    logger.info("=" * 80)

    # Fail the script if there were any LLM errors (so cron/CI can detect issues).
    if error_count > 0:
        logger.error(f"Stage 3 finished with {error_count} LLM errors. Exiting with code 1.")
        sys.exit(1)


if __name__ == '__main__':
    main()

