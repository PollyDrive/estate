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
from telegram_notifier import TelegramNotifier

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


def _token_match(text: str, token: str) -> bool:
    """Case-insensitive whole-token match (prevents NY -> Nyuh)."""
    if not text or not token:
        return False
    pattern = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])"
    return bool(re.search(pattern, text, re.IGNORECASE))


def _matches_stop_location(location: str, llm_text: str, stop_loc: str) -> bool:
    """
    Safer stop-location matching:
    - Short codes (NY/NJ/CA/...) are matched ONLY against `location` as whole tokens.
    - Longer values are matched as whole tokens in location first, then LLM text.
    """
    stop_raw = (stop_loc or "").strip()
    if not stop_raw:
        return False

    clean = re.sub(r"^in\s+", "", stop_raw, flags=re.IGNORECASE).strip()
    if not clean:
        return False

    location = location or ""
    llm_text = llm_text or ""

    is_short_code = bool(re.fullmatch(r"[A-Za-z]{2,3}", clean))
    if is_short_code:
        return _token_match(location, clean)

    return _token_match(location, clean) or _token_match(llm_text, clean)


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
        config_path = '/app/config/config.json'

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    openrouter_api_key = os.getenv('OPENROUTER_API_KEY')
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    admin_id = os.getenv("TELEGRAM_ADMIN_ID")

    notifier = None
    if bot_token and (chat_id or admin_id):
        notifier = TelegramNotifier(bot_token, chat_id, config, admin_chat_id=admin_id)

    if not all([db_url, openrouter_api_key]):
        msg = "Missing required environment variables (DATABASE_URL, OPENROUTER_API_KEY)!"
        logger.error(msg)
        if notifier:
            notifier.send_error("Stage 3 LLM", msg)
        sys.exit(1)

    try:
        # Initialize OpenRouter filter
        try:
            llm_filter = OpenRouterFilter(config, openrouter_api_key)
            logger.info("✓ OpenRouter filter initialized")
        except Exception as e:
            logger.error(f"✗ Failed to initialize OpenRouter filter: {e}")
            if notifier:
                notifier.send_error("Stage 3 LLM", f"Initialization failed: {e}")
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
            return

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
                allowed_locations = criterias.get('allowed_locations', [])
                allowed_locations_lower = [a.lower() for a in allowed_locations]
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
                    if _matches_stop_location(location, llm_text, stop_loc):
                        if enforce_stop_locations:
                            passed = False
                            reason = f"REJECT_LOCATION (stop location: {stop_loc})"
                            logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                        else:
                            logger.info(
                                f"  ⚠ stop location matched ({stop_loc}) but enforce_stop_locations=false, continue to LLM"
                            )
                        break

                # If location is empty or not in allowed_locations, ask LLM to determine country.
                # This catches USA/other-country listings that slip through with blank location.
                if passed and allowed_locations:
                    location_known = location_lower and any(
                        loc in location_lower for loc in allowed_locations_lower
                    )
                    if not location_known:
                        # Ask LLM: is this a Bali/Indonesia listing?
                        loc_prompt = (
                            f"You are a geo-classifier. Based ONLY on the text below, "
                            f"determine what country/region this real estate listing is from.\n\n"
                            f"Location field: \"{location}\"\n"
                            f"Title: \"{listing.get('title', '')}\"\n"
                            f"Description: \"{llm_text[:600]}\"\n\n"
                            f"Answer with ONE of:\n"
                            f"- BALI if listing is clearly in Bali or Indonesia\n"
                            f"- NOT_BALI if listing is from another country/region (USA, Europe, Asia, etc.)\n"
                            f"- UNKNOWN if impossible to determine\n\n"
                            f"COUNTRY:"
                        )
                        try:
                            geo_answer = llm_filter.client.generate_text(loc_prompt).strip().upper()
                            import re as _re
                            geo_code = "UNKNOWN"
                            if _re.search(r"\bBALI\b", geo_answer):
                                geo_code = "BALI"
                            elif _re.search(r"\bNOT_BALI\b", geo_answer):
                                geo_code = "NOT_BALI"
                            logger.info(f"  GEO CHECK: location='{location}' → LLM says: {geo_code} (raw: {geo_answer[:80]})")
                            if geo_code == "NOT_BALI":
                                passed = False
                                reason = f"REJECT_LOCATION (LLM geo-check: not Bali, location='{location}')"
                                logger.info(f"  ✗ FILTERED: {reason} → status: stage3_failed")
                            elif geo_code == "UNKNOWN":
                                logger.info(f"  ⚠ GEO UNKNOWN — passing to main LLM filter")
                        except Exception as geo_err:
                            logger.warning(f"  ⚠ GEO CHECK failed: {geo_err} — skipping geo filter")

                # If location check passed, run OpenRouter analysis
                if passed:
                    try:
                        passed, reason, model_used = llm_filter.filter(llm_text)
                        reject_type = _extract_reject_type(reason)

                        # Deterministic overrides for known false negatives.
                        # IMPORTANT: never override REJECT_LOCATION — geo check is authoritative.
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
                            "UPDATE listings SET status = %s, llm_reason = %s, pass_reason = %s, llm_passed = %s, llm_model = %s, llm_analyzed_at = NOW() WHERE fb_id = %s",
                            (new_status, reason, pass_reason, passed, model_used, fb_id)
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
                        try:
                            db.conn.rollback()
                        except Exception:
                            pass
                        logger.error(f"Stopping Stage 3 due to LLM error (fb_id={fb_id}).")
                        raise e # Re-raise to be caught by outer try-except
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
        logger.info("=" * 80)

    except Exception as e:
        error_msg = f"Unexpected error in Stage 3 LLM: {e}"
        logger.error(error_msg, exc_info=True)
        if notifier:
            import traceback
            full_error = f"{error_msg}\n\n{traceback.format_exc()[-500:]}"
            notifier.send_error("Stage 3 LLM", full_error)
        sys.exit(1)


if __name__ == '__main__':
    main()