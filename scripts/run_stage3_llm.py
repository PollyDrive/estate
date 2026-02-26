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
# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from llm_filters import OpenRouterFilter
from telegram_notifier import TelegramNotifier
from config_loader import load_config
from property_parser import PropertyParser

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
    config = load_config()
    parser = PropertyParser(config)

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
        # Sync chat_profiles from config → DB
        chat_profiles_cfg = config.get('chat_profiles', []) or []
        with Database() as db:
            if chat_profiles_cfg:
                db.sync_chat_profiles(chat_profiles_cfg)
                logger.info(f"✓ Synced {len(chat_profiles_cfg)} chat profiles to DB")
            enabled_profiles = db.get_enabled_chat_profiles()
        logger.info(f"✓ Loaded {len(enabled_profiles)} enabled profiles: "
                    f"{[p['name'] for p in enabled_profiles]}")

        # Build union of allowed_locations from all enabled profiles (for geo-check)
        all_allowed_locations = []
        for p in enabled_profiles:
            for loc in (p.get('allowed_locations') or []):
                if loc not in all_allowed_locations:
                    all_allowed_locations.append(loc)

        if not enabled_profiles:
            logger.error("No enabled chat profiles found in DB. Add profiles to config/profiles.json and re-run.")
            sys.exit(1)

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
        enforce_stop_locations = bool(config.get('filters', {}).get('enforce_stop_locations', False))

        with Database() as db:
            for listing in listings:
                fb_id = listing['fb_id']
                description = listing.get('description') or ''
                title = listing.get('title') or ''
                location = listing.get('location') or ''
                llm_text = description.strip() if description and description.strip() else title.strip()
                parse_text = f"{title} {description}".strip()
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

                # Check location against global stop_locations FIRST
                stop_locations = config.get('filters', {}).get('stop_locations', [])
                allowed_locations_lower = [a.lower() for a in all_allowed_locations]
                location_lower = location.lower() if location else ''

                passed = True
                reason = None

                # Pre-filter: stop_locations (deterministic, no LLM call needed).
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
                if passed and all_allowed_locations:
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
                        # Secondary structured parse to avoid carrying unknown bedrooms
                        # into stage4 where profile checks require concrete bedroom counts.
                        parsed = parser.parse(parse_text) if parse_text else {}
                        parsed_bedrooms = parsed.get("bedrooms")
                        current_bedrooms = listing.get("bedrooms")
                        try:
                            parsed_bedrooms = int(parsed_bedrooms) if parsed_bedrooms is not None else None
                        except Exception:
                            parsed_bedrooms = None
                        try:
                            current_bedrooms = int(current_bedrooms) if current_bedrooms is not None else None
                        except Exception:
                            current_bedrooms = None

                        # Replace NULL/invalid bedroom values with sane parsed value.
                        # Prevents anomalies like postal codes being treated as bedrooms.
                        if parsed_bedrooms is not None and 0 <= parsed_bedrooms <= 20:
                            if current_bedrooms is None or current_bedrooms < 0 or current_bedrooms > 20:
                                listing["bedrooms"] = parsed_bedrooms

                        passed, reason, model_used = llm_filter.filter(llm_text)
                        reject_type = _extract_reject_type(reason)

                        # Update status based on LLM result
                        new_status = 'stage3' if passed else 'stage3_failed'
                        pass_reason = reason if passed else f"{_extract_reject_type(reason)} | {reason}"

                        # Save LLM analysis result
                        db.cursor.execute(
                            "UPDATE listings SET status = %s, llm_reason = %s, pass_reason = %s, llm_passed = %s, llm_model = %s, bedrooms = %s, llm_analyzed_at = NOW() WHERE fb_id = %s",
                            (new_status, reason, pass_reason, passed, model_used, listing.get("bedrooms"), fb_id)
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

                # listing_profiles are filled in stage4 --chat, not here.

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