#!/usr/bin/env python3
"""
STAGE 4: Per-profile filtering and Russian summary generation.

Usage:
    python scripts/run_stage4.py --chat <chat_id>

For each listing with status IN ('stage3', 'stage4') that has NOT yet been
evaluated for the given chat profile:
  1. Run check_profile_criteria (bedrooms, price, allowed_locations, stop_locations).
  2. If passed AND summary_ru IS NULL → generate a brief Russian summary.
  3. Promote listing status to 'stage4' (if it was 'stage3').
  4. Write result to listing_profiles (fb_id × chat_id → passed/reason).

Profile-specific criteria (bedrooms, price, allowed_locations) are handled
ONLY here, not in stage2 or stage3.
"""

import argparse
import os
import sys
import logging
import time
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from llm_filters import OpenRouterClient
from config_loader import load_config
from profile_filter import check_profile_criteria
from property_parser import PropertyParser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage4.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def generate_summary_ru(
    listing: dict,
    or_client: OpenRouterClient,
    config: dict,
    last_request_time: dict,
) -> str:
    """
    Generate a brief Russian summary using OpenRouter.

    Args:
        listing: Listing dict with title, description, price, location, etc.
        or_client: Initialized OpenRouterClient instance.
        config: Full config dict.
        last_request_time: Mutable dict tracking last API call time for rate-limiting.

    Returns:
        Bullet-list Russian summary string, or a short fallback on error.
    """
    try:
        or_cfg = (config.get("llm", {}) or {}).get("openrouter", {}) or {}
        request_delay = float(or_cfg.get("request_delay", 1.0))

        # Rate limiting
        if last_request_time.get('time'):
            elapsed = time.time() - last_request_time['time']
            if elapsed < request_delay:
                wait_time = request_delay - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                time.sleep(wait_time)

        title       = str(listing.get('title') or 'N/A')
        price       = str(listing.get('price') or 'N/A')
        location    = str(listing.get('location') or 'N/A')
        description = str(listing.get('description') or 'N/A')

        full_text = (
            f"Заголовок: {title}\n"
            f"Цена: {price}\n"
            f"Локация: {location}\n"
            f"Описание: {description[:800]}"
        )

        prompt = f"""Извлеки из объявления ключевую информацию и верни СТРОГО в формате списка с маркерами.

ФОРМАТ (используй ТОЛЬКО маркеры •):
• [количество] спальни/спален
• [район, город]
• [удобства через запятую: бассейн, кухня, AC, WiFi и т.д.]
• [цена]/мес

ПРАВИЛА:
- Каждый пункт начинается с •
- Каждый пункт на новой строке
- БЕЗ лишних слов и предложений
- Если информация отсутствует - пропускай пункт
- НЕ добавляй эмодзи
- НЕ добавляй комментарии

ПРИМЕР:
• 2 спальни
• Убуд, Бали
• Бассейн, кухня, AC, WiFi
• 12 млн IDR/мес

Текст объявления:
{full_text}

СПИСОК:"""

        summary = or_client.generate_text(prompt, model=or_cfg.get("model"))
        last_request_time['time'] = time.time()
        return (summary or "").strip()

    except Exception as e:
        logger.error(f"OpenRouter summary error for {listing.get('fb_id')}: {e}")
        desc = str(listing.get('description') or 'Нет описания')[:150]
        return f"Описание: {desc}..."


def main():
    """Run Stage 4: per-profile filtering and summary generation."""

    parser = argparse.ArgumentParser(
        description="Stage 4: per-profile filter + summary generation"
    )
    parser.add_argument(
        '--chat', type=int, required=True,
        metavar='CHAT_ID',
        help='Telegram chat_id of the profile to process (required)'
    )
    parser.add_argument(
        '--include-room-only', action='store_true', default=False,
        help=(
            'Include stage3_room_only listings in profile evaluation. '
            'Use for 1BR profiles where guesthouses/rooms may be relevant. '
            'Without this flag, stage3_room_only listings are auto-rejected.'
        )
    )
    args = parser.parse_args()
    chat_id          = args.chat
    include_room_only = args.include_room_only

    logger.info("=" * 80)
    logger.info(
        f"STAGE 4: Per-profile Filter + Summary  [chat_id={chat_id}"
        + ("  +room-only" if include_room_only else "") + "]"
    )
    logger.info("=" * 80)

    load_dotenv()
    config = load_config()
    prop_parser = PropertyParser(config)

    db_url             = os.getenv('DATABASE_URL')
    openrouter_api_key = os.getenv('OPENROUTER_API_KEY')

    if not all([db_url, openrouter_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, OPENROUTER_API_KEY)!")
        sys.exit(1)

    # ── Load and sync the target profile ─────────────────────────────────────
    chat_profiles_cfg = config.get('chat_profiles', []) or []
    with Database() as db:
        if chat_profiles_cfg:
            db.sync_chat_profiles(chat_profiles_cfg)
            logger.info(f"✓ Synced {len(chat_profiles_cfg)} chat profiles to DB")

        profile = db.get_chat_profile(chat_id)

    if not profile:
        logger.error(
            f"Chat profile {chat_id} not found in DB. "
            f"Add it to config/chat_profiles or config.json and re-run."
        )
        sys.exit(1)

    logger.info(
        f"✓ Profile: '{profile['name']}'  "
        f"beds={profile['bedrooms_min']}–{profile.get('bedrooms_max') or '∞'}  "
        f"price_max={profile['price_max']:,}  "
        f"locations={len(profile.get('allowed_locations') or [])}"
    )

    # ── Initialize OpenRouter client ─────────────────────────────────────────
    or_client = OpenRouterClient(config, openrouter_api_key)
    logger.info("✓ OpenRouter client initialized")

    # ── Fetch listings not yet evaluated for this chat ───────────────────────
    # Candidates:
    #   • status IN ('stage3', 'stage4') — passed global LLM, need profile check
    #   • status = 'stage3_room_only'    — included only when --include-room-only is set;
    #     otherwise auto-rejected without profile check (unsuitable for 4BR profiles)
    # Also re-evaluates stale PASS rows (reason='PASS', not yet sent).
    statuses = ['stage3', 'stage4', 'stage3_room_only'] if include_room_only else ['stage3', 'stage4']
    statuses_sql = ','.join(f"'{s}'" for s in statuses)

    with Database() as db:
        db.cursor.execute(
            f"""
            SELECT l.fb_id, l.title, l.description, l.location,
                   l.price, l.price_extracted, l.bedrooms, l.summary_ru,
                   l.phone_number, l.listing_url, l.source, l.status
            FROM listings l
            LEFT JOIN listing_profiles lp
                   ON lp.fb_id = l.fb_id AND lp.chat_id = %(chat_id)s
            WHERE l.status IN ({statuses_sql})
              AND (
                    lp.fb_id IS NULL                        -- never evaluated
                    OR (
                        lp.passed = TRUE
                        AND lp.reason = 'PASS'
                        AND lp.sent_at IS NULL              -- not yet sent → safe to re-evaluate
                    )
                  )
            ORDER BY l.created_at DESC
            """,
            {'chat_id': chat_id}
        )
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]

    if not listings:
        logger.info("No new listings to evaluate for this profile.")
        return

    room_only_count = sum(1 for l in listings if l['status'] == 'stage3_room_only')
    regular_count   = len(listings) - room_only_count
    logger.info(
        f"Found {len(listings)} listings to evaluate for profile '{profile['name']}' "
        f"({regular_count} regular"
        + (f", {room_only_count} room-only (included via --include-room-only)" if room_only_count else "")
        + ")"
    )

    # ── Process ───────────────────────────────────────────────────────────────
    passed_count   = 0
    rejected_count = 0
    summary_count  = 0
    promoted_count = 0

    last_request_time = {'time': None}

    with Database() as db:
        for listing in listings:
            fb_id       = listing['fb_id']
            title       = listing.get('title') or ''
            description = listing.get('description') or ''
            lst_status  = listing.get('status') or ''

            logger.info(f"\nEvaluating {fb_id}: {title[:60] or 'No title'}")

            # stage3_room_only — room/guesthouse listings.
            # Without --include-room-only: auto-reject (not a standalone property → unsuitable for 4BR).
            # With    --include-room-only: run through full profile check (1BR profiles may want these).
            if lst_status == 'stage3_room_only' and not include_room_only:
                reason = 'REJECT_ROOM_ONLY (global: room/guesthouse, not a standalone property)'
                logger.info(f"  ✗ ROOM_ONLY (global) — skipping profile check")
                rejected_count += 1
                db.upsert_listing_profile(fb_id, chat_id, False, reason)
                continue
            # If include_room_only=True, fall through to normal profile check below

            # Price fallback for profile check:
            # if price_extracted is missing or clearly malformed, derive it from
            # title/price/description text (stage4 has richer text than earlier stages).
            raw_price_extracted = listing.get('price_extracted')
            try:
                numeric_price = float(raw_price_extracted) if raw_price_extracted is not None else None
            except (TypeError, ValueError):
                numeric_price = None

            if numeric_price is None or numeric_price < 100_000:
                parse_text = "\n".join([
                    str(title or ""),
                    f"Price: {listing.get('price') or ''}",
                    str(description or ""),
                ]).strip()
                parsed = prop_parser.parse(parse_text) if parse_text else {}
                parsed_price = parsed.get('price')
                try:
                    parsed_price = float(parsed_price) if parsed_price is not None else None
                except (TypeError, ValueError):
                    parsed_price = None

                if parsed_price is not None and parsed_price > 0:
                    listing['price_extracted'] = parsed_price
                    db.cursor.execute(
                        "UPDATE listings SET price_extracted = %s, updated_at = NOW() WHERE fb_id = %s",
                        (parsed_price, fb_id),
                    )
                    db.conn.commit()
                    logger.info(f"  → Re-extracted price_extracted={parsed_price:,.0f} from text")

            # 1. Profile-specific criteria check
            p_passed, p_reason = check_profile_criteria(
                listing, profile, description=description
            )

            if p_passed:
                logger.info(f"  ✓ PASS — {p_reason}")
                passed_count += 1

                # 2. Generate summary if not yet done
                if not listing.get('summary_ru'):
                    logger.info("  → Generating Russian summary...")
                    summary_ru = generate_summary_ru(listing, or_client, config, last_request_time)
                    logger.info(f"  → Summary: {summary_ru[:100]}...")

                    db.cursor.execute(
                        "UPDATE listings SET summary_ru = %s WHERE fb_id = %s",
                        (summary_ru, fb_id)
                    )
                    db.conn.commit()
                    summary_count += 1
                else:
                    logger.info("  → summary_ru already exists, skipping generation")

            else:
                logger.info(f"  ✗ REJECT — {p_reason}")
                rejected_count += 1

            # 3. Promote status from 'stage3' → 'stage4' (idempotent for already-stage4)
            db.cursor.execute(
                """
                UPDATE listings
                SET status = 'stage4', updated_at = NOW()
                WHERE fb_id = %s AND status = 'stage3'
                """,
                (fb_id,)
            )
            if db.cursor.rowcount > 0:
                promoted_count += 1
                logger.info(f"  ↑ Promoted status: stage3 → stage4")
            db.conn.commit()

            # 4. Write / overwrite listing_profiles row for this chat
            db.upsert_listing_profile(fb_id, chat_id, p_passed, p_reason)

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 80)
    logger.info("STAGE 4 COMPLETE")
    logger.info(f"  Profile:          '{profile['name']}' (chat_id={chat_id})")
    logger.info(f"  Listings checked: {len(listings)}")
    logger.info(f"    ↳ regular:      {regular_count}")
    logger.info(f"    ↳ room-only:    {room_only_count} (global reject, no profile check)")
    logger.info(f"  Passed:           {passed_count}")
    logger.info(f"  Rejected:         {rejected_count}")
    logger.info(f"  Summaries gen:    {summary_count}")
    logger.info(f"  Promoted s3→s4:   {promoted_count}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
