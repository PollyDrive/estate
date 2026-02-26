#!/usr/bin/env python3
"""
STAGE 5: Send listings to Telegram (with batching and quiet hours)
Sends stage4 listings in batches for a specific chat profile,
routing through listing_profiles table.

Usage:
  python scripts/run_stage5.py --chat <chat_id>
"""

import argparse
import os
import sys
import time
import logging
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from telegram_notifier import TelegramNotifier
from config_loader import load_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage5.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def is_quiet_hours(config: dict) -> bool:
    """Check if current time is within quiet hours."""
    quiet_config = config.get('quiet_hours', {})
    start_hour = quiet_config.get('start_hour', 0)
    end_hour = quiet_config.get('end_hour', 7)

    gmt_plus_8 = timezone(timedelta(hours=8))
    current_time = datetime.now(gmt_plus_8)
    current_hour = current_time.hour

    if start_hour <= current_hour < end_hour:
        logger.info(f"⏰ Quiet hours active ({current_hour}:00 GMT+8). Skipping send.")
        return True

    return False


def format_regular_message(listing: dict) -> str:
    """Format regular listing message with Russian summary."""
    summary_ru = listing.get('summary_ru', 'Описание не сгенерировано')
    price = listing.get('price', 'Цена не указана')
    phone = listing.get('phone_number', 'Не указан')
    url = listing.get('listing_url', '')

    message = f"""🏡 *Новый вариант!*

{summary_ru}

💰 *Цена:* {price}
📞 *Телефон:* {phone}
🔗 *Ссылка:* {url}"""

    return message


def stage5_guard_reason(listing: dict, config: dict, db: Database, profile: dict) -> str | None:
    """
    Final safety net before Telegram sending.
    Returns reject reason string if listing must be blocked, otherwise None.
    Uses profile's price_max for the price guard.
    """
    filters_cfg = config.get("filters", {}) or {}
    guard_cfg = filters_cfg.get("stage5_guard", {}) or {}
    if not bool(guard_cfg.get("enabled", True)):
        return None

    title = str(listing.get("title") or "")
    description = str(listing.get("description") or "")
    location = str(listing.get("location") or "")
    price_text = str(listing.get("price") or "")
    phone = str(listing.get('phone_number') or '').strip()
    bedrooms = listing.get('bedrooms')
    price_extracted = listing.get('price_extracted')
    listing_payload = {
        "title": title,
        "description": description,
        "location": location,
        "price": price_text,
    }

    regex_rules = guard_cfg.get("regex_rules", []) or []
    for rule in regex_rules:
        pattern = str(rule.get("regex") or "").strip()
        reason = str(rule.get("reason") or "REJECT_STAGE5: regex rule")
        fields = rule.get("fields") or ["title", "description", "location", "price"]
        if not pattern:
            continue
        try:
            haystack = "\n".join(str(listing_payload.get(f, "")) for f in fields)
            if re.search(pattern, haystack, re.IGNORECASE):
                return reason
        except re.error:
            logger.warning(f"Invalid regex in filters.stage5_guard.regex_rules: {pattern}")

    blocked_locations = [str(x).strip().lower() for x in (guard_cfg.get("blocked_locations") or []) if str(x).strip()]
    location_joined = f"{title}\n{description}\n{location}".lower()
    for blocked in blocked_locations:
        if blocked and blocked in location_joined:
            return f"REJECT_STAGE5: blocked location {blocked}"

    # Use profile's price_max as the price guard (required field, no fallback)
    try:
        max_price = float(profile['price_max'])
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"profile missing valid price_max: {e}. Skipping price guard.")
        max_price = None

    if max_price is not None:
        try:
            if price_extracted is not None and float(price_extracted) > max_price:
                return f"REJECT_STAGE5: price_extracted>{int(max_price)}"
        except (TypeError, ValueError):
            pass

    # Duplicate protection: same phone+location+bedrooms already sent to this chat.
    duplicate_check_enabled = bool(guard_cfg.get("duplicate_check", True))
    if duplicate_check_enabled and phone:
        db.cursor.execute(
            """
            SELECT 1
            FROM listing_profiles lp
            JOIN listings l ON l.fb_id = lp.fb_id
            WHERE lp.chat_id = %s
              AND lp.sent_at IS NOT NULL
              AND l.fb_id <> %s
              AND COALESCE(l.phone_number, '') = %s
              AND COALESCE(l.location, '') = %s
              AND COALESCE(l.bedrooms, -1) = COALESCE(%s, -1)
            LIMIT 1
            """,
            (profile['chat_id'], listing.get('fb_id'), phone, location, bedrooms),
        )
        if db.cursor.fetchone():
            return str(guard_cfg.get("duplicate_reason") or "REJECT_STAGE5: duplicate phone/location/bedrooms")

    return None


def main():
    """Run Stage 5: Send listings to Telegram for a specific chat profile."""

    logger.info("=" * 80)
    logger.info("STAGE 5: Telegram Notifications (Batched)")
    logger.info("=" * 80)

    # Parse --chat argument (required)
    parser_cli = argparse.ArgumentParser(description="Stage 5 Telegram sender")
    parser_cli.add_argument(
        "--chat", type=int, required=True,
        help="chat_id of the profile to send listings for (required)"
    )
    args = parser_cli.parse_args()
    target_chat_id = args.chat

    # Load environment
    load_dotenv()

    # Load config
    config = load_config()

    telegram_config = config.get('telegram', {})
    batch_size = telegram_config.get('batch_size', 10)
    delay_between_messages = telegram_config.get('delay_between_messages', 2)

    # Check quiet hours
    if is_quiet_hours(telegram_config):
        logger.info("=" * 80)
        logger.info("Exiting due to quiet hours (00:00-07:00 GMT+8)")
        logger.info("=" * 80)
        sys.exit(0)

    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')

    if not all([db_url, telegram_token]):
        logger.error("Missing required environment variables (DATABASE_URL, TELEGRAM_BOT_TOKEN)!")
        sys.exit(1)

    db = Database()
    db.connect()

    try:
        # Sync chat profiles and load target profile
        chat_profiles_cfg = config.get('chat_profiles', []) or []
        if chat_profiles_cfg:
            db.sync_chat_profiles(chat_profiles_cfg)

        profile = db.get_chat_profile(target_chat_id)
        if not profile:
            logger.error(f"No chat profile found for chat_id={target_chat_id}")
            sys.exit(1)

        profile_name = profile.get('name', str(target_chat_id))
        logger.info(f"✓ Profile loaded: '{profile_name}' (chat_id={target_chat_id})")

        # Initialize Telegram notifier for this chat
        notifier = TelegramNotifier(telegram_token, str(target_chat_id), config)
        logger.info(f"✓ Telegram notifier initialized for chat {target_chat_id}")
        logger.info(f"📦 Batch size: {batch_size}")
        logger.info(f"⏱️  Delay between messages: {delay_between_messages}s")

        # Log batch start with chat_id
        batch_date = date.today()
        batch_number = db.get_batch_count_today() + 1
        query_batch_start = """
            INSERT INTO batch_runs (batch_date, batch_number, started_at, status, chat_id)
            VALUES (%s, %s, NOW(), 'running', %s)
            RETURNING id
        """
        db.cursor.execute(query_batch_start, (batch_date, batch_number, target_chat_id))
        db.conn.commit()
        batch_run_id = db.cursor.fetchone()[0]
        logger.info(f"📊 Starting batch run #{batch_number} for {batch_date}, chat={target_chat_id}")

        sent_count = 0
        error_count = 0
        blocked_count = 0

        # Get pending listings for this chat via listing_profiles
        listings = db.get_listings_for_chat(target_chat_id, batch_size=batch_size)
        total_pending = len(listings)

        logger.info(f"\n📊 Pending listings for chat {target_chat_id}: {total_pending}")

        if total_pending == 0:
            logger.info("✓ No listings to send in this batch.")
        else:
            logger.info(f"\nFetching up to {batch_size} pending listings...")

            for i, listing in enumerate(listings, 1):
                fb_id = listing['fb_id']
                lp_id = listing.get('lp_id')

                try:
                    guard_reason = stage5_guard_reason(listing, config, db, profile)
                    if guard_reason:
                        # Mark rejected in listing_profiles (set sent_at=NULL, update reason)
                        db.cursor.execute(
                            """
                            UPDATE listing_profiles
                            SET passed = FALSE, reason = %s
                            WHERE fb_id = %s AND chat_id = %s
                            """,
                            (guard_reason, fb_id, target_chat_id),
                        )
                        db.conn.commit()
                        blocked_count += 1
                        logger.warning(f"✗ [{i}/{total_pending}] BLOCKED by stage5 guard: {fb_id} | {guard_reason}")
                        continue

                    message = format_regular_message(listing)

                    # Send to Telegram and get message_id
                    message_id = notifier.send_message(message)

                    if message_id:
                        # Mark sent in listing_profiles
                        db.mark_profile_sent(fb_id, target_chat_id, message_id)

                        # Update global listing status to stage5_sent (first send only)
                        db.cursor.execute(
                            """
                            UPDATE listings
                            SET status = 'stage5_sent',
                                telegram_sent = TRUE,
                                telegram_sent_at = COALESCE(telegram_sent_at, NOW()),
                                telegram_message_id = COALESCE(telegram_message_id, %s)
                            WHERE fb_id = %s
                            """,
                            (message_id, fb_id),
                        )
                        db.conn.commit()

                        created_at = listing.get('created_at', 'unknown')
                        logger.info(f"✓ [{i}/{total_pending}] SENT: {fb_id} (msg_id: {message_id})")
                        sent_count += 1

                        # Delay between messages (except for last one)
                        if i < total_pending:
                            logger.debug(f"  ⏱️  Waiting {delay_between_messages}s before next message...")
                            time.sleep(delay_between_messages)
                    else:
                        logger.error(f"✗ [{i}/{total_pending}] FAILED to send: {fb_id}")
                        error_count += 1

                except Exception as e:
                    logger.error(f"✗ [{i}/{total_pending}] ERROR sending {fb_id}: {e}")
                    error_count += 1

        # Complete batch run
        db.cursor.execute(
            """
            UPDATE batch_runs
            SET finished_at = NOW(),
                listings_sent = %s,
                no_desc_sent = 0,
                blocked_count = %s,
                error_count = %s,
                status = 'completed'
            WHERE id = %s
            """,
            (sent_count, blocked_count, error_count, batch_run_id),
        )
        db.conn.commit()

        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 5 COMPLETE")
        logger.info(f"Profile: {profile_name} (chat_id={target_chat_id})")
        logger.info(f"Listings sent: {sent_count}")
        logger.info(f"Blocked by guard: {blocked_count}")
        logger.info(f"Errors: {error_count}")
        logger.info("=" * 80)

    finally:
        db.close()


if __name__ == '__main__':
    main()
