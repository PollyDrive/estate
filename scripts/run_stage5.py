#!/usr/bin/env python3
"""
STAGE 5: Send listings to Telegram (with batching and quiet hours)
Sends stage4 listings in batches, respecting quiet hours and delays.
"""

import os
import sys
import time
import logging
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from telegram_notifier import TelegramNotifier
import json

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
    """
    Check if current time is within quiet hours.
    
    Args:
        config: Telegram configuration with quiet_hours settings
        
    Returns:
        True if in quiet hours, False otherwise
    """
    quiet_config = config.get('quiet_hours', {})
    start_hour = quiet_config.get('start_hour', 0)
    end_hour = quiet_config.get('end_hour', 7)
    
    # Get current time in GMT+8 (Asia/Singapore)
    # GMT+8 = UTC+8
    gmt_plus_8 = timezone(timedelta(hours=8))
    current_time = datetime.now(gmt_plus_8)
    current_hour = current_time.hour
    
    # Check if in quiet hours (midnight to 7am GMT+8)
    if start_hour <= current_hour < end_hour:
        logger.info(f"⏰ Quiet hours active ({current_hour}:00 GMT+8). Skipping send.")
        return True
    
    return False


def format_regular_message(listing: dict) -> str:
    """
    Format regular listing message with Russian summary.
    
    Args:
        listing: Listing dictionary
        
    Returns:
        Formatted Telegram message
    """
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


def format_no_description_batch(listings: list) -> str:
    """
    Format batch of no_description listings as simple links.
    
    Args:
        listings: List of listing dictionaries (up to 5)
        
    Returns:
        Formatted Telegram message with links
    """
    message = "📝 *Найдены объявления без описания*\n\n"
    
    for listing in listings:
        url = listing.get('listing_url', '')
        price = listing.get('price', '')
        title = listing.get('title', '')
        
        # Start with bullet point and title/URL
        if title:
            message += f"• {title}\n"
        else:
            message += f"• Объявление\n"
        
        # Add price if available
        if price:
            message += f"  💰 {price}\n"
        
        # Add URL
        message += f"  🔗 {url}\n\n"
    
    return message.strip()


def build_pipeline_stats_message(db: Database) -> str:
    """
    Build cumulative pipeline stats message.

    Format requested by user:
    Всего обработано объявлений: <n>
    Распределение по этапам:
    filter_failed: <n>
    llm_failed: <n>
    duplicates: <n>
    sent: <n>
    no_description queue: <n>
    """
    db.cursor.execute("SELECT COUNT(*) FROM listings")
    listings_total = int(db.cursor.fetchone()[0] or 0)

    # Use one non-listings table to avoid double counting mirrored records.
    db.cursor.execute("SELECT COUNT(*) FROM listing_non_relevant")
    non_listings_total = int(db.cursor.fetchone()[0] or 0)

    db.cursor.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'stage2_failed') AS filter_failed,
            COUNT(*) FILTER (WHERE status = 'stage3_failed') AS llm_failed,
            COUNT(*) FILTER (WHERE status = 'stage4_duplicate') AS duplicates_cnt,
            COUNT(*) FILTER (WHERE status = 'stage5_sent') AS sent_cnt,
            COUNT(*) FILTER (
                WHERE status = 'no_description'
                  AND (telegram_sent IS NULL OR telegram_sent = FALSE)
            ) AS no_description_queue
        FROM listings
        """
    )
    row = db.cursor.fetchone() or (0, 0, 0, 0, 0)
    filter_failed, llm_failed, duplicates_cnt, sent_cnt, no_description_queue = [int(x or 0) for x in row]

    total_processed = listings_total + non_listings_total
    return (
        f"Всего обработано объявлений: {total_processed}\n"
        "Распределение по этапам:\n"
        f"filter_failed: {filter_failed}\n"
        f"llm_failed: {llm_failed}\n"
        f"duplicates: {duplicates_cnt}\n"
        f"sent: {sent_cnt}\n"
        f"no_description queue: {no_description_queue}"
    )


def stage5_guard_reason(listing: dict, config: dict, db: Database) -> str | None:
    """
    Final safety net before Telegram sending.
    Returns reject reason string if listing must be blocked, otherwise None.
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

    try:
        max_price = float(guard_cfg.get("max_price", config.get("criterias", {}).get("price_max", 40000000)))
    except Exception:
        max_price = 40000000.0

    try:
        if price_extracted is not None and float(price_extracted) > max_price:
            return f"REJECT_STAGE5: price_extracted>{int(max_price)}"
    except Exception:
        pass

    # Duplicate protection: same phone+location+bedrooms already sent.
    duplicate_check_enabled = bool(guard_cfg.get("duplicate_check", True))
    if duplicate_check_enabled and phone:
        db.cursor.execute(
            """
            SELECT 1
            FROM listings
            WHERE status = 'stage5_sent'
              AND fb_id <> %s
              AND COALESCE(phone_number, '') = %s
              AND COALESCE(location, '') = %s
              AND COALESCE(bedrooms, -1) = COALESCE(%s, -1)
            LIMIT 1
            """,
            (listing.get('fb_id'), phone, location, bedrooms),
        )
        if db.cursor.fetchone():
            return str(guard_cfg.get("duplicate_reason") or "REJECT_STAGE5: duplicate phone/location/bedrooms")

    return None


def check_and_send_no_description(db: Database, notifier: TelegramNotifier) -> int:
    """
    Check for no_description listings that passed all filters and send to Telegram.
    Sends in batches of 5 listings per message.
    
    Args:
        db: Database connection
        notifier: TelegramNotifier instance
        
    Returns:
        Number of no_description listings sent
    """
    logger.info("\n" + "=" * 80)
    logger.info("Checking for no_description listings...")
    logger.info("=" * 80)
    
    # Get no_description listings that haven't been sent to Telegram
    query = """
        SELECT fb_id, title, price, listing_url, created_at
        FROM listings
        WHERE status = 'no_description'
          AND telegram_sent = FALSE
        ORDER BY created_at ASC
    """
    
    db.cursor.execute(query)
    columns = [desc[0] for desc in db.cursor.description]
    listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
    
    total_count = len(listings)
    logger.info(f"Found {total_count} no_description listings pending notification")
    
    if total_count == 0:
        logger.info("✓ No no_description listings to send")
        return 0
    
    sent_count = 0
    batch_size = 5
    
    # Split into batches of 5
    for i in range(0, total_count, batch_size):
        batch = listings[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_count + batch_size - 1) // batch_size
        
        logger.info(f"\nSending batch {batch_num}/{total_batches} ({len(batch)} listings)...")
        
        try:
            message = format_no_description_batch(batch)
            message_id = notifier.send_message(message)

            if message_id:
                # Mark all listings in this batch as sent
                fb_ids = [listing['fb_id'] for listing in batch]
                placeholders = ', '.join(['%s'] * len(fb_ids))
                db.cursor.execute(
                    f"UPDATE listings SET telegram_sent = TRUE, telegram_sent_at = NOW() WHERE fb_id IN ({placeholders})",
                    fb_ids
                )
                db.conn.commit()

                # Note: For no_description batches, we don't save message_id per listing
                # since multiple listings share one message

                sent_count += len(batch)
                logger.info(f"✓ Batch {batch_num}/{total_batches} sent successfully (msg_id: {message_id}, {len(batch)} listings)")

                # Delay between batches (except last one)
                if i + batch_size < total_count:
                    logger.debug("  ⏱️  Waiting 2s before next batch...")
                    time.sleep(2)
            else:
                logger.error(f"✗ Failed to send batch {batch_num}/{total_batches}")
                
        except Exception as e:
            logger.error(f"✗ Error sending batch {batch_num}/{total_batches}: {e}")
    
    logger.info(f"\n✓ Sent {sent_count}/{total_count} no_description listings to Telegram")
    return sent_count


def main():
    """Run Stage 5: Send listings to Telegram with batching"""

    logger.info("=" * 80)
    logger.info("STAGE 5: Telegram Notifications (Batched)")
    logger.info("=" * 80)

    # Load environment
    load_dotenv()

    # Load config
    config_path = 'config/config.json'
    if not os.path.exists(config_path):
        config_path = '/app/config/config.json'

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

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
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

    if not all([db_url, telegram_token, telegram_chat_id]):
        logger.error("Missing required environment variables!")
        sys.exit(1)

    # Initialize Telegram notifier
    notifier = TelegramNotifier(telegram_token, telegram_chat_id, config)
    logger.info("✓ Telegram notifier initialized")
    logger.info(f"📦 Batch size: {batch_size}")
    logger.info(f"⏱️  Delay between messages: {delay_between_messages}s")

    sent_count = 0
    error_count = 0
    blocked_count = 0

    db = Database()
    db.connect()

    # Log batch start
    from datetime import date
    batch_date = date.today()
    batch_number = db.get_batch_count_today() + 1
    batch_run_id = db.log_batch_start(batch_date, batch_number)

    if not batch_run_id:
        logger.error("Failed to log batch start. Continuing anyway...")
        batch_run_id = None

    logger.info(f"📊 Starting batch run #{batch_number} for {batch_date}")
    
    try:
        # Get total count of unsent listings
        db.cursor.execute("""
            SELECT COUNT(*) 
            FROM listings 
            WHERE status = 'stage4' 
              AND (telegram_sent IS NULL OR telegram_sent = FALSE)
        """)
        total_unsent = db.cursor.fetchone()[0]
        logger.info(f"\n📊 Total unsent regular listings: {total_unsent}")
        
        if total_unsent == 0:
            logger.info("✓ No regular listings to send in this batch.")
        
        # Fetch OLDEST unsent listings (FIFO - First In First Out) if any
        if total_unsent > 0:
            logger.info(f"\nFetching up to {batch_size} OLDEST unsent listings...")
            query = """
                SELECT fb_id, title, summary_ru, price, phone_number, listing_url, created_at,
                       description, location, bedrooms, price_extracted, pass_reason, llm_reason
                FROM listings
                WHERE status = 'stage4'
                  AND (telegram_sent IS NULL OR telegram_sent = FALSE)
                ORDER BY created_at ASC
                LIMIT %s
            """
            db.cursor.execute(query, (batch_size,))
            columns = [desc[0] for desc in db.cursor.description]
            listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
            
            logger.info(f"Found {len(listings)} listings to send in this batch")
        else:
            listings = []
        
        # Send each listing with delay
        if listings:
            for i, listing in enumerate(listings, 1):
                fb_id = listing['fb_id']
                
                try:
                    guard_reason = stage5_guard_reason(listing, config, db)
                    if guard_reason:
                        db.cursor.execute(
                            """
                            UPDATE listings
                            SET status = 'stage3_failed',
                                llm_reason = %s,
                                pass_reason = %s,
                                llm_passed = FALSE,
                                llm_analyzed_at = NOW(),
                                updated_at = NOW()
                            WHERE fb_id = %s
                            """,
                            (guard_reason, guard_reason, fb_id),
                        )
                        db.conn.commit()
                        blocked_count += 1
                        logger.warning(f"✗ [{i}/{len(listings)}] BLOCKED by stage5 guard: {fb_id} | {guard_reason}")
                        continue

                    message = format_regular_message(listing)

                    # Send to Telegram and get message_id
                    message_id = notifier.send_message(message)

                    if message_id:
                        # Update status to stage5_sent and mark as sent
                        db.cursor.execute(
                            "UPDATE listings SET status = 'stage5_sent', telegram_sent = TRUE, telegram_sent_at = NOW() WHERE fb_id = %s",
                            (fb_id,)
                        )
                        db.conn.commit()

                        # Save telegram message_id
                        db.save_telegram_message_id(fb_id, message_id)

                        created_at = listing.get('created_at', 'unknown')
                        logger.info(f"✓ [{i}/{len(listings)}] SENT: {fb_id} (msg_id: {message_id}, created: {created_at})")
                        sent_count += 1

                        # Delay between messages (except for last one)
                        if i < len(listings):
                            logger.debug(f"  ⏱️  Waiting {delay_between_messages}s before next message...")
                            time.sleep(delay_between_messages)
                    else:
                        logger.error(f"✗ [{i}/{len(listings)}] FAILED to send: {fb_id}")
                        error_count += 1
                        
                except Exception as e:
                    logger.error(f"✗ [{i}/{len(listings)}] ERROR sending {fb_id}: {e}")
                    error_count += 1
            
            # Check if more listings remain
            remaining = total_unsent - sent_count
            
            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("REGULAR LISTINGS BATCH COMPLETE")
            logger.info(f"Successfully sent: {sent_count}")
            logger.info(f"Errors: {error_count}")
            logger.info(f"Blocked by Stage5 guard: {blocked_count}")
            logger.info(f"Remaining unsent: {remaining}")
            
            if remaining > 0:
                logger.info(f"\n💡 Run again in 30 minutes to send next batch of {min(batch_size, remaining)}")
            else:
                logger.info(f"\n✅ All regular listings sent! No more pending.")
            
            logger.info("=" * 80)
        
        # Check and send no_description listings
        no_desc_sent = check_and_send_no_description(db, notifier)
        
        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 5 COMPLETE")
        logger.info(f"Regular listings sent: {sent_count}")
        logger.info(f"No-description listings sent: {no_desc_sent}")
        logger.info(f"Total sent: {sent_count + no_desc_sent}")
        logger.info("=" * 80)

        # Log batch completion
        if batch_run_id:
            db.log_batch_complete(batch_run_id, sent_count, no_desc_sent, blocked_count, error_count)

        # Send cumulative pipeline stats after each batch run.
        try:
            stats_message = build_pipeline_stats_message(db)
            stats_msg_id = notifier.send_message(stats_message)
            if stats_msg_id:
                logger.info(f"✓ Pipeline stats message sent (msg_id: {stats_msg_id})")
            else:
                logger.warning("✗ Failed to send pipeline stats message")
        except Exception as e:
            logger.warning(f"✗ Could not build/send pipeline stats message: {e}")

    finally:
        db.close()


if __name__ == '__main__':
    main()
