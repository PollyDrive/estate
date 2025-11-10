#!/usr/bin/env python3
"""
STAGE 5: Send listings to Telegram (with batching and quiet hours)
Sends stage4 listings in batches, respecting quiet hours and delays.
"""

import os
import sys
import time
import logging
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
    
    db = Database()
    db.connect()
    
    try:
        # Get total count of unsent listings
        db.cursor.execute("""
            SELECT COUNT(*) 
            FROM listings 
            WHERE status = 'stage4' 
              AND (telegram_sent IS NULL OR telegram_sent = FALSE)
        """)
        total_unsent = db.cursor.fetchone()[0]
        logger.info(f"\n📊 Total unsent listings: {total_unsent}")
        
        if total_unsent == 0:
            logger.info("✓ No listings to send. All caught up!")
            logger.info("=" * 80)
            sys.exit(0)
        
        # Fetch OLDEST unsent listings (FIFO - First In First Out)
        logger.info(f"\nFetching up to {batch_size} OLDEST unsent listings...")
        query = """
            SELECT fb_id, title, summary_ru, price, phone_number, listing_url, created_at
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
        
        if len(listings) == 0:
            logger.info("✓ No listings in this batch. Done!")
            logger.info("=" * 80)
            sys.exit(0)
        
        # Send each listing with delay
        for i, listing in enumerate(listings, 1):
            fb_id = listing['fb_id']
            
            try:
                message = format_regular_message(listing)
                
                # Send to Telegram
                success = notifier.send_message(message)
                
                if success:
                    # Update status to stage5_sent and mark as sent
                    db.cursor.execute(
                        "UPDATE listings SET status = 'stage5_sent', telegram_sent = TRUE, telegram_sent_at = NOW() WHERE fb_id = %s",
                        (fb_id,)
                    )
                    db.conn.commit()
                    
                    created_at = listing.get('created_at', 'unknown')
                    logger.info(f"✓ [{i}/{len(listings)}] SENT: {fb_id} (created: {created_at})")
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
        logger.info("BATCH COMPLETE")
        logger.info(f"Successfully sent: {sent_count}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Remaining unsent: {remaining}")
        
        if remaining > 0:
            logger.info(f"\n💡 Run again in 30 minutes to send next batch of {min(batch_size, remaining)}")
        else:
            logger.info(f"\n✅ All listings sent! No more pending.")
        
        logger.info("=" * 80)
        
    finally:
        db.close()


if __name__ == '__main__':
    main()
