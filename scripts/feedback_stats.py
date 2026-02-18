#!/usr/bin/env python3
"""
Feedback Stats: Generate and send feedback statistics reports.

This script:
1. Checks if it should send stats (after 3 batches or at 21:00)
2. Generates a report with feedback statistics
3. Sends the report to Telegram
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, time
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from telegram_notifier import TelegramNotifier

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/feedback_stats.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def should_send_stats(db: Database) -> bool:
    """
    Check if we should send statistics now.

    Rules:
    - Send after every 3rd completed batch
    - Send at 21:00 if there were any batches today

    Args:
        db: Database connection

    Returns:
        True if should send stats, False otherwise
    """
    batch_count = db.get_batch_count_today()
    current_hour = datetime.now().hour

    # Check if it's 21:00 and we had batches today
    if current_hour == 21 and batch_count > 0:
        logger.info(f"📊 Sending daily summary at 21:00 (batches today: {batch_count})")
        return True

    # Check if we just completed 3rd, 6th, 9th, etc. batch
    if batch_count > 0 and batch_count % 3 == 0:
        logger.info(f"📊 Sending stats after {batch_count} batches")
        return True

    logger.info(f"⏭️  Not sending stats yet (batches: {batch_count}, hour: {current_hour})")
    return False


def generate_stats_report(db: Database) -> str:
    """
    Generate a formatted statistics report.

    Args:
        db: Database connection

    Returns:
        Formatted report message
    """
    stats = db.get_feedback_stats()
    batch_count = db.get_batch_count_today()

    # Build header
    current_time = datetime.now().strftime('%H:%M')
    message = f"📊 *Статистика фидбека* (на {current_time})\n\n"
    message += f"Батчей сегодня: {batch_count}\n\n"

    # Count totals
    good_count = stats.get('❤️', {}).get('message_count', 0)
    bad_count = stats.get('💩', {}).get('message_count', 0)
    error_count = stats.get('🤡', {}).get('message_count', 0)

    total = good_count + bad_count + error_count

    if total == 0:
        message += "Пока нет реакций на объявления.\n"
        return message

    # Summary
    message += f"Всего объявлений с реакциями: {total}\n\n"
    message += f"❤️ Хорошие варианты: {good_count}\n"
    message += f"💩 Плохие варианты: {bad_count}\n"
    message += f"🤡 Ошибки (требуют правки): {error_count}\n"

    # Add links to error listings
    if error_count > 0:
        message += f"\n*🤡 Объявления с ошибками:*\n"
        error_listings = stats.get('🤡', {}).get('listings', [])

        # Show top 5 error listings (sorted by reaction_count)
        for i, listing in enumerate(error_listings[:5], 1):
            url = listing.get('listing_url', 'N/A')
            count = listing.get('reaction_count', 1)
            message += f"{i}. {url} ({count}x)\n"

        if len(error_listings) > 5:
            message += f"... и ещё {len(error_listings) - 5}\n"

    # Add links to bad listings (top 3)
    if bad_count > 0:
        message += f"\n*💩 Плохие варианты (топ-3):*\n"
        bad_listings = stats.get('💩', {}).get('listings', [])

        for i, listing in enumerate(bad_listings[:3], 1):
            url = listing.get('listing_url', 'N/A')
            count = listing.get('reaction_count', 1)
            message += f"{i}. {url} ({count}x)\n"

    return message


def main():
    """Main function to check and send feedback stats."""

    logger.info("=" * 80)
    logger.info("FEEDBACK STATS: Checking if stats should be sent...")
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
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

    if not all([telegram_token, telegram_chat_id]):
        logger.error("Missing required environment variables!")
        sys.exit(1)

    # Initialize services
    db = Database()
    db.connect()

    notifier = TelegramNotifier(telegram_token, telegram_chat_id, config)

    try:
        # Check if we should send stats
        if not should_send_stats(db):
            logger.info("✓ No stats to send at this time")
            sys.exit(0)

        # Generate report
        report = generate_stats_report(db)

        # Send to Telegram
        message_id = notifier.send_message(report)

        if message_id:
            logger.info(f"✓ Feedback stats sent successfully (msg_id: {message_id})")
        else:
            logger.error("✗ Failed to send feedback stats")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error generating/sending stats: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()
