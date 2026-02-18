#!/usr/bin/env python3
"""
Quick script to send feedback statistics NOW (bypasses time/batch checks)
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from telegram_notifier import TelegramNotifier

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
    print("ERROR: Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    sys.exit(1)

# Initialize services
db = Database()
db.connect()

notifier = TelegramNotifier(telegram_token, telegram_chat_id, config)

try:
    # Get statistics
    stats = db.get_feedback_stats()
    batch_count = db.get_batch_count_today()

    # Build message
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
    else:
        message += f"Всего объявлений с реакциями: {total}\n\n"
        message += f"❤️ Хорошие варианты: {good_count}\n"
        message += f"💩 Плохие варианты: {bad_count}\n"
        message += f"🤡 Ошибки (требуют правки): {error_count}\n"

        # Add links to error listings
        if error_count > 0:
            message += f"\n*🤡 Объявления с ошибками:*\n"
            error_listings = stats.get('🤡', {}).get('listings', [])

            # Show top 5 error listings
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

    # Send to Telegram
    message_id = notifier.send_message(message)

    if message_id:
        print(f"✓ Statistics sent successfully (message_id: {message_id})")
        print(f"\nSummary:")
        print(f"  Batches today: {batch_count}")
        print(f"  ❤️ Good: {good_count}")
        print(f"  💩 Bad: {bad_count}")
        print(f"  🤡 Errors: {error_count}")
    else:
        print("✗ Failed to send statistics")
        sys.exit(1)

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    db.close()
