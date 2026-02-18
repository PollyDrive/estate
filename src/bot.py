#!/usr/bin/env python3
"""
Telegram Bot for RealtyBot-Bali
- Handles message reactions (feedback system)
- Provides /stats command for daily statistics
- Runs continuously in Docker
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import MessageReactionUpdated, ReactionTypeEmoji

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from database import Database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Valid reaction emojis for feedback
VALID_REACTIONS = {'❤️', '💩', '🤡'}


async def handle_reaction(event: MessageReactionUpdated, bot: Bot, db: Database):
    """
    Handle MessageReactionUpdated event and save to database.

    Args:
        event: MessageReactionUpdated event from Telegram
        bot: Bot instance
        db: Database connection
    """
    message_id = event.message_id
    chat_id = event.chat.id

    # Get new reactions (added reactions)
    new_reactions = event.new_reaction

    if not new_reactions:
        logger.debug(f"No new reactions for message {message_id}")
        return

    # Process each new reaction
    for reaction in new_reactions:
        # Check if it's an emoji reaction (not custom emoji)
        if isinstance(reaction, ReactionTypeEmoji):
            emoji = reaction.emoji

            # Only track our valid feedback emojis
            if emoji in VALID_REACTIONS:
                # Get fb_id from database using message_id
                fb_id = db.get_fb_id_by_message_id(message_id)

                if fb_id:
                    # Save reaction to database
                    db.save_reaction(message_id, fb_id, emoji)
                    logger.info(f"✓ Saved reaction {emoji} for message {message_id} (fb_id: {fb_id})")
                else:
                    logger.warning(f"⚠️  Message {message_id} not found in database. Skipping reaction {emoji}.")
            else:
                logger.debug(f"Ignoring non-feedback emoji: {emoji}")


def generate_stats_report(db: Database) -> str:
    """
    Generate a formatted statistics report for today.

    Args:
        db: Database connection

    Returns:
        Formatted report message
    """
    stats = db.get_feedback_stats()
    batch_count = db.get_batch_count_today()
    sent_count = db.get_sent_listings_count_today()

    # Build header
    current_time = datetime.now().strftime('%H:%M')
    message = f"📊 *Статистика фидбека* (на {current_time})\n\n"

    if sent_count == 0:
        message += "Сегодня объявления ещё не отправлялись.\n"
        return message

    message += f"Батчей сегодня: {batch_count}\n"
    message += f"Отправлено объявлений: {sent_count}\n\n"

    # Count totals
    good_count = stats.get('❤️', {}).get('message_count', 0)
    bad_count = stats.get('💩', {}).get('message_count', 0)
    error_count = stats.get('🤡', {}).get('message_count', 0)

    total = good_count + bad_count + error_count

    if total == 0:
        message += "Пока нет реакций на объявления.\n"
        return message

    # Summary
    message += f"Получено реакций: {total} из {sent_count}\n\n"
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


async def main():
    """Main function to run the Telegram bot."""

    logger.info("=" * 80)
    logger.info("TELEGRAM BOT: Starting...")
    logger.info("=" * 80)

    # Load environment
    load_dotenv()

    # Get credentials
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')

    if not telegram_token:
        logger.error("TELEGRAM_BOT_TOKEN not found in environment!")
        sys.exit(1)

    # Initialize bot and dispatcher
    bot = Bot(token=telegram_token)
    dp = Dispatcher()

    # Initialize database connection
    db = Database()
    db.connect()

    logger.info("✓ Bot initialized")
    logger.info(f"✓ Database connected")
    logger.info(f"✓ Tracking reactions: {', '.join(VALID_REACTIONS)}")

    # Register message reaction handler
    @dp.message_reaction()
    async def on_reaction(event: MessageReactionUpdated):
        """Handler for message reactions."""
        try:
            await handle_reaction(event, bot, db)
        except Exception as e:
            logger.error(f"Error handling reaction: {e}", exc_info=True)

    # /start command
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        """Handle /start command."""
        await message.reply(
            "🤖 *RealtyBot-Bali активен!*\n\n"
            "📊 Команды:\n"
            "/stats - показать статистику за сегодня\n\n"
            "📝 Отслеживаемые реакции:\n"
            "❤️ - Хороший вариант\n"
            "💩 - Плохой вариант\n"
            "🤡 - Ошибка, требует исправления",
            parse_mode='Markdown'
        )

    # /stats command - show daily feedback stats
    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        """Handle /stats command - show today's feedback stats."""
        try:
            # Try today first
            sent_today = db.get_sent_listings_count_today()

            if sent_today == 0:
                # Nothing sent today, show yesterday's stats
                from datetime import datetime, timedelta
                yesterday = datetime.now() - timedelta(days=1)
                stats = db.get_feedback_stats(since=yesterday)
                batch_count = db.get_batch_count_today()  # Will check yesterday via fallback

                # Get sent count for yesterday
                db.cursor.execute("""
                    SELECT COUNT(*)
                    FROM listings
                    WHERE status = 'stage5_sent'
                      AND updated_at::date = (CURRENT_DATE - INTERVAL '1 day')::date
                """)
                sent_yesterday = db.cursor.fetchone()[0]

                # Build report for yesterday
                yesterday_str = yesterday.strftime('%d.%m.%Y')
                message_text = f"📊 *Статистика фидбека за {yesterday_str}*\n\n"
                message_text += f"Батчей: {batch_count}\n"
                message_text += f"Отправлено объявлений: {sent_yesterday}\n\n"

                good_count = stats.get('❤️', {}).get('message_count', 0)
                bad_count = stats.get('💩', {}).get('message_count', 0)
                error_count = stats.get('🤡', {}).get('message_count', 0)
                total = good_count + bad_count + error_count

                if total == 0:
                    message_text += "Нет реакций на объявления.\n"
                else:
                    message_text += f"Получено реакций: {total} из {sent_yesterday}\n\n"
                    message_text += f"❤️ Хорошие варианты: {good_count}\n"
                    message_text += f"💩 Плохие варианты: {bad_count}\n"
                    message_text += f"🤡 Ошибки (требуют правки): {error_count}\n"

                    # Add error listings
                    if error_count > 0:
                        message_text += f"\n*🤡 Объявления с ошибками:*\n"
                        error_listings = stats.get('🤡', {}).get('listings', [])
                        for i, listing in enumerate(error_listings[:5], 1):
                            url = listing.get('listing_url', 'N/A')
                            count = listing.get('reaction_count', 1)
                            message_text += f"{i}. {url}\n"
                        if len(error_listings) > 5:
                            message_text += f"... и ещё {len(error_listings) - 5}\n"

                    # Add bad listings
                    if bad_count > 0:
                        message_text += f"\n*💩 Плохие варианты:*\n"
                        bad_listings = stats.get('💩', {}).get('listings', [])
                        for i, listing in enumerate(bad_listings[:3], 1):
                            url = listing.get('listing_url', 'N/A')
                            message_text += f"{i}. {url}\n"

                message_text += f"\n_Сегодня объявления ещё не отправлялись._"
                await message.reply(message_text, parse_mode='Markdown')
            else:
                # Show today's stats
                report = generate_stats_report(db)
                await message.reply(report, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error getting stats: {e}", exc_info=True)
            await message.reply("❌ Ошибка при получении статистики")

    # /help command
    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        """Handle /help command."""
        await message.reply(
            "📖 *Доступные команды:*\n\n"
            "/start - информация о боте\n"
            "/stats - статистика за сегодня\n"
            "/help - эта справка\n\n"
            "🔔 *Система фидбека:*\n"
            "Ставьте реакции на объявления:\n"
            "❤️ - хорошее объявление\n"
            "💩 - плохое объявление\n"
            "🤡 - ошибка фильтрации\n\n"
            "Статистика собирается автоматически и помогает улучшить качество фильтрации.",
            parse_mode='Markdown'
        )

    logger.info("🚀 Starting polling...")

    try:
        # Start polling
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot error: {e}", exc_info=True)
    finally:
        db.close()
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
