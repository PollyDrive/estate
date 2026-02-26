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
from pipeline_stats import build_pipeline_stats_message

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
# Note: Telegram sends ❤ without variation selector (U+2764), not ❤️ (U+2764 U+FE0F)
VALID_REACTIONS = {'❤', '💩', '🤡'}


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
            logger.info(f"📨 Received emoji reaction: '{emoji}' (repr: {repr(emoji)}) for message {message_id}")

            # Only track our valid feedback emojis
            if emoji in VALID_REACTIONS:
                # Get fb_id from database using message_id
                fb_id = db.get_fb_id_by_message_id(message_id)

                if fb_id:
                    # Save reaction to database, tagged with the chat it came from
                    db.save_reaction(message_id, fb_id, emoji, chat_id=chat_id)
                    logger.info(f"✓ Saved reaction {emoji} for message {message_id} (fb_id: {fb_id}, chat_id: {chat_id})")
                else:
                    logger.warning(f"⚠️  Message {message_id} not found in database. Skipping reaction {emoji}.")
            else:
                logger.debug(f"Ignoring non-feedback emoji: {emoji}")


def generate_stats_report(db: Database, chat_id: int, sent_count: int = None) -> str:
    """
    Generate a formatted statistics report for today.

    Args:
        db: Database connection
        chat_id: Telegram chat ID to filter feedback by
        sent_count: Pre-fetched sent count (avoids redundant re-query on same cursor)

    Returns:
        Formatted report message
    """
    stats = db.get_feedback_stats(chat_id=chat_id)
    batch_count = db.get_batch_count_today(chat_id=chat_id)
    if sent_count is None:
        sent_count = db.get_sent_listings_count_today(chat_id=chat_id)

    # Build header
    current_time = datetime.now().strftime('%H:%M')
    message = f"📊 *Статистика фидбека* (на {current_time})\n\n"

    if sent_count == 0:
        message += "Сегодня объявления ещё не отправлялись.\n"
        return message

    message += f"Батчей сегодня: {batch_count}\n"
    message += f"Отправлено объявлений: {sent_count}\n\n"

    # Count totals (Telegram sends ❤ without variation selector)
    good_count = stats.get('❤', {}).get('message_count', 0)
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

    def _chat_display_name(chat: types.Chat) -> str:
        """Extract a human-readable name from a Chat object."""
        return chat.title or chat.full_name or chat.username or f"chat_{chat.id}"

    # /start command
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        """Handle /start command. Always shows chat_id so it can be added to config."""
        chat = message.chat
        chat_id = chat.id
        name = _chat_display_name(chat)

        # Register if not yet known (idempotent — safe to call every time)
        db.register_chat(chat_id, name)
        logger.info(f"/start from chat_id={chat_id}, name='{name}', type={chat.type}")

        is_known = db.get_chat_profile(chat_id)
        is_enabled = is_known.get('enabled', False) if is_known else False

        if not is_enabled:
            # Not yet configured — show chat_id for copy-paste into profiles.json
            await message.reply(
                f"👋 *RealtyBot-Bali*\n\n"
                f"📋 Chat ID этого чата:\n`{chat_id}`\n\n"
                f"Добавь этот `chat_id` в `config/profiles.json`, "
                f"чтобы активировать отправку объявлений.\n\n"
                f"_Статус: ожидает настройки_",
                parse_mode='Markdown'
            )
        else:
            # Known and enabled — normal welcome
            await message.reply(
                "🤖 *RealtyBot-Bali активен!*\n\n"
                "📊 Команды:\n"
                "/stats - показать статистику за сегодня\n"
                "/favorites - показать все избранные объявления (❤)\n\n"
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
            this_chat_id = message.chat.id

            # Try today first
            sent_today = db.get_sent_listings_count_today(chat_id=this_chat_id)

            if sent_today == 0:

                # Nothing sent today, show yesterday's stats
                from datetime import datetime, timedelta
                yesterday = datetime.now() - timedelta(days=1)
                stats = db.get_feedback_stats(since=yesterday, chat_id=this_chat_id)
                batch_count = db.get_batch_count_today(chat_id=this_chat_id)

                # Get sent count for yesterday for this chat
                db.cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM listing_profiles
                    WHERE chat_id = %s
                      AND sent_at::date = (CURRENT_DATE - INTERVAL '1 day')::date
                    """,
                    (this_chat_id,)
                )
                sent_yesterday = db.cursor.fetchone()[0]

                # Build report for yesterday
                yesterday_str = yesterday.strftime('%d.%m.%Y')
                message_text = f"📊 *Статистика фидбека за {yesterday_str}*\n\n"
                message_text += f"Батчей: {batch_count}\n"
                message_text += f"Отправлено объявлений: {sent_yesterday}\n\n"

                good_count = stats.get('❤', {}).get('message_count', 0)
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

                message_text += f"\n_Сегодня объявления ещё не отправлялись._\n\n"
                message_text += "📈 *Pipeline:*\n" + build_pipeline_stats_message(db, this_chat_id)
                await message.reply(message_text, parse_mode='Markdown')
            else:
                # Show today's stats — pass sent_today to avoid redundant re-query
                report = generate_stats_report(db, this_chat_id, sent_count=sent_today)
                report += "\n\n📈 *Pipeline:*\n" + build_pipeline_stats_message(db, this_chat_id)
                await message.reply(report, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error getting stats: {e}", exc_info=True)
            await message.reply("❌ Ошибка при получении статистики")

    # /favorites command - show all listings with ❤ reactions
    @dp.message(Command("favorites"))
    async def cmd_favorites(message: types.Message):
        """Handle /favorites command - show all favorite listings."""
        try:
            favorites = db.get_favorite_listings(limit=50, chat_id=message.chat.id)

            if not favorites:
                await message.reply(
                    "❤️ *Избранные объявления*\n\n"
                    "Пока нет объявлений с ❤ реакцией.\n"
                    "Поставьте ❤ на понравившиеся объявления!",
                    parse_mode='Markdown'
                )
                return

            # Build message with all favorites
            msg = f"❤️ *Избранные объявления* ({len(favorites)})\n\n"

            for i, fav in enumerate(favorites, 1):
                title = fav['title'][:50] + "..." if len(fav['title']) > 50 else fav['title']
                location = fav['location'] or 'N/A'
                price = fav['price'] or 'N/A'
                url = fav['url']

                msg += f"{i}. *{title}*\n"
                msg += f"   📍 {location} | 💰 {price}\n"
                msg += f"   🔗 {url}\n\n"

                # Telegram message limit is 4096 chars
                if len(msg) > 3500:
                    await message.reply(msg, parse_mode='Markdown', disable_web_page_preview=True)
                    msg = f"❤️ *Избранные объявления (продолжение)*\n\n"

            # Send remaining message
            if msg.strip() != f"❤️ *Избранные объявления (продолжение)*\n\n":
                await message.reply(msg, parse_mode='Markdown', disable_web_page_preview=True)

        except Exception as e:
            logger.error(f"Error showing favorites: {e}", exc_info=True)
            await message.reply("❌ Ошибка при получении избранных объявлений")

    # /help command
    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        """Handle /help command."""
        await message.reply(
            "📖 *Доступные команды:*\n\n"
            "/start - информация о боте\n"
            "/stats - статистика за сегодня\n"
            "/favorites - показать все избранные объявления (❤)\n"
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
        # Start polling with explicit allowed_updates to receive reactions and chat member events
        await dp.start_polling(bot, allowed_updates=["message", "message_reaction"])
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot error: {e}", exc_info=True)
    finally:
        db.close()
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
