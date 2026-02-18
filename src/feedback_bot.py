#!/usr/bin/env python3
"""
Feedback Bot: Handles Telegram reactions to track user feedback on listings.
Listens for MessageReactionUpdated events and saves them to the database.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
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
        logging.FileHandler('logs/feedback_bot.log'),
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


async def main():
    """Main function to run the feedback bot."""

    logger.info("=" * 80)
    logger.info("FEEDBACK BOT: Starting...")
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

    # Simple /start command for testing
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        """Handle /start command."""
        await message.reply(
            "Feedback Bot активен! 🤖\n\n"
            "Отслеживаемые реакции:\n"
            "❤️ - Хороший вариант\n"
            "💩 - Плохой вариант\n"
            "🤡 - Ошибка, требует исправления"
        )

    # Simple /stats command for testing
    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        """Handle /stats command - show today's feedback stats."""
        try:
            stats = db.get_feedback_stats()

            msg = "📊 *Статистика реакций за сегодня:*\n\n"

            for emoji, data in stats.items():
                count = data['message_count']
                total = data['total_reactions']
                msg += f"{emoji} {count} объявлений ({total} реакций)\n"

            await message.reply(msg, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            await message.reply("Ошибка при получении статистики")

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
