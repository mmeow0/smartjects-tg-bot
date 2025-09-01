import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

from config import (
    BOT_TOKEN,
    ALLOWED_USERS,
    DEFAULT_LOGOS_FILE
)
from handlers import file_handler, command_handler, smartject_manager
from services.csv_processor import CSVProcessor
from services.logo_matcher import LogoMatcher
from services.supabase_client import SupabaseClient
from utils.logging_config import setup_bot_logging, get_logger, suppress_external_loggers

# Configure logging using centralized configuration
setup_bot_logging()
suppress_external_loggers()
logger = get_logger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Initialize services
supabase_client = SupabaseClient()
csv_processor = CSVProcessor(supabase_client)
logo_matcher = LogoMatcher(supabase_client, DEFAULT_LOGOS_FILE)

async def main():
    """Main function to start the bot"""
    try:
        # Register handlers
        dp.include_router(command_handler.router)
        dp.include_router(file_handler.router)
        dp.include_router(smartject_manager.router)

        # Pass services to handlers via bot data
        bot_data = {
            "csv_processor": csv_processor,
            "logo_matcher": logo_matcher,
            "supabase_client": supabase_client,
            "allowed_users": ALLOWED_USERS
        }

        # Delete webhook and start polling
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Bot started successfully")

        await dp.start_polling(bot, **bot_data)

    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        raise
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
