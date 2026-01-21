import asyncio
import os
import logging
from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from bot.handlers import router
from bot.utils import setup_logger

from aiogram.client.session.aiohttp import AiohttpSession

from bot.services.database import DatabaseService
from bot.worker import BackgroundWorker

async def main():
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    # Check tokens
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN is not set in .env")
        return
    
    # Init Database
    db = DatabaseService()
    await db.init_db()

    # Initialize Bot and Dispatcher with increased timeout
    session = AiohttpSession(timeout=600.0) # 10 minutes timeout
    bot = Bot(token=bot_token, session=session)
    dp = Dispatcher()
    
    # Initialize and start Worker
    worker = BackgroundWorker(bot, db)
    asyncio.create_task(worker.run())
    
    # Register routers
    dp.include_router(router)
    
    logger.info("Starting bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
