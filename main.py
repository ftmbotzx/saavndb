#!/usr/bin/env python3
"""
Main entry point for the Telegram Captain Bot system.
This script initializes and runs the captain bot that manages worker bots
for downloading songs from JioSaavn API using Spotify track IDs.
"""

import asyncio
import logging
import os
from captain_bot import CaptainBot
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Main function to start the captain bot."""
    try:
        # Initialize configuration
        config = Config()
        
        # Validate required environment variables
        if not config.TELEGRAM_BOT_TOKEN:
            logger.error("TELEGRAM_BOT_TOKEN environment variable is required")
            return
        
        # Check if we have Spotify credentials either from env or clients.json
        import os
        if not os.path.exists('clients.json') and (not config.SPOTIFY_CLIENT_ID or not config.SPOTIFY_CLIENT_SECRET):
            logger.error("Spotify credentials are required: provide SPOTIFY_CLIENT_ID/SPOTIFY_CLIENT_SECRET or clients.json file")
            return
        
        # Initialize and start the captain bot
        logger.info("Starting Captain Bot...")
        captain = CaptainBot(config)
        await captain.start()
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
    finally:
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
