"""
Configuration management for the Telegram Captain Bot system.
Handles environment variables and default settings.
"""

import os
from typing import Optional

class Config:
    """Configuration class for the bot system."""
    
    # Telegram API credentials
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    
    # Spotify API credentials (optional - will use clients.json if available)
    SPOTIFY_CLIENT_ID: str = os.getenv("SPOTIFY_CLIENT_ID", "")
    SPOTIFY_CLIENT_SECRET: str = os.getenv("SPOTIFY_CLIENT_SECRET", "")
    
    # JioSaavn API settings
    JIOSAAVN_API_BASE_URL: str = os.getenv("JIOSAAVN_API_BASE_URL", "https://ftm-saavnapi.vercel.app")
    
    # Bot settings - UNLIMITED WORKERS
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "999999"))  # Unlimited workers
    MAX_CONCURRENT_DOWNLOADS: int = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "100"))  # Increased concurrency
    DOWNLOAD_TIMEOUT: int = int(os.getenv("DOWNLOAD_TIMEOUT", "300"))  # 5 minutes
    
    # File handling
    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
    TEMP_DIR: str = os.getenv("TEMP_DIR", "/tmp")
    
    # Rate limiting
    REQUESTS_PER_MINUTE: int = int(os.getenv("REQUESTS_PER_MINUTE", "30"))
    SPOTIFY_REQUESTS_PER_MINUTE: int = int(os.getenv("SPOTIFY_REQUESTS_PER_MINUTE", "100"))
    
    # Database settings (if using database)
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "bot.log")
    
    # Security
    ALLOWED_USERS: list = [
        int(user_id) for user_id in os.getenv("ALLOWED_USERS", "").split(",") 
        if user_id.strip().isdigit()
    ]
    
    # Worker bot settings
    WORKER_HEALTH_CHECK_INTERVAL: int = int(os.getenv("WORKER_HEALTH_CHECK_INTERVAL", "300"))  # 5 minutes
    WORKER_TIMEOUT: int = int(os.getenv("WORKER_TIMEOUT", "60"))  # 1 minute
    
    # Task management - UNLIMITED PROCESSING
    MAX_TRACKS_PER_TASK: int = int(os.getenv("MAX_TRACKS_PER_TASK", "999999999"))  # Unlimited tracks (2M+)
    TASK_CLEANUP_INTERVAL: int = int(os.getenv("TASK_CLEANUP_INTERVAL", "3600"))  # 1 hour
    
    @classmethod
    def validate(cls) -> list:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not cls.API_ID or cls.API_ID == 0:
            errors.append("API_ID is required")
        
        if not cls.API_HASH:
            errors.append("API_HASH is required")
        
        if not cls.TELEGRAM_BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN is required")
        
        # Check if clients.json exists, if not require environment variables
        import os
        if not os.path.exists('clients.json'):
            if not cls.SPOTIFY_CLIENT_ID:
                errors.append("SPOTIFY_CLIENT_ID is required (or provide clients.json)")
            
            if not cls.SPOTIFY_CLIENT_SECRET:
                errors.append("SPOTIFY_CLIENT_SECRET is required (or provide clients.json)")
        
        return errors
    
    @classmethod
    def get_required_env_vars(cls) -> dict:
        """Get dictionary of required environment variables with descriptions."""
        return {
            "API_ID": "Telegram API ID from my.telegram.org",
            "API_HASH": "Telegram API Hash from my.telegram.org",
            "TELEGRAM_BOT_TOKEN": "Bot token from @BotFather",
            "SPOTIFY_CLIENT_ID": "Spotify API Client ID (optional if clients.json provided)",
            "SPOTIFY_CLIENT_SECRET": "Spotify API Client Secret (optional if clients.json provided)"
        }
    
    @classmethod
    def get_optional_env_vars(cls) -> dict:
        """Get dictionary of optional environment variables with descriptions."""
        return {
            "JIOSAAVN_API_BASE_URL": f"JioSaavn API base URL (default: {cls.JIOSAAVN_API_BASE_URL})",
            "MAX_WORKERS": f"Maximum number of worker bots (default: {cls.MAX_WORKERS})",
            "MAX_CONCURRENT_DOWNLOADS": f"Max concurrent downloads per worker (default: {cls.MAX_CONCURRENT_DOWNLOADS})",
            "DOWNLOAD_TIMEOUT": f"Download timeout in seconds (default: {cls.DOWNLOAD_TIMEOUT})",
            "MAX_FILE_SIZE_MB": f"Maximum file size in MB (default: {cls.MAX_FILE_SIZE_MB})",
            "TEMP_DIR": f"Temporary directory for downloads (default: {cls.TEMP_DIR})",
            "REQUESTS_PER_MINUTE": f"API requests per minute limit (default: {cls.REQUESTS_PER_MINUTE})",
            "SPOTIFY_REQUESTS_PER_MINUTE": f"Spotify API requests per minute (default: {cls.SPOTIFY_REQUESTS_PER_MINUTE})",
            "DATABASE_URL": "Database URL for persistent storage (optional)",
            "LOG_LEVEL": f"Logging level (default: {cls.LOG_LEVEL})",
            "LOG_FILE": f"Log file path (default: {cls.LOG_FILE})",
            "ALLOWED_USERS": "Comma-separated list of allowed user IDs (optional)",
            "WORKER_HEALTH_CHECK_INTERVAL": f"Worker health check interval in seconds (default: {cls.WORKER_HEALTH_CHECK_INTERVAL})",
            "WORKER_TIMEOUT": f"Worker response timeout in seconds (default: {cls.WORKER_TIMEOUT})",
            "MAX_TRACKS_PER_TASK": f"Maximum tracks per task (default: {cls.MAX_TRACKS_PER_TASK})",
            "TASK_CLEANUP_INTERVAL": f"Task cleanup interval in seconds (default: {cls.TASK_CLEANUP_INTERVAL})"
        }
