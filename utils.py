"""
Utility functions for the Telegram Captain Bot system.
Contains helper functions for file operations, validation, and data processing.
"""

import asyncio
import aiohttp
import logging
import os
import re
import tempfile
from typing import List, Optional, Set
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def extract_track_ids(file_path: str) -> List[str]:
    """Extract Spotify track IDs from a text file."""
    try:
        track_ids = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                # Extract track ID from various Spotify URL formats
                track_id = extract_spotify_track_id(line)
                
                if track_id:
                    if validate_spotify_track_id(track_id):
                        track_ids.append(track_id)
                    else:
                        logger.warning(f"Invalid Spotify track ID on line {line_num}: {track_id}")
                else:
                    logger.warning(f"Could not extract track ID from line {line_num}: {line}")
        
        # Remove duplicates while preserving order
        unique_track_ids = []
        seen = set()
        for track_id in track_ids:
            if track_id not in seen:
                unique_track_ids.append(track_id)
                seen.add(track_id)
        
        logger.info(f"Extracted {len(unique_track_ids)} unique track IDs from {file_path}")
        return unique_track_ids
        
    except Exception as e:
        logger.error(f"Error extracting track IDs from file: {e}")
        return []

def extract_spotify_track_id(text: str) -> Optional[str]:
    """Extract Spotify track ID from various formats."""
    text = text.strip()
    
    # Direct track ID (22 characters, alphanumeric)
    if re.match(r'^[a-zA-Z0-9]{22}$', text):
        return text
    
    # Spotify URI format: spotify:track:TRACK_ID
    uri_match = re.search(r'spotify:track:([a-zA-Z0-9]{22})', text)
    if uri_match:
        return uri_match.group(1)
    
    # Spotify URL format: https://open.spotify.com/track/TRACK_ID
    url_match = re.search(r'open\.spotify\.com/track/([a-zA-Z0-9]{22})', text)
    if url_match:
        return url_match.group(1)
    
    # Spotify share URL with query parameters
    share_match = re.search(r'spotify\.com/track/([a-zA-Z0-9]{22})', text)
    if share_match:
        return share_match.group(1)
    
    return None

def validate_spotify_track_id(track_id: str) -> bool:
    """Validate if a string is a valid Spotify track ID."""
    if not track_id:
        return False
    
    # Spotify track IDs are 22 characters long and contain only alphanumeric characters
    return bool(re.match(r'^[a-zA-Z0-9]{22}$', track_id))

async def download_file(url: str, file_path: str, timeout: int = 300) -> bool:
    """Download a file from URL to local path."""
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        ) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    with open(file_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(8192):
                            file.write(chunk)
                    
                    logger.info(f"Successfully downloaded file to {file_path}")
                    return True
                else:
                    logger.error(f"Failed to download file: HTTP {response.status}")
                    return False
                    
    except asyncio.TimeoutError:
        logger.error(f"Download timeout for URL: {url}")
        return False
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return False

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file system operations."""
    # Remove or replace invalid characters
    invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    sanitized = filename
    
    for char in invalid_chars:
        sanitized = sanitized.replace(char, '_')
    
    # Remove extra spaces and dots
    sanitized = re.sub(r'\s+', ' ', sanitized)
    sanitized = sanitized.strip('. ')
    
    # Limit length
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    
    return sanitized or "unknown"

def format_duration(seconds: int) -> str:
    """Format duration in seconds to human-readable format."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds}s"
    else:
        hours = seconds // 3600
        remaining_minutes = (seconds % 3600) // 60
        return f"{hours}h {remaining_minutes}m"

def format_file_size(size_bytes: int) -> str:
    """Format file size in bytes to human-readable format."""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size_float = float(size_bytes)
    while size_float >= 1024 and i < len(size_names) - 1:
        size_float /= 1024.0
        i += 1
    
    return f"{size_float:.1f} {size_names[i]}"

def is_valid_url(url: str) -> bool:
    """Check if a string is a valid URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and special characters."""
    if not text:
        return ""
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', text)
    
    # Remove control characters
    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
    
    return cleaned.strip()

def split_list(lst: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def get_file_extension(filename: str) -> str:
    """Get file extension from filename."""
    return os.path.splitext(filename)[1].lower()

def is_audio_file(filename: str) -> bool:
    """Check if file is an audio file based on extension."""
    audio_extensions = {'.mp3', '.flac', '.wav', '.aac', '.ogg', '.m4a', '.wma'}
    return get_file_extension(filename) in audio_extensions

async def validate_telegram_channel(channel_id: int) -> bool:
    """Validate if a Telegram channel ID is valid format."""
    try:
        # Telegram channel IDs are typically negative integers starting with -100
        if isinstance(channel_id, int) and channel_id < -1000000000:
            return True
        return False
    except Exception:
        return False

def extract_numbers_from_text(text: str) -> List[int]:
    """Extract all numbers from text."""
    return [int(match) for match in re.findall(r'-?\d+', text)]

def format_track_info(track_info: dict) -> str:
    """Format track information for display."""
    parts = []
    
    if track_info.get('name'):
        parts.append(f"🎵 {track_info['name']}")
    
    if track_info.get('artist'):
        parts.append(f"👤 {track_info['artist']}")
    
    if track_info.get('album'):
        parts.append(f"💿 {track_info['album']}")
    
    if track_info.get('duration_ms'):
        duration_sec = track_info['duration_ms'] // 1000
        parts.append(f"⏱️ {format_duration(duration_sec)}")
    
    return "\n".join(parts)

class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, max_calls: int, time_window: int = 60):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
    
    async def acquire(self):
        """Acquire permission to make an API call."""
        import time
        current_time = time.time()
        
        # Remove old calls outside the time window
        self.calls = [call_time for call_time in self.calls 
                     if current_time - call_time < self.time_window]
        
        # Check if we can make a new call
        if len(self.calls) >= self.max_calls:
            # Calculate wait time
            oldest_call = min(self.calls)
            wait_time = self.time_window - (current_time - oldest_call)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        # Record the new call
        self.calls.append(current_time)

def create_temp_file(suffix: str = ".tmp", prefix: str = "bot_") -> str:
    """Create a temporary file and return its path."""
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, 
        suffix=suffix, 
        prefix=prefix,
        dir=tempfile.gettempdir()
    )
    temp_file.close()
    return temp_file.name

def cleanup_temp_files(file_paths: List[str]):
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up temp file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up temp file {file_path}: {e}")
