"""
JioSaavn Client - Handles JioSaavn API integration for song search and download.
"""

import asyncio
import logging
import aiohttp
from typing import List, Dict, Any, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

class JioSaavnClient:
    def __init__(self, base_url: str = "https://ftm-saavnapi.vercel.app"):
        self.base_url = base_url
        self.session = None
    
    async def _get_session(self):
        """Get or create aiohttp session."""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
        return self.session
    
    async def search_songs(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search for songs using the universal endpoint."""
        try:
            session = await self._get_session()
            
            # Encode the query for URL
            encoded_query = quote(query)
            url = f"{self.base_url}/result/?query={encoded_query}"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Handle different response formats
                    songs = []
                    if isinstance(data, list):
                        songs = data[:limit]
                    elif isinstance(data, dict):
                        if 'data' in data and isinstance(data['data'], list):
                            songs = data['data'][:limit]
                        elif 'results' in data and isinstance(data['results'], list):
                            songs = data['results'][:limit]
                        elif 'songs' in data and isinstance(data['songs'], list):
                            songs = data['songs'][:limit]
                        else:
                            # Check if the dict itself contains song data
                            if 'title' in data and 'url' in data:
                                songs = [data]
                    
                    logger.info(f"Found {len(songs)} songs for query: {query}")
                    return songs
                else:
                    logger.error(f"JioSaavn search failed with status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error searching songs on JioSaavn: {e}")
            return []
    
    async def get_song_details(self, song_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed song information from song URL."""
        try:
            session = await self._get_session()
            
            encoded_url = quote(song_url)
            url = f"{self.base_url}/song/?query={encoded_url}&lyrics=true"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Retrieved song details for: {data.get('title', 'Unknown')}")
                    return data
                else:
                    logger.error(f"Failed to get song details with status {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting song details: {e}")
            return None
    
    async def get_download_urls(self, song_url: str) -> List[str]:
        """Get download URLs for a song. Returns multiple quality options."""
        try:
            # First get the song details
            song_data = await self.get_song_details(song_url)
            if not song_data:
                return []
            
            download_urls = []
            
            # Check for direct download URL in the response
            if 'url' in song_data and song_data['url']:
                download_urls.append(song_data['url'])
            
            # Check for multiple download links (different qualities)
            if 'download_url' in song_data:
                if isinstance(song_data['download_url'], list):
                    download_urls.extend(song_data['download_url'])
                elif isinstance(song_data['download_url'], str):
                    download_urls.append(song_data['download_url'])
            
            # Check for media_url (sometimes used)
            if 'media_url' in song_data and song_data['media_url']:
                download_urls.append(song_data['media_url'])
            
            # Try to get additional quality links by making a direct API call
            try:
                session = await self._get_session()
                song_id = song_data.get('songid') or song_data.get('id')
                
                if song_id:
                    quality_url = f"{self.base_url}/song/?query={song_id}"
                    async with session.get(quality_url) as response:
                        if response.status == 200:
                            quality_data = await response.json()
                            
                            # Extract all possible download URLs
                            for key in ['url', 'media_url', 'download_url', '320kbps', '160kbps', '96kbps', '48kbps']:
                                if key in quality_data and quality_data[key]:
                                    if isinstance(quality_data[key], list):
                                        download_urls.extend(quality_data[key])
                                    elif isinstance(quality_data[key], str):
                                        download_urls.append(quality_data[key])
            except Exception as e:
                logger.warning(f"Failed to get additional quality URLs: {e}")
            
            # Remove duplicates while preserving order
            unique_urls = []
            for url in download_urls:
                if url and url not in unique_urls:
                    unique_urls.append(url)
            
            # Filter out preview URLs (typically contain 'preview' in the URL)
            filtered_urls = [url for url in unique_urls if 'preview' not in url.lower()]
            
            # If no non-preview URLs found, return all URLs
            final_urls = filtered_urls if filtered_urls else unique_urls
            
            logger.info(f"Found {len(final_urls)} download URLs for song")
            return final_urls
            
        except Exception as e:
            logger.error(f"Error getting download URLs: {e}")
            return []
    
    async def get_playlist_songs(self, playlist_url: str) -> List[Dict[str, Any]]:
        """Get all songs from a playlist."""
        try:
            session = await self._get_session()
            
            encoded_url = quote(playlist_url)
            url = f"{self.base_url}/playlist/?query={encoded_url}"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    songs = []
                    if 'songs' in data and isinstance(data['songs'], list):
                        songs = data['songs']
                    elif 'data' in data and isinstance(data['data'], list):
                        songs = data['data']
                    elif isinstance(data, list):
                        songs = data
                    
                    logger.info(f"Found {len(songs)} songs in playlist")
                    return songs
                else:
                    logger.error(f"Failed to get playlist with status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error getting playlist songs: {e}")
            return []
    
    async def get_album_songs(self, album_url: str) -> List[Dict[str, Any]]:
        """Get all songs from an album."""
        try:
            session = await self._get_session()
            
            encoded_url = quote(album_url)
            url = f"{self.base_url}/album/?query={encoded_url}"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    songs = []
                    if 'songs' in data and isinstance(data['songs'], list):
                        songs = data['songs']
                    elif 'data' in data and isinstance(data['data'], list):
                        songs = data['data']
                    elif isinstance(data, list):
                        songs = data
                    
                    logger.info(f"Found {len(songs)} songs in album")
                    return songs
                else:
                    logger.error(f"Failed to get album with status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error getting album songs: {e}")
            return []
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None
