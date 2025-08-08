"""
Spotify Client - Handles Spotify API integration for track metadata retrieval.
"""

import json
import logging
import random
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from typing import Optional, Dict, Any, List
from config import Config
import asyncio

logger = logging.getLogger(__name__)

class SpotifyClient:
    def __init__(self, config: Config):
        self.config = config
        self.spotify = None
        self.clients = []
        self.current_client_index = 0
        self._load_clients()

    def _load_clients(self):
        """Load Spotify client credentials from clients.json file."""
        try:
            with open('clients.json', 'r') as f:
                data = json.load(f)
                self.clients = data['clients']
            logger.info(f"Loaded {len(self.clients)} Spotify client credentials")
        except FileNotFoundError:
            logger.warning("clients.json not found. Falling back to config.")
            # Fallback to config if clients.json not found
            if self.config.SPOTIFY_CLIENT_ID and self.config.SPOTIFY_CLIENT_SECRET:
                self.clients = [{
                    "client_id": self.config.SPOTIFY_CLIENT_ID,
                    "client_secret": self.config.SPOTIFY_CLIENT_SECRET
                }]
            else:
                logger.error("Spotify Client ID and Secret not found in config.")
        except json.JSONDecodeError:
            logger.error("Error decoding clients.json. Ensure it's valid JSON.")
        except Exception as e:
            logger.error(f"Unexpected error loading clients.json: {e}")


    async def initialize(self):
        """Initialize Spotify client with credentials from clients pool."""
        if not self.clients:
            logger.error("No Spotify client credentials available.")
            return False

        # Try to initialize with all clients until one works
        for i, client in enumerate(self.clients):
            try:
                logger.info(f"Trying Spotify client {i+1}/{len(self.clients)}: {client['client_id'][:8]}...")

                # Use custom cache handler to avoid permission issues
                from spotipy.cache_handler import MemoryCacheHandler
                cache_handler = MemoryCacheHandler()

                client_credentials_manager = SpotifyClientCredentials(
                    client_id=client['client_id'],
                    client_secret=client['client_secret'],
                    cache_handler=cache_handler  # Use memory cache instead of file cache
                )

                self.spotify = spotipy.Spotify(
                    client_credentials_manager=client_credentials_manager,
                    requests_timeout=10,
                    retries=0  # Disable retries for faster failover
                )

                # Test the connection by fetching some data
                test_result = self.spotify.search(q="test", type="track", limit=1)
                if test_result:
                    logger.info(f"Spotify client {i+1} initialized successfully")
                    self.current_client_index = i
                    return True

            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    logger.warning(f"Client {i+1} rate limited. Trying next client...")
                elif e.http_status == 401:
                    logger.warning(f"Client {i+1} invalid credentials. Trying next client...")
                else:
                    logger.warning(f"Client {i+1} error: {e}. Trying next client...")
                continue
            except Exception as e:
                logger.warning(f"Client {i+1} unexpected error: {e}. Trying next client...")
                continue

        logger.error("Failed to initialize any Spotify client from the pool.")
        self.spotify = None
        return False

    async def _try_next_client(self):
        """Try to initialize with the next available client."""
        if len(self.clients) <= 1:
            logger.error("Only one or no Spotify clients available. Cannot switch.")
            return False

        # Iterate through remaining clients to find one that works
        start_index = self.current_client_index
        for i in range(len(self.clients)):
            self.current_client_index = (self.current_client_index + 1) % len(self.clients)

            # Don't try the same client again
            if self.current_client_index == start_index:
                continue

            client = self.clients[self.current_client_index]

            try:
                logger.info(f"Switching to client {self.current_client_index + 1}: {client['client_id'][:8]}...")

                # Use custom cache handler to avoid permission issues
                from spotipy.cache_handler import MemoryCacheHandler
                cache_handler = MemoryCacheHandler()

                client_credentials_manager = SpotifyClientCredentials(
                    client_id=client['client_id'],
                    client_secret=client['client_secret'],
                    cache_handler=cache_handler  # Use memory cache instead of file cache
                )

                self.spotify = spotipy.Spotify(
                    client_credentials_manager=client_credentials_manager,
                    requests_timeout=10,
                    retries=0  # Disable retries for faster failover
                )

                # Test the connection
                test_result = self.spotify.search(q="test", type="track", limit=1)
                if test_result:
                    logger.info(f"Successfully switched to Spotify client {self.current_client_index + 1}")
                    return True

            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    logger.warning(f"Client {self.current_client_index + 1} rate limited. Trying next...")
                elif e.http_status == 401:
                    logger.warning(f"Client {self.current_client_index + 1} invalid credentials. Trying next...")
                else:
                    logger.warning(f"Client {self.current_client_index + 1} error: {e}. Trying next...")
                continue
            except Exception as e:
                logger.warning(f"Client {self.current_client_index + 1} error: {e}. Trying next...")
                continue

        logger.error("Failed to switch to any working Spotify client.")
        self.spotify = None
        return False

    async def get_track_info(self, track_id: str) -> Optional[Dict[str, Any]]:
        """Get track information from Spotify."""
        if not self.spotify:
            logger.warning("Spotify client not initialized. Attempting to initialize...")
            if not await self.initialize():
                logger.error("Failed to initialize Spotify client.")
                return None

        try:
            # Clean track ID (remove spotify: prefix or URL parts)
            if track_id.startswith('spotify:track:'):
                track_id = track_id.replace('spotify:track:', '')
            elif track_id.startswith('https://open.spotify.com/track/'):
                track_id = track_id.replace('https://open.spotify.com/track/', '').split('?')[0]

            # Ensure the cleaned ID is valid
            if len(track_id) != 22:
                logger.warning(f"Invalid Spotify track ID format after cleaning: {track_id}")
                return None

            # Get track information
            track = self.spotify.track(track_id)

            if not track:
                logger.warning(f"Track not found on Spotify: {track_id}")
                return None

            # Extract relevant information
            track_info = {
                'spotify_id': track_id,
                'name': track['name'],
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'explicit': track['explicit'],
                'preview_url': track.get('preview_url'),
                'external_urls': track.get('external_urls', {}),
                'release_date': track['album'].get('release_date'),
                'total_tracks': track['album'].get('total_tracks'),
                'genres': track['album'].get('genres', []),
                'image_url': track['album']['images'][0]['url'] if track['album']['images'] else None
            }

            logger.info(f"Retrieved track info: {track_info['name']} by {track_info['artist']}")
            return track_info

        except spotipy.exceptions.SpotifyException as e:
            logger.error(f"Spotify API error for track {track_id}: {e}")
            # Handle specific rate limit errors
            if e.http_status == 429:
                logger.warning("Rate limit exceeded. Switching to next client...")
                if await self._try_next_client():
                    logger.info("Successfully switched client. Retrying track...")
                    return await self.get_track_info(track_id)
                else:
                    logger.error("No working clients available. Skipping track.")
                    return None
            elif e.http_status == 401:
                logger.warning("Invalid credentials. Switching to next client...")
                if await self._try_next_client():
                    logger.info("Successfully switched client. Retrying track...")
                    return await self.get_track_info(track_id)
                else:
                    logger.error("No working clients available. Skipping track.")
                    return None
            return None
        except Exception as e:
            logger.error(f"Error getting track info for {track_id}: {e}")
            return None

    async def get_multiple_tracks(self, track_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get multiple tracks from Spotify with rate limit handling."""
        if not self.spotify:
            await self.initialize()

        if not self.spotify:
            logger.error("Spotify client not initialized")
            return {}

        tracks_info = {}
        batch_size = 50  # Spotify API limit

        try:
            for i in range(0, len(track_ids), batch_size):
                batch = track_ids[i:i + batch_size]

                try:
                    # Clean track IDs (remove any URL parts)
                    clean_ids = []
                    for track_id in batch:
                        clean_id = track_id.split('/')[-1].split('?')[0]
                        if len(clean_id) == 22:  # Valid Spotify ID length
                            clean_ids.append(clean_id)

                    if not clean_ids:
                        continue

                    # Get tracks from Spotify
                    tracks = self.spotify.tracks(clean_ids)

                    for j, track in enumerate(tracks['tracks']):
                        if track:  # Track found
                            original_id = batch[j] if j < len(batch) else clean_ids[j]
                            tracks_info[original_id] = {
                                'id': track['id'],
                                'name': track['name'],
                                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                                'album': track['album']['name'],
                                'duration_ms': track['duration_ms']
                            }

                    # Add delay between batches to respect rate limits
                    await asyncio.sleep(0.1) # Small delay between batches

                except spotipy.exceptions.SpotifyException as batch_error:
                    logger.error(f"Spotify API error for batch {i//batch_size + 1}: {batch_error}")
                    if batch_error.http_status == 429:
                        logger.warning("Rate limit exceeded for batch. Waiting before next batch.")
                        retry_after = int(batch_error.headers.get('Retry-After', 60))
                        await asyncio.sleep(retry_after)
                    # Continue with next batch instead of failing completely
                    continue
                except Exception as batch_error:
                    logger.error(f"Error processing batch {i//batch_size + 1}: {batch_error}")
                    # Continue with next batch instead of failing completely
                    continue


            logger.info(f"Retrieved Spotify info for {len(tracks_info)} out of {len(track_ids)} tracks")
            return tracks_info

        except Exception as e:
            logger.error(f"Error in get_multiple_tracks: {e}")
            # Return partial results instead of empty dict
            return tracks_info

    async def search_track(self, query: str, limit: int = 10) -> list:
        """Search for tracks on Spotify."""
        if not self.spotify:
            logger.warning("Spotify client not initialized. Attempting to initialize...")
            if not await self.initialize():
                logger.error("Failed to initialize Spotify client.")
                return []

        try:
            results = self.spotify.search(q=query, type='track', limit=limit)

            tracks = []
            for track in results['tracks']['items'] or []:
                track_info = {
                    'spotify_id': track['id'],
                    'name': track['name'],
                    'artist': ', '.join([artist['name'] for artist in track['artists']]),
                    'album': track['album']['name'],
                    'duration_ms': track['duration_ms'],
                    'popularity': track['popularity'],
                    'explicit': track['explicit'],
                    'preview_url': track.get('preview_url'),
                    'external_urls': track.get('external_urls', {}),
                    'release_date': track['album'].get('release_date'),
                    'image_url': track['album']['images'][0]['url'] if track['album']['images'] else None
                }
                tracks.append(track_info)

            logger.info(f"Found {len(tracks)} tracks for query: {query}")
            return tracks

        except spotipy.exceptions.SpotifyException as e:
            logger.error(f"Spotify API error during search for '{query}': {e}")
            if e.http_status == 429:
                logger.warning("Rate limit exceeded during search. Will retry after delay.")
                retry_after = int(e.headers.get('Retry-After', 60))
                await asyncio.sleep(retry_after)
                # Retry the search after delay
                return await self.search_track(query, limit)
            return []
        except Exception as e:
            logger.error(f"Error searching tracks for {query}: {e}")
            return []