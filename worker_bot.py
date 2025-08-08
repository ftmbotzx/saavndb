"""
Worker Bot Manager - Manages worker bots that handle song downloading and sending.
"""

import asyncio
import logging
import os
import tempfile
from typing import Dict, List, Optional, Any
from pyrogram.client import Client
from jiosaavn_client import JioSaavnClient
from utils import download_file
from config import Config
import pyroutils
from progress_tracker import progress_tracker

logger = logging.getLogger(__name__)

class WorkerBotManager:
    def __init__(self):
        self.workers: Dict[str, Client] = {}
        self.worker_status: Dict[str, bool] = {}
        self.worker_tasks: Dict[str, asyncio.Queue] = {}  # Individual task queues for unlimited workers
        self.worker_processors: Dict[str, asyncio.Task] = {}  # Background processors
        self.jiosaavn_client = JioSaavnClient()
        self.max_concurrent_per_worker = 10  # Unlimited processing capability
        self.semaphore = asyncio.Semaphore(200)  # Global rate limiting for unlimited workers
    
    async def add_worker(self, bot_token: str) -> Optional[str]:
        """Add a new worker bot."""
        try:
            # Extract bot ID from token
            worker_id = bot_token.split(':')[0]
            
            # Create Pyrogram client
            worker_app = Client(
                f"worker_{worker_id}",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=bot_token
            )
            
            # Test the bot token and get bot info
            await worker_app.start()
            me = await worker_app.get_me()
            logger.info(f"Worker bot connected: {me.username} ({worker_id})")
            
            # Keep the worker connected for processing
            # Don't stop it immediately
            
            # Store worker and create unlimited processing queue
            self.workers[worker_id] = worker_app
            self.worker_status[worker_id] = True
            self.worker_tasks[worker_id] = asyncio.Queue(maxsize=1000)  # Large queue for unlimited tracks
            
            # Start background processor for unlimited asynchronous processing
            self.worker_processors[worker_id] = asyncio.create_task(
                self._process_worker_queue(worker_id)
            )
            
            logger.info(f"Worker bot added with unlimited processing: {me.username} ({worker_id})")
            return worker_id
            
        except Exception as e:
            logger.error(f"Error adding worker bot: {e}")
            return None
    
    async def _process_worker_queue(self, worker_id: str):
        """Process worker queue asynchronously for unlimited tracks."""
        while True:
            try:
                if worker_id not in self.worker_tasks:
                    break
                
                # Get task from queue
                task_data = await self.worker_tasks[worker_id].get()
                
                if task_data is None:  # Shutdown signal
                    break
                
                # Process multiple tasks concurrently for unlimited processing
                async with self.semaphore:
                    await self._process_single_track_async(worker_id, task_data)
                
                # Mark task as done
                self.worker_tasks[worker_id].task_done()
                
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} processor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in worker {worker_id} processor: {e}")
                continue
    
    async def _process_single_track_async(self, worker_id: str, task_data: Dict[str, Any]):
        """Process a single track asynchronously with progress tracking."""
        track_id = task_data['track_id']
        dump_channel = task_data['dump_channel']
        spotify_data = task_data.get('spotify_data')
        worker = self.workers.get(worker_id)
        
        try:
            # Validate channel using pyroutils
            if not pyroutils.is_channel_id(dump_channel):
                logger.error(f"Invalid dump channel {dump_channel} for worker {worker_id}")
                await progress_tracker.update_worker_progress(
                    worker_id, "failed", track_id, "Invalid dump channel", worker
                )
                return
            
            # Search JioSaavn for the song
            query = f"{spotify_data.get('name', '')} {spotify_data.get('artist', '')}"
            search_results = await self.jiosaavn_client.search_songs(query)
            
            if not search_results:
                logger.warning(f"No song found on JioSaavn for track {track_id}")
                await progress_tracker.update_worker_progress(
                    worker_id, "skipped", track_id, "No song found on JioSaavn", worker
                )
                return
            
            # Download and send the first result
            song_data = search_results[0]
            success = await self._download_and_send_song_async(worker_id, song_data, spotify_data, dump_channel)
            
            if success:
                await progress_tracker.update_worker_progress(
                    worker_id, "sent", track_id, None, worker
                )
            else:
                await progress_tracker.update_worker_progress(
                    worker_id, "failed", track_id, "Download/send failed", worker
                )
            
        except Exception as e:
            logger.error(f"Error processing track {track_id} in worker {worker_id}: {e}")
            await progress_tracker.update_worker_progress(
                worker_id, "failed", track_id, str(e), worker
            )
    
    async def _download_and_send_song_async(self, worker_id: str, song_data: Dict[str, Any], 
                                          spotify_data: Dict[str, Any], dump_channel: int) -> bool:
        """Download and send song asynchronously. Returns True if successful."""
        temp_file = None
        try:
            worker = self.workers[worker_id]
            
            # Ensure worker is connected
            if not worker.is_connected:
                await worker.start()
            
            # Get download URLs
            download_urls = await self.jiosaavn_client.get_download_urls(song_data['url'])
            if not download_urls:
                logger.error(f"No download URLs found for song: {song_data.get('title', 'Unknown')}")
                return
            
            # Filter out preview URLs and get the best quality
            filtered_urls = [url for url in download_urls if 'preview' not in url.lower()]
            if not filtered_urls:
                filtered_urls = download_urls  # Fallback to all URLs if no non-preview found
            
            # Download the song file
            song_url = filtered_urls[0]  # Use the first (usually best quality) URL
            temp_file = await download_file(song_url)
            
            if not temp_file or not os.path.exists(temp_file):
                logger.error(f"Failed to download song: {song_data.get('title', 'Unknown')}")
                return False
            
            # Prepare caption with requested format
            caption = self._prepare_song_caption(song_data, spotify_data)
            
            # Send audio file to dump channel
            await worker.send_audio(
                chat_id=dump_channel,
                audio=temp_file,
                caption=caption,
                title=song_data.get('title', 'Unknown'),
                performer=song_data.get('singers', 'Unknown'),
                duration=int(song_data.get('duration', 0)) if song_data.get('duration') else None
            )
            
            logger.info(f"Song sent to dump channel {dump_channel}: {song_data.get('title', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading and sending song: {e}")
            return False
        finally:
            # Cleanup temporary file
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temp file {temp_file}: {cleanup_error}")
    
    def _prepare_song_caption(self, song_data: Dict[str, Any], spotify_data: Dict[str, Any]) -> str:
        """Prepare caption for the song in the requested format."""
        caption_parts = []
        
        # Title
        title = song_data.get('title') or spotify_data.get('name', 'Unknown')
        caption_parts.append(f"🎵 {title}")
        
        # Artist
        artists = song_data.get('singers') or spotify_data.get('artist', 'Unknown')
        caption_parts.append(f"👤 {artists}")
        
        # Spotify Track ID
        if spotify_data.get('id'):
            caption_parts.append(f"🆔 {spotify_data['id']}")
        
        return "\n".join(caption_parts)
    
    async def add_track_to_worker_queue(self, worker_id: str, task_data: Dict[str, Any]):
        """Add track to worker queue for unlimited asynchronous processing."""
        if worker_id not in self.worker_tasks:
            logger.error(f"Worker {worker_id} not found")
            return False
        
        try:
            await self.worker_tasks[worker_id].put(task_data)
            return True
        except Exception as e:
            logger.error(f"Error adding track to worker {worker_id} queue: {e}")
            return False
    
    async def remove_worker(self, worker_id: str) -> bool:
        """Remove a worker bot and clean up unlimited processing resources."""
        try:
            if worker_id in self.workers:
                # Stop background processor
                if worker_id in self.worker_processors:
                    self.worker_processors[worker_id].cancel()
                    del self.worker_processors[worker_id]
                
                # Clean up queues and resources
                if worker_id in self.worker_tasks:
                    # Send shutdown signal
                    await self.worker_tasks[worker_id].put(None)
                    del self.worker_tasks[worker_id]
                
                # Stop the worker if it's running
                try:
                    await self.workers[worker_id].stop()
                except:
                    pass
                
                # Remove from dictionaries
                del self.workers[worker_id]
                del self.worker_status[worker_id]
                
                logger.info(f"Worker bot removed with unlimited processing cleanup: {worker_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error removing worker bot: {e}")
            return False
    
    async def check_worker_status(self, worker_id: str) -> bool:
        """Check if a worker bot is online and responsive."""
        try:
            if worker_id not in self.workers:
                return False
            
            worker = self.workers[worker_id]
            
            # Try to get bot info and ensure connection
            if not worker.is_connected:
                try:
                    await worker.start()
                    logger.info(f"Reconnected worker {worker_id}")
                except Exception as start_error:
                    logger.error(f"Failed to start worker {worker_id}: {start_error}")
                    self.worker_status[worker_id] = False
                    return False
            
            # Test with a simple API call
            try:
                me = await worker.get_me()
                self.worker_status[worker_id] = True
                logger.debug(f"Worker {worker_id} status: OK ({me.username})")
                return True
            except Exception as api_error:
                logger.error(f"Worker {worker_id} API test failed: {api_error}")
                self.worker_status[worker_id] = False
                return False
            
        except Exception as e:
            logger.error(f"Worker {worker_id} status check failed: {e}")
            self.worker_status[worker_id] = False
            return False
    
    async def get_available_workers(self) -> List[str]:
        """Get list of available worker bot IDs."""
        available_workers = []
        
        for worker_id in self.workers.keys():
            if await self.check_worker_status(worker_id):
                available_workers.append(worker_id)
        
        return available_workers
    
    async def download_and_send_song(self, worker_id: str, song_data: Dict[str, Any], 
                                   dump_channel: int, track_info: Dict[str, Any]) -> bool:
        """Download a song and send it to the dump channel using a specific worker."""
        try:
            if worker_id not in self.workers:
                logger.error(f"Worker {worker_id} not found")
                return False
            
            worker = self.workers[worker_id]
            
            # Ensure worker is connected
            if not worker.is_connected:
                await worker.start()
            
            # Get download URLs
            download_urls = await self.jiosaavn_client.get_download_urls(song_data['url'])
            if not download_urls:
                logger.error(f"No download URLs found for song: {song_data.get('title', 'Unknown')}")
                return False
            
            # Filter out preview URLs and get the best quality
            filtered_urls = [url for url in download_urls if 'preview' not in url.lower()]
            if not filtered_urls:
                filtered_urls = download_urls  # Fallback to all URLs if no non-preview found
            
            # Try to download and send each URL
            success_count = 0
            for i, download_url in enumerate(filtered_urls):
                try:
                    # Download the song
                    temp_file = await self._download_song_file(download_url, song_data, i)
                    if not temp_file:
                        continue
                    
                    # Prepare caption
                    caption = self._prepare_song_caption(song_data, track_info)
                    
                    # Send to dump channel
                    await worker.send_audio(
                        chat_id=dump_channel,
                        audio=temp_file,
                        caption=caption,
                        title=song_data.get('title', 'Unknown'),
                        performer=song_data.get('singers', 'Unknown'),
                        duration=int(song_data.get('duration', 0)) if song_data.get('duration') else 0
                    )
                    
                    # Clean up temp file
                    os.remove(temp_file)
                    success_count += 1
                    
                    logger.info(f"Successfully sent song: {song_data.get('title', 'Unknown')} (Quality {i+1})")
                    
                except Exception as e:
                    logger.error(f"Error sending song quality {i+1}: {e}")
                    # Clean up temp file if it exists
                    try:
                        if 'temp_file' in locals() and temp_file and os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in download_and_send_song: {e}")
            return False
    
    async def _download_song_file(self, download_url: str, song_data: Dict[str, Any], quality_index: int) -> Optional[str]:
        """Download song file to temporary location."""
        try:
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            safe_title = "".join(c for c in song_data.get('title', 'Unknown') if c.isalnum() or c in (' ', '-', '_'))
            temp_filename = f"{safe_title}_q{quality_index}.mp3"
            temp_path = os.path.join(temp_dir, temp_filename)
            
            # Download the file
            success = await download_file(download_url, temp_path)
            if success and os.path.exists(temp_path):
                return temp_path
            
            return None
            
        except Exception as e:
            logger.error(f"Error downloading song file: {e}")
            return None
    
    def _prepare_song_caption(self, song_data: Dict[str, Any], track_info: Dict[str, Any]) -> str:
        """Prepare caption for the song in the requested format."""
        caption_parts = []
        
        # Title
        title = song_data.get('title') or track_info.get('name', 'Unknown')
        caption_parts.append(f"🎵 {title}")
        
        # Artist
        artists = song_data.get('singers') or track_info.get('artist', 'Unknown')
        caption_parts.append(f"👤 {artists}")
        
        # Spotify Track ID
        if track_info.get('spotify_id'):
            caption_parts.append(f"🆔 {track_info['spotify_id']}")
        
        return "\n".join(caption_parts)
    
    async def shutdown_all_workers(self):
        """Shutdown all worker bots."""
        for worker_id, worker in self.workers.items():
            try:
                await worker.stop()
                logger.info(f"Worker {worker_id} stopped")
            except Exception as e:
                logger.error(f"Error stopping worker {worker_id}: {e}")
        
        self.workers.clear()
        self.worker_status.clear()
