"""
Captain Bot - Main bot that supervises worker bots and manages song downloading tasks.
Handles Spotify track ID processing, task distribution, and worker bot coordination.
"""

import asyncio
import logging
import os
import tempfile
import time
from typing import List, Dict, Any
from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, Document
from spotify_client import SpotifyClient
from jiosaavn_client import JioSaavnClient
from task_manager import TaskManager
from worker_bot import WorkerBotManager
from database import Database
from utils import download_file, extract_track_ids, validate_spotify_track_id, validate_telegram_channel
from config import Config
import pyroutils
from unlimited_processor import UnlimitedProcessor
from progress_tracker import progress_tracker

logger = logging.getLogger(__name__)

class CaptainBot:
    def __init__(self, config: Config):
        self.config = config
        self.app = Client(
            "captain_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.TELEGRAM_BOT_TOKEN
        )
        
        # Initialize components for UNLIMITED PROCESSING
        self.spotify_client = SpotifyClient(config)
        self.jiosaavn_client = JioSaavnClient()
        self.task_manager = TaskManager()
        self.worker_manager = WorkerBotManager()
        self.database = Database()
        self.unlimited_processor = UnlimitedProcessor()  # For unlimited tracks and workers
        
        # Configure unlimited processing parameters
        self.config.MAX_TRACKS_PER_TASK = 999999999  # Override to unlimited
        self.config.MAX_WORKERS = 999999  # Override to unlimited workers
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register message handlers for the bot."""
        
        @self.app.on_message(filters.command("start"))
        async def start_command(client, message: Message):
            await self._handle_start(message)
        
        @self.app.on_message(filters.command("help"))
        async def help_command(client, message: Message):
            await self._handle_help(message)
        
        @self.app.on_message(filters.command("add_worker"))
        async def add_worker_command(client, message: Message):
            await self._handle_add_worker(message)
        
        @self.app.on_message(filters.command("list_workers"))
        async def list_workers_command(client, message: Message):
            await self._handle_list_workers(message)
        
        @self.app.on_message(filters.command("remove_worker"))
        async def remove_worker_command(client, message: Message):
            await self._handle_remove_worker(message)
        
        @self.app.on_message(filters.command("set_dump_channel"))
        async def set_dump_channel_command(client, message: Message):
            await self._handle_set_dump_channel(message)
        
        @self.app.on_message(filters.command("download") & filters.reply)
        async def download_command(client, message: Message):
            await self._handle_download(message)
        
        @self.app.on_message(filters.command("status"))
        async def status_command(client, message: Message):
            await self._handle_status(message)
        
        @self.app.on_message(filters.document)
        async def document_handler(client, message: Message):
            await self._handle_document(message)
        
        @self.app.on_message(filters.command("monitor"))
        async def monitor_command(client, message: Message):
            await self._handle_monitor(message)
        
        @self.app.on_message(filters.command("failed"))
        async def failed_command(client, message: Message):
            await self._handle_failed(message)
    
    async def _handle_start(self, message: Message):
        """Handle /start command."""
        welcome_text = """
🎵 **Captain Bot - Music Download Manager**

I'm your music download supervisor! I can manage worker bots to download songs from JioSaavn using Spotify track IDs.

**Commands:**
• `/help` - Show detailed help
• `/add_worker <bot_token>` - Add a worker bot
• `/list_workers` - List all worker bots
• `/remove_worker <worker_id>` - Remove a worker bot
• `/set_dump_channel <channel_id>` - Set dump channel for downloads
• `/download` - Download songs (reply to a .txt file with Spotify track IDs)
• `/status` - Show system status
• `/monitor` - Monitor active tasks and worker progress
• `/failed <task_id>` - Get failed tracks for a task in batches
• `/status` - Show current status

**Setup Steps:**
1. Add worker bots using `/add_worker`
2. Set dump channel using `/set_dump_channel`
3. Send a .txt file with Spotify track IDs
4. Reply to the file with `/download` command

Let's get started! 🚀
        """
        await message.reply_text(welcome_text)
    
    async def _handle_help(self, message: Message):
        """Handle /help command."""
        help_text = """
📖 **Detailed Help**

**Worker Bot Management:**
• `/add_worker <bot_token>` - Add a new worker bot
  Example: `/add_worker 1234567890:ABCdef...`

• `/list_workers` - Show all registered worker bots
• `/remove_worker <worker_id>` - Remove a worker bot by ID

**Channel Setup:**
• `/set_dump_channel <channel_id>` - Set channel where songs will be sent
  Example: `/set_dump_channel -1001234567890`

**Song Download Process:**
1. Upload a .txt file containing Spotify track IDs (one per line)
2. Reply to that file with `/download` command
3. Bot will process all track IDs and distribute download tasks among workers

**File Format Example:**
```
4uLU6hMCjMI75M1A2tKUQC
https://open.spotify.com/track/1A2B3C4D5E6F7G8H9I0J1K
spotify:track:2B3C4D5E6F7G8H9I0J1K2L
```

**Need Help?** Contact your bot administrator.
        """
        await message.reply_text(help_text)
    
    async def _handle_add_worker(self, message: Message):
        """Handle /add_worker command."""
        try:
            # Extract bot token from command
            args = message.text.split()[1:]
            if not args:
                await message.reply_text("❌ Please provide a bot token.\n\nExample: `/add_worker 1234567890:ABCdef...`")
                return
            
            bot_token = args[0]
            
            # Validate bot token format
            if ':' not in bot_token or len(bot_token.split(':')[0]) < 8:
                await message.reply_text("❌ Invalid bot token format.")
                return
            
            # Add worker bot
            worker_id = await self.worker_manager.add_worker(bot_token)
            
            if worker_id:
                # Save to database
                await self.database.save_worker_bot(worker_id, bot_token, message.from_user.id)
                await message.reply_text(f"✅ Worker bot added successfully!\n\n**Worker ID:** `{worker_id}`")
            else:
                await message.reply_text("❌ Failed to add worker bot. Please check the token is valid and the bot is accessible.")
                
        except Exception as e:
            logger.error(f"Error adding worker: {e}")
            await message.reply_text("❌ An error occurred while adding the worker bot.")
    
    async def _handle_list_workers(self, message: Message):
        """Handle /list_workers command."""
        try:
            worker_bots = await self.database.get_worker_bots(message.from_user.id)
            available_workers = await self.worker_manager.get_available_workers()
            
            if not worker_bots:
                await message.reply_text("📝 No worker bots configured.\n\nUse `/add_worker <bot_token>` to add one.")
                return
            
            response_parts = ["👥 **Your Worker Bots:**\n"]
            
            for worker in worker_bots:
                worker_id = worker['worker_id']
                status = "🟢 Online" if worker_id in available_workers else "🔴 Offline"
                response_parts.append(f"• **{worker_id}** - {status}")
            
            response_parts.append(f"\n**Total:** {len(worker_bots)} worker bots")
            response_parts.append(f"**Online:** {len(available_workers)} worker bots")
            
            await message.reply_text("\n".join(response_parts))
            
        except Exception as e:
            logger.error(f"Error listing workers: {e}")
            await message.reply_text("❌ An error occurred while listing worker bots.")
    
    async def _handle_remove_worker(self, message: Message):
        """Handle /remove_worker command."""
        try:
            args = message.text.split()[1:]
            if not args:
                await message.reply_text("❌ Please provide a worker ID.\n\nExample: `/remove_worker 1234567890`")
                return
            
            worker_id = args[0]
            
            # Remove from worker manager
            removed = await self.worker_manager.remove_worker(worker_id)
            
            if removed:
                # Remove from database
                db_removed = await self.database.remove_worker_bot(worker_id, message.from_user.id)
                
                if db_removed:
                    await message.reply_text(f"✅ Worker bot `{worker_id}` removed successfully!")
                else:
                    await message.reply_text(f"⚠️ Worker bot removed from system but not found in your records.")
            else:
                await message.reply_text(f"❌ Worker bot `{worker_id}` not found.")
                
        except Exception as e:
            logger.error(f"Error removing worker: {e}")
            await message.reply_text("❌ An error occurred while removing the worker bot.")
    
    async def _handle_set_dump_channel(self, message: Message):
        """Handle /set_dump_channel command."""
        try:
            args = message.text.split()[1:]
            if not args:
                await message.reply_text("❌ Please provide a channel ID.\n\nExample: `/set_dump_channel -1001234567890`")
                return
            
            try:
                channel_id = int(args[0])
            except ValueError:
                await message.reply_text("❌ Invalid channel ID. Please provide a numeric channel ID.")
                return
            
            # Validate channel ID format
            if not await validate_telegram_channel(channel_id):
                await message.reply_text("❌ Invalid channel ID format. Channel IDs should be negative numbers starting with -100.")
                return
            
            # Save dump channel
            saved = await self.database.save_dump_channel(message.from_user.id, channel_id)
            
            if saved:
                await message.reply_text(f"✅ Dump channel set to: `{channel_id}`\n\nMake sure the worker bots have admin access to this channel.")
            else:
                await message.reply_text("❌ Failed to save dump channel configuration.")
                
        except Exception as e:
            logger.error(f"Error setting dump channel: {e}")
            await message.reply_text("❌ An error occurred while setting the dump channel.")
    
    async def _handle_download(self, message: Message):
        """Handle /download command when replying to a file."""
        try:
            if not message.reply_to_message or not message.reply_to_message.document:
                await message.reply_text("❌ Please reply to a .txt file containing Spotify track IDs.")
                return
            
            document = message.reply_to_message.document
            
            # Check file extension
            if not document.file_name.endswith('.txt'):
                await message.reply_text("❌ Please upload a .txt file.")
                return
            
            # Check file size
            if document.file_size > 5 * 1024 * 1024:  # 5MB limit
                await message.reply_text("❌ File too large. Please upload a file smaller than 5MB.")
                return
            
            # Check if user has workers and dump channel configured
            worker_bots = await self.database.get_worker_bots(message.from_user.id)
            dump_channel = await self.database.get_dump_channel(message.from_user.id)
            
            if not worker_bots:
                await message.reply_text("❌ No worker bots configured. Use `/add_worker` to add worker bots first.")
                return
            
            if not dump_channel:
                await message.reply_text("❌ No dump channel configured. Use `/set_dump_channel` to set a channel first.")
                return
            
            # Download and process file
            processing_msg = await message.reply_text("🔄 Processing your file...")
            
            # Download file
            file_path = await message.reply_to_message.download(file_name=f"tracks_{message.from_user.id}_{int(time.time())}.txt")
            
            # Extract track IDs
            track_ids = extract_track_ids(file_path)
            
            # Clean up downloaded file
            os.remove(file_path)
            
            if not track_ids:
                await processing_msg.edit_text("❌ No valid Spotify track IDs found in the file.")
                return
            
            # UNLIMITED TRACKS PROCESSING - No limits for massive scale
            track_count = len(track_ids)
            if track_count > 2000000:  # Only warn for extremely large files (2M+)
                await processing_msg.edit_text(
                    f"⚡ **UNLIMITED PROCESSING MODE**\n\n"
                    f"📊 **Processing {track_count:,} tracks**\n"
                    f"🚀 This will take significant time but will process ALL tracks\n"
                    f"⏳ Estimated time: {track_count//1000:.0f} minutes with multiple workers\n\n"
                    f"✅ Starting unlimited processing..."
                )
            else:
                await processing_msg.edit_text(
                    f"⚡ **UNLIMITED PROCESSING**\n"
                    f"📊 Processing {track_count:,} tracks\n"
                    f"🚀 Starting unlimited asynchronous distribution..."
                )
            
            # Initialize Spotify client if needed
            if not self.spotify_client.spotify:
                spotify_init = await self.spotify_client.initialize()
                if not spotify_init:
                    await processing_msg.edit_text(
                        "⚠️ **Spotify API Warning**\n\n"
                        "🟡 All Spotify clients are rate limited - proceeding without metadata\n"
                        "🎵 Songs will be downloaded using basic search without Spotify info\n"
                        "⏳ Consider waiting a few hours for rate limits to reset for better accuracy"
                    )
                    # Continue without Spotify metadata instead of returning
            
            # Create unlimited task (supports 2M+ tracks)
            task_id = await self.task_manager.create_task(
                user_id=message.from_user.id,
                track_ids=track_ids,
                dump_channel=dump_channel,
                total_tracks=len(track_ids)
            )
            
            # Initialize unlimited processing architecture and progress tracking
            available_workers = await self.worker_manager.get_available_workers()
            if available_workers:
                await self.unlimited_processor.initialize_unlimited_processing(
                    total_tracks=len(track_ids),
                    worker_ids=[int(w) for w in available_workers]
                )
                
                # Initialize progress tracking for task
                await progress_tracker.initialize_task_progress(task_id, len(track_ids))
            
            # Start processing with unlimited capabilities
            await processing_msg.edit_text(
                f"✅ **UNLIMITED TASK CREATED**\n"
                f"🆔 Task ID: `{task_id}`\n"
                f"📊 Processing {len(track_ids):,} tracks\n"
                f"🎯 Target channel: `{dump_channel}`\n"
                f"👥 Available workers: {len(available_workers) if available_workers else 0}\n\n"
                f"⚡ **UNLIMITED PROCESSING STARTED**\n"
                f"🚀 All {len(track_ids):,} tracks will be processed asynchronously"
            )
            
            # Process tracks asynchronously with progress tracking
            asyncio.create_task(self._process_download_task_with_progress(task_id, track_ids, dump_channel, message.from_user.id))
            
        except Exception as e:
            logger.error(f"Error handling download: {e}")
            await message.reply_text("❌ An error occurred while processing your download request.")
    
    async def _handle_status(self, message: Message):
        """Handle /status command."""
        try:
            # Get user's active tasks
            active_tasks = await self.task_manager.get_active_tasks(message.from_user.id)
            
            # Get worker status
            worker_bots = await self.database.get_worker_bots(message.from_user.id)
            available_workers = await self.worker_manager.get_available_workers()
            
            # Get dump channel
            dump_channel = await self.database.get_dump_channel(message.from_user.id)
            
            # Build status message
            status_parts = ["📊 **Bot Status**\n"]
            
            # Worker status
            status_parts.append(f"👥 **Workers:** {len(available_workers)}/{len(worker_bots)} online")
            
            # Dump channel status
            if dump_channel:
                status_parts.append(f"🎯 **Dump Channel:** `{dump_channel}`")
            else:
                status_parts.append("🎯 **Dump Channel:** Not configured")
            
            # Active tasks
            if active_tasks:
                status_parts.append(f"\n🔄 **Active Tasks:** {len(active_tasks)}")
                for task in active_tasks[:3]:  # Show max 3 tasks
                    progress = task['progress']
                    completed = progress['successful'] + progress['failed']
                    total = progress['total']
                    percentage = (completed / total) * 100 if total > 0 else 0
                    status_parts.append(f"• Task `{task['task_id'][:8]}...` - {percentage:.1f}% ({completed}/{total})")
            else:
                status_parts.append("\n🔄 **Active Tasks:** None")
            
            # System status
            status_parts.append(f"\n🟢 **System:** Online")
            status_parts.append(f"🔗 **JioSaavn API:** {self.jiosaavn_client.base_url}")
            
            await message.reply_text("\n".join(status_parts))
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            await message.reply_text("❌ An error occurred while getting the status.")
    
    async def _handle_document(self, message: Message):
        """Handle document uploads."""
        try:
            document = message.document
            
            if document.file_name and document.file_name.endswith('.txt'):
                help_msg = (
                    "📄 **Text file detected!**\n\n"
                    "If this file contains Spotify track IDs, reply to this message with `/download` to process them.\n\n"
                    "**Supported formats:**\n"
                    "• Direct track IDs: `4uLU6hMCjMI75M1A2tKUQC`\n"
                    "• Spotify URLs: `https://open.spotify.com/track/...`\n"
                    "• Spotify URIs: `spotify:track:...`"
                )
                await message.reply_text(help_msg)
            
        except Exception as e:
            logger.error(f"Error handling document: {e}")
    
    async def _handle_monitor(self, message: Message):
        """Handle /monitor command - show active task monitoring."""
        try:
            # Get all active tasks
            active_tasks = await self.task_manager.get_active_tasks(message.from_user.id)
            
            if not active_tasks:
                await message.reply_text("📊 No active tasks found.")
                return
            
            # Show monitoring for each active task
            for task in active_tasks:
                task_id = task['task_id']
                monitoring_msg = await progress_tracker.format_captain_monitoring_message(task_id)
                
                await message.reply_text(monitoring_msg)
                
                # Also show individual worker progress
                worker_summaries = await progress_tracker.get_all_workers_summary()
                
                if worker_summaries:
                    worker_details = ["👥 **Individual Worker Progress:**"]
                    
                    for worker in worker_summaries:
                        worker_id = worker['worker_id']
                        progress_msg = (
                            f"🤖 **Worker {worker_id[:8]}...:**\n"
                            f"⬇️ **Downloading {worker['current']:,} of {worker['total']:,}**\n"
                            f"✅ **Sent:** {worker['sent']:,}\n"
                            f"⏭️ **Skipped:** {worker['skipped']:,}\n"
                            f"❌ **Failed:** {worker['failed']:,}\n"
                            f"⏳ **Remaining:** {worker['remaining']:,}"
                        )
                        
                        if worker['is_complete']:
                            progress_msg = (
                                f"✅ **Worker {worker_id[:8]}... - BATCH DONE!**\n"
                                f"🎵 **Total:** {worker['total']:,}\n"
                                f"✅ **Sent:** {worker['sent']:,}\n"
                                f"⏭️ **Skipped:** {worker['skipped']:,}\n"
                                f"❌ **Failed:** {worker['failed']:,}\n"
                                f"⏳ **Time Taken:** {worker['time_taken']}"
                            )
                        
                        worker_details.append(progress_msg)
                    
                    await message.reply_text("\n\n".join(worker_details))
                
        except Exception as e:
            logger.error(f"Error handling monitor command: {e}")
            await message.reply_text("❌ An error occurred while monitoring tasks.")
    
    async def _handle_failed(self, message: Message):
        """Handle /failed command - show failed tracks in batches."""
        try:
            # Get task ID from command arguments
            command_parts = message.text.split()
            if len(command_parts) < 2:
                await message.reply_text(
                    "❌ **Usage:** `/failed <task_id>`\n\n"
                    "Use `/monitor` to see active task IDs."
                )
                return
            
            task_id_partial = command_parts[1]
            
            # Find matching task
            active_tasks = await self.task_manager.get_active_tasks(message.from_user.id)
            completed_tasks = self.task_manager.task_history
            
            matching_task = None
            for task in active_tasks + completed_tasks:
                if task['task_id'].startswith(task_id_partial):
                    matching_task = task
                    break
            
            if not matching_task:
                await message.reply_text(f"❌ Task not found: {task_id_partial}")
                return
            
            # Get failed tracks in batches of 5000
            failed_batches = await progress_tracker.get_failed_tracks_batch(
                matching_task['task_id'], batch_size=5000
            )
            
            if not failed_batches:
                await message.reply_text("✅ No failed tracks found for this task!")
                return
            
            # Send failed tracks summary and first batch
            total_failed = sum(len(batch) for batch in failed_batches)
            
            summary_msg = (
                f"❌ **Failed Tracks Report**\n\n"
                f"🆔 **Task:** {matching_task['task_id'][:8]}...\n"
                f"📊 **Total Failed:** {total_failed:,} tracks\n"
                f"📦 **Batches:** {len(failed_batches)} (5000 tracks each)\n\n"
            )
            
            # Add first batch of failed track IDs
            if failed_batches:
                first_batch = failed_batches[0]
                summary_msg += f"**📄 Batch 1/{len(failed_batches)} ({len(first_batch):,} tracks):**\n"
                summary_msg += "\n".join(first_batch[:50])  # Show first 50 IDs
                
                if len(first_batch) > 50:
                    summary_msg += f"\n... and {len(first_batch) - 50:,} more tracks in this batch"
            
            await message.reply_text(summary_msg)
            
            # Send remaining batches if requested
            if len(failed_batches) > 1:
                await message.reply_text(
                    f"📦 **{len(failed_batches) - 1} more batches available.**\n"
                    f"Reply with numbers (e.g., '2' or '2-5') to get specific batches."
                )
                
        except Exception as e:
            logger.error(f"Error handling failed command: {e}")
            await message.reply_text("❌ An error occurred while getting failed tracks.")
    
    async def start(self):
        """Start the captain bot."""
        try:
            # Initialize components
            await self.database.initialize()
            await self.spotify_client.initialize()
            
            # Start the bot
            await self.app.start()
            logger.info("Captain Bot started successfully")
            
            # Keep the bot running
            await asyncio.Event().wait()
            
        except KeyboardInterrupt:
            logger.info("Bot stopping...")
        except Exception as e:
            logger.error(f"Error running bot: {e}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown the bot and cleanup resources."""
        try:
            await self.worker_manager.shutdown_all_workers()
            await self.jiosaavn_client.close()
            await self.app.stop()
            logger.info("Captain Bot shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def _process_download_task_with_progress(self, task_id: str, track_ids: List[str], dump_channel: int, user_id: int):
        """Process download task with enhanced progress tracking and monitoring."""
        try:
            # Get available workers
            available_workers = await self.worker_manager.get_available_workers()
            
            if not available_workers:
                logger.error(f"No available workers for task {task_id}")
                await self.task_manager.cancel_task(task_id)
                return
            
            logger.info(f"Processing task {task_id} with {len(available_workers)} workers and {len(track_ids)} tracks")
            
            # Get Spotify track information first
            try:
                if not self.spotify_client.spotify:
                    await self.spotify_client.initialize()
                
                if self.spotify_client.spotify:
                    tracks_info = await self.spotify_client.get_multiple_tracks(track_ids)
                    logger.info(f"Retrieved Spotify info for {len(tracks_info)} tracks")
                else:
                    logger.warning("Spotify client not available - using track IDs as fallback")
                    tracks_info = {}
            except Exception as e:
                logger.error(f"Failed to get Spotify track info: {e}")
                # Continue with empty track info - workers will handle this
                tracks_info = {}
            
            # Distribute tracks across workers
            tracks_per_worker = len(track_ids) // len(available_workers)
            remainder = len(track_ids) % len(available_workers)
            
            start_idx = 0
            worker_tasks = []
            
            for i, worker_id in enumerate(available_workers):
                # Calculate tracks for this worker
                worker_tracks = tracks_per_worker + (1 if i < remainder else 0)
                end_idx = start_idx + worker_tracks
                worker_track_ids = track_ids[start_idx:end_idx]
                
                if worker_track_ids:  # Only process if worker has tracks
                    # Initialize progress tracking for this worker
                    await progress_tracker.initialize_worker_progress(
                        worker_id, len(worker_track_ids), user_id
                    )
                    
                    # Start worker processing
                    task = asyncio.create_task(
                        self._process_worker_batch(task_id, worker_id, worker_track_ids, tracks_info, dump_channel)
                    )
                    worker_tasks.append(task)
                    
                    logger.info(f"Assigned {len(worker_track_ids)} tracks to worker {worker_id}")
                
                start_idx = end_idx
            
            # Wait for all workers to complete
            if worker_tasks:
                results = await asyncio.gather(*worker_tasks, return_exceptions=True)
                logger.info(f"All workers completed for task {task_id}")
                
                # Update final task status
                await self.task_manager.update_task_progress(task_id, "", "completed")
            else:
                logger.error(f"No worker tasks created for task {task_id}")
                await self.task_manager.cancel_task(task_id)
            
        except Exception as e:
            logger.error(f"Error in enhanced download processing: {e}")
            await self.task_manager.cancel_task(task_id)

    async def _process_download_task(self, task_id: str, track_ids: List[str], dump_channel: int, user_id: int):
        """Process download task by distributing work among available workers."""
        try:
            # Get available workers
            available_workers = await self.worker_manager.get_available_workers()
            
            if not available_workers:
                await self.task_manager.cancel_task(task_id)
                return
            
            # Get track information from Spotify
            tracks_info = await self.spotify_client.get_multiple_tracks(track_ids)
            
            # Process tracks in chunks per worker
            from utils import split_list
            chunk_size = max(1, len(track_ids) // len(available_workers))
            track_chunks = split_list(track_ids, chunk_size)
            
            # Assign work to workers
            tasks = []
            for i, chunk in enumerate(track_chunks):
                if i < len(available_workers):
                    worker_id = available_workers[i]
                    await self.task_manager.assign_worker_to_task(task_id, worker_id, chunk)
                    
                    # Create async task for this worker
                    task = asyncio.create_task(
                        self._process_worker_chunk(task_id, worker_id, chunk, tracks_info, dump_channel)
                    )
                    tasks.append(task)
            
            # Wait for all workers to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update final task progress
            successful = sum(r.get('successful', 0) for r in results if isinstance(r, dict))
            failed = sum(r.get('failed', 0) for r in results if isinstance(r, dict))
            
            await self.task_manager.update_task_progress(task_id, successful, failed)
            
            # Save task to history
            task_data = await self.task_manager.get_task_status(task_id)
            if task_data:
                await self.database.save_task_history(task_data)
            
        except Exception as e:
            logger.error(f"Error processing download task {task_id}: {e}")
            await self.task_manager.cancel_task(task_id)
    
    async def _process_worker_batch(self, task_id: str, worker_id: str, track_ids: List[str], 
                                   tracks_info: Dict[str, Any], dump_channel: int) -> Dict[str, int]:
        """Process a batch of tracks using a specific worker with proper task distribution."""
        successful = 0
        failed = 0
        
        try:
            logger.info(f"Worker {worker_id} starting batch of {len(track_ids)} tracks")
            
            # Process each track for this worker
            for track_id in track_ids:
                try:
                    # Get Spotify track info
                    spotify_data = tracks_info.get(track_id, {
                        'id': track_id,
                        'name': 'Unknown',
                        'artist': 'Unknown'
                    })
                    
                    # Create task data for worker
                    task_data = {
                        'task_id': task_id,
                        'track_id': track_id,
                        'dump_channel': dump_channel,
                        'spotify_data': spotify_data
                    }
                    
                    # Add to worker queue for processing
                    success = await self.worker_manager.add_track_to_worker_queue(worker_id, task_data)
                    
                    if success:
                        # Update task progress
                        await self.task_manager.update_task_progress(task_id, track_id, "in_progress")
                        logger.debug(f"Added track {track_id} to worker {worker_id} queue")
                    else:
                        failed += 1
                        await self.task_manager.update_task_progress(task_id, track_id, "failed", "Failed to add to worker queue")
                        
                except Exception as e:
                    logger.error(f"Error processing track {track_id} for worker {worker_id}: {e}")
                    failed += 1
                    await self.task_manager.update_task_progress(task_id, track_id, "failed", str(e))
            
            logger.info(f"Worker {worker_id} batch queuing completed: {len(track_ids) - failed} queued, {failed} failed")
            
        except Exception as e:
            logger.error(f"Error in worker batch processing: {e}")
            failed = len(track_ids)
        
        return {"successful": len(track_ids) - failed, "failed": failed}

    async def _process_worker_chunk(self, task_id: str, worker_id: str, track_ids: List[str], 
                                   tracks_info: Dict[str, Any], dump_channel: int) -> Dict[str, int]:
        """Process a chunk of tracks using a specific worker."""
        successful = 0
        failed = 0
        
        try:
            await self.task_manager.update_worker_progress(task_id, worker_id, "processing")
            
            for track_id in track_ids:
                try:
                    # Get Spotify track info
                    track_info = tracks_info.get(track_id)
                    if not track_info:
                        logger.warning(f"No Spotify info found for track: {track_id}")
                        failed += 1
                        continue
                    
                    # Search for song on JioSaavn
                    search_query = f"{track_info['name']} {track_info['artist']}"
                    songs = await self.jiosaavn_client.search_songs(search_query, limit=3)
                    
                    if not songs:
                        logger.warning(f"No songs found on JioSaavn for: {search_query}")
                        failed += 1
                        continue
                    
                    # Try to download and send the best match
                    download_success = False
                    for song in songs:
                        try:
                            success = await self.worker_manager.download_and_send_song(
                                worker_id, song, dump_channel, track_info
                            )
                            if success:
                                download_success = True
                                break
                        except Exception as e:
                            logger.error(f"Error downloading song: {e}")
                            continue
                    
                    if download_success:
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Error processing track {track_id}: {e}")
                    failed += 1
            
            await self.task_manager.update_worker_progress(task_id, worker_id, "completed")
            
        except Exception as e:
            logger.error(f"Error in worker chunk processing: {e}")
            await self.task_manager.update_worker_progress(task_id, worker_id, "failed")
        
        return {"successful": successful, "failed": failed}
