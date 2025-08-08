"""
Database module for storing bot configuration and task history.
Uses simple JSON file storage for configuration persistence.
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, List, Optional, Any
from threading import Lock

logger = logging.getLogger(__name__)

class Database:
    """Simple file-based database for bot configuration and data."""
    
    def __init__(self, db_file: str = "bot_data.json"):
        self.db_file = db_file
        self.data = {
            "worker_bots": {},
            "dump_channels": {},
            "user_settings": {},
            "task_history": [],
            "bot_stats": {
                "total_downloads": 0,
                "total_tasks": 0,
                "successful_downloads": 0,
                "failed_downloads": 0
            }
        }
        self.lock = Lock()
        self._load_data()
    
    def _load_data(self):
        """Load data from JSON file."""
        try:
            if os.path.exists(self.db_file):
                with open(self.db_file, 'r', encoding='utf-8') as file:
                    loaded_data = json.load(file)
                    
                    # Merge with default structure
                    for key, value in loaded_data.items():
                        if key in self.data:
                            self.data[key] = value
                
                logger.info(f"Database loaded from {self.db_file}")
            else:
                logger.info("Database file not found, starting with empty database")
                
        except Exception as e:
            logger.error(f"Error loading database: {e}")
            logger.info("Starting with empty database")
    
    def _save_data(self):
        """Save data to JSON file."""
        try:
            with self.lock:
                # Create backup of existing file
                if os.path.exists(self.db_file):
                    backup_file = f"{self.db_file}.backup"
                    try:
                        os.rename(self.db_file, backup_file)
                    except Exception:
                        pass
                
                # Save current data
                with open(self.db_file, 'w', encoding='utf-8') as file:
                    json.dump(self.data, file, indent=2, ensure_ascii=False)
                
                # Remove backup on successful save
                backup_file = f"{self.db_file}.backup"
                if os.path.exists(backup_file):
                    try:
                        os.remove(backup_file)
                    except Exception:
                        pass
                
        except Exception as e:
            logger.error(f"Error saving database: {e}")
            
            # Restore from backup if available
            backup_file = f"{self.db_file}.backup"
            if os.path.exists(backup_file):
                try:
                    os.rename(backup_file, self.db_file)
                    logger.info("Restored database from backup")
                except Exception:
                    pass
    
    async def initialize(self):
        """Initialize the database."""
        try:
            # Ensure data directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_file)), exist_ok=True)
            
            # Load existing data
            self._load_data()
            
            # Clean up old data
            await self._cleanup_old_data()
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    async def save_worker_bot(self, worker_id: str, bot_token: str, user_id: int) -> bool:
        """Save worker bot configuration."""
        try:
            worker_data = {
                "worker_id": worker_id,
                "bot_token": bot_token,
                "user_id": user_id,
                "added_at": time.time(),
                "status": "active"
            }
            
            if str(user_id) not in self.data["worker_bots"]:
                self.data["worker_bots"][str(user_id)] = {}
            
            self.data["worker_bots"][str(user_id)][worker_id] = worker_data
            self._save_data()
            
            logger.info(f"Saved worker bot {worker_id} for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving worker bot: {e}")
            return False
    
    async def get_worker_bots(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all worker bots for a user."""
        try:
            user_workers = self.data["worker_bots"].get(str(user_id), {})
            return list(user_workers.values())
            
        except Exception as e:
            logger.error(f"Error getting worker bots: {e}")
            return []
    
    async def remove_worker_bot(self, worker_id: str, user_id: int) -> bool:
        """Remove a worker bot."""
        try:
            user_workers = self.data["worker_bots"].get(str(user_id), {})
            
            if worker_id in user_workers:
                del user_workers[worker_id]
                self._save_data()
                
                logger.info(f"Removed worker bot {worker_id} for user {user_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error removing worker bot: {e}")
            return False
    
    async def save_dump_channel(self, user_id: int, channel_id: int) -> bool:
        """Save dump channel configuration."""
        try:
            self.data["dump_channels"][str(user_id)] = {
                "channel_id": channel_id,
                "set_at": time.time()
            }
            self._save_data()
            
            logger.info(f"Saved dump channel {channel_id} for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving dump channel: {e}")
            return False
    
    async def get_dump_channel(self, user_id: int) -> Optional[int]:
        """Get dump channel for a user."""
        try:
            channel_data = self.data["dump_channels"].get(str(user_id))
            return channel_data["channel_id"] if channel_data else None
            
        except Exception as e:
            logger.error(f"Error getting dump channel: {e}")
            return None
    
    async def save_user_settings(self, user_id: int, settings: Dict[str, Any]) -> bool:
        """Save user settings."""
        try:
            if str(user_id) not in self.data["user_settings"]:
                self.data["user_settings"][str(user_id)] = {}
            
            self.data["user_settings"][str(user_id)].update(settings)
            self.data["user_settings"][str(user_id)]["updated_at"] = time.time()
            self._save_data()
            
            logger.info(f"Saved settings for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving user settings: {e}")
            return False
    
    async def get_user_settings(self, user_id: int) -> Dict[str, Any]:
        """Get user settings."""
        try:
            return self.data["user_settings"].get(str(user_id), {})
            
        except Exception as e:
            logger.error(f"Error getting user settings: {e}")
            return {}
    
    async def save_task_history(self, task_data: Dict[str, Any]) -> bool:
        """Save task to history."""
        try:
            # Add timestamp if not present
            if "completed_at" not in task_data:
                task_data["completed_at"] = time.time()
            
            # Add to history
            self.data["task_history"].append(task_data)
            
            # Keep only recent history (last 100 tasks)
            if len(self.data["task_history"]) > 100:
                self.data["task_history"] = self.data["task_history"][-100:]
            
            # Update stats
            self._update_stats(task_data)
            
            self._save_data()
            
            logger.info(f"Saved task {task_data.get('task_id', 'unknown')} to history")
            return True
            
        except Exception as e:
            logger.error(f"Error saving task history: {e}")
            return False
    
    async def get_task_history(self, user_id: Optional[int] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get task history, optionally filtered by user."""
        try:
            history = self.data["task_history"]
            
            # Filter by user if specified
            if user_id is not None:
                history = [task for task in history if task.get("user_id") == user_id]
            
            # Sort by completion time (most recent first)
            history.sort(key=lambda x: x.get("completed_at", 0), reverse=True)
            
            return history[:limit]
            
        except Exception as e:
            logger.error(f"Error getting task history: {e}")
            return []
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get bot statistics."""
        try:
            stats = self.data["bot_stats"].copy()
            
            # Add current active counts
            stats["total_users"] = len(self.data["user_settings"])
            stats["total_worker_bots"] = sum(len(workers) for workers in self.data["worker_bots"].values())
            stats["total_dump_channels"] = len(self.data["dump_channels"])
            
            # Calculate success rate
            total_attempts = stats["successful_downloads"] + stats["failed_downloads"]
            if total_attempts > 0:
                stats["success_rate"] = (stats["successful_downloads"] / total_attempts) * 100
            else:
                stats["success_rate"] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def _update_stats(self, task_data: Dict[str, Any]):
        """Update bot statistics based on completed task."""
        try:
            stats = self.data["bot_stats"]
            
            stats["total_tasks"] += 1
            
            progress = task_data.get("progress", {})
            successful = progress.get("successful", 0)
            failed = progress.get("failed", 0)
            
            stats["successful_downloads"] += successful
            stats["failed_downloads"] += failed
            stats["total_downloads"] += successful + failed
            
        except Exception as e:
            logger.error(f"Error updating stats: {e}")
    
    async def _cleanup_old_data(self, max_age_days: int = 30):
        """Clean up old data."""
        try:
            current_time = time.time()
            cutoff_time = current_time - (max_age_days * 24 * 3600)
            
            # Clean up old task history
            original_count = len(self.data["task_history"])
            self.data["task_history"] = [
                task for task in self.data["task_history"]
                if task.get("completed_at", 0) > cutoff_time
            ]
            
            cleaned_count = original_count - len(self.data["task_history"])
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} old task history entries")
                self._save_data()
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
    
    async def backup_database(self, backup_file: Optional[str] = None) -> bool:
        """Create a backup of the database."""
        try:
            if not backup_file:
                timestamp = int(time.time())
                backup_file = f"{self.db_file}.backup.{timestamp}"
            
            import shutil
            shutil.copy2(self.db_file, backup_file)
            
            logger.info(f"Database backed up to {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error backing up database: {e}")
            return False
    
    async def restore_database(self, backup_file: str) -> bool:
        """Restore database from backup."""
        try:
            if not os.path.exists(backup_file):
                logger.error(f"Backup file not found: {backup_file}")
                return False
            
            import shutil
            shutil.copy2(backup_file, self.db_file)
            
            # Reload data
            self._load_data()
            
            logger.info(f"Database restored from {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error restoring database: {e}")
            return False
