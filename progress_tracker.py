"""
Progress tracking system for worker bots and captain bot monitoring.
Handles real-time progress updates, batch notifications, and failed track reporting.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class ProgressStats:
    """Progress statistics for a task or batch."""
    total: int = 0
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    current: int = 0
    start_time: float = 0.0
    last_update: float = 0.0
    
    @property
    def remaining(self) -> int:
        return self.total - (self.sent + self.skipped + self.failed)
    
    @property
    def completed(self) -> int:
        return self.sent + self.skipped + self.failed
    
    @property
    def is_complete(self) -> bool:
        return self.completed >= self.total
    
    def format_time_taken(self) -> str:
        """Format elapsed time as readable string."""
        elapsed = time.time() - self.start_time
        return self._format_duration(elapsed)
    
    def _format_duration(self, seconds: int) -> str:
        """Format duration in seconds to readable format."""
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        parts = []
        if days > 0:
            parts.append(f"{int(days)}d")
        if hours > 0:
            parts.append(f"{int(hours)}h")
        if minutes > 0:
            parts.append(f"{int(minutes)}m")
        if secs > 0 or not parts:
            parts.append(f"{int(secs)}s")
        
        return " ".join(parts)

class ProgressTracker:
    """Tracks progress for worker bots and provides monitoring capabilities."""
    
    def __init__(self):
        self.worker_progress: Dict[str, ProgressStats] = {}
        self.task_progress: Dict[str, ProgressStats] = {}
        self.failed_tracks: Dict[str, List[Dict[str, Any]]] = {}
        self.progress_messages: Dict[str, int] = {}  # message_id for editing
        self.update_lock = asyncio.Lock()
        
    async def initialize_worker_progress(self, worker_id: str, total_tracks: int, 
                                       chat_id: int, message_id: Optional[int] = None):
        """Initialize progress tracking for a worker bot."""
        async with self.update_lock:
            self.worker_progress[worker_id] = ProgressStats(
                total=total_tracks,
                start_time=time.time(),
                last_update=time.time()
            )
            self.failed_tracks[worker_id] = []
            
            if message_id:
                self.progress_messages[worker_id] = message_id
    
    async def initialize_task_progress(self, task_id: str, total_tracks: int):
        """Initialize progress tracking for a task."""
        async with self.update_lock:
            self.task_progress[task_id] = ProgressStats(
                total=total_tracks,
                start_time=time.time(),
                last_update=time.time()
            )
            self.failed_tracks[task_id] = []
    
    async def update_worker_progress(self, worker_id: str, status: str, 
                                   track_id: Optional[str] = None, 
                                   error: Optional[str] = None,
                                   worker_client = None):
        """Update progress for a worker bot and send progress message."""
        async with self.update_lock:
            if worker_id not in self.worker_progress:
                logger.warning(f"No progress tracking initialized for worker {worker_id}")
                return
            
            progress = self.worker_progress[worker_id]
            progress.current += 1
            progress.last_update = time.time()
            
            # Update counters based on status
            if status == "sent":
                progress.sent += 1
            elif status == "skipped":
                progress.skipped += 1
            elif status == "failed":
                progress.failed += 1
                if track_id and error:
                    self.failed_tracks[worker_id].append({
                        "track_id": track_id,
                        "error": error,
                        "timestamp": time.time()
                    })
            
            # Send progress update every 10 tracks or at completion
            if progress.current % 10 == 0 or progress.is_complete:
                await self._send_worker_progress_update(worker_id, worker_client)
    
    async def _send_worker_progress_update(self, worker_id: str, worker_client = None):
        """Send progress update message for worker bot."""
        if not worker_client or worker_id not in self.worker_progress:
            return
        
        progress = self.worker_progress[worker_id]
        
        if progress.is_complete:
            # Send batch completion message
            message = (
                f"✅ **Batch Done!**\n"
                f"🎵 **Total:** {progress.total:,}\n"
                f"✅ **Sent:** {progress.sent:,}\n"
                f"⏭️ **Skipped:** {progress.skipped:,}\n"
                f"❌ **Failed:** {progress.failed:,}\n"
                f"⏳ **Time Taken:** {progress.format_time_taken()}"
            )
        else:
            # Send progress bar update
            message = (
                f"⬇️ **Downloading {progress.current:,} of {progress.total:,}**\n"
                f"✅ **Sent:** {progress.sent:,}\n"
                f"⏭️ **Skipped:** {progress.skipped:,}\n"
                f"❌ **Failed:** {progress.failed:,}\n"
                f"⏳ **Remaining:** {progress.remaining:,}"
            )
        
        try:
            # Send to worker's own chat for monitoring
            await worker_client.send_message("me", message)
        except Exception as e:
            logger.error(f"Failed to send progress update for worker {worker_id}: {e}")
    
    async def get_worker_progress_summary(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get current progress summary for a worker."""
        if worker_id not in self.worker_progress:
            return None
        
        progress = self.worker_progress[worker_id]
        failed_tracks = self.failed_tracks.get(worker_id, [])
        
        return {
            "worker_id": worker_id,
            "total": progress.total,
            "sent": progress.sent,
            "skipped": progress.skipped,
            "failed": progress.failed,
            "remaining": progress.remaining,
            "current": progress.current,
            "is_complete": progress.is_complete,
            "time_taken": progress.format_time_taken(),
            "failed_tracks_count": len(failed_tracks),
            "recent_failures": failed_tracks[-5:] if failed_tracks else []
        }
    
    async def get_all_workers_summary(self) -> List[Dict[str, Any]]:
        """Get progress summary for all workers."""
        summaries = []
        for worker_id in self.worker_progress:
            summary = await self.get_worker_progress_summary(worker_id)
            if summary:
                summaries.append(summary)
        return summaries
    
    async def get_failed_tracks_batch(self, identifier: str, batch_size: int = 5000) -> List[List[str]]:
        """Get failed tracks in batches for reporting."""
        if identifier not in self.failed_tracks:
            return []
        
        failed_tracks = [track["track_id"] for track in self.failed_tracks[identifier]]
        
        # Split into batches
        batches = []
        for i in range(0, len(failed_tracks), batch_size):
            batch = failed_tracks[i:i + batch_size]
            batches.append(batch)
        
        return batches
    
    async def cleanup_worker_progress(self, worker_id: str):
        """Clean up progress tracking for a worker."""
        async with self.update_lock:
            self.worker_progress.pop(worker_id, None)
            self.failed_tracks.pop(worker_id, None)
            self.progress_messages.pop(worker_id, None)
    
    async def update_task_progress(self, task_id: str, worker_summaries: List[Dict[str, Any]]):
        """Update overall task progress from worker summaries."""
        if task_id not in self.task_progress:
            return
        
        async with self.update_lock:
            task_progress = self.task_progress[task_id]
            
            # Aggregate progress from all workers
            total_sent = sum(w["sent"] for w in worker_summaries)
            total_skipped = sum(w["skipped"] for w in worker_summaries)
            total_failed = sum(w["failed"] for w in worker_summaries)
            
            task_progress.sent = total_sent
            task_progress.skipped = total_skipped
            task_progress.failed = total_failed
            task_progress.last_update = time.time()
    
    async def format_captain_monitoring_message(self, task_id: str) -> str:
        """Format monitoring message for captain bot."""
        if task_id not in self.task_progress:
            return "❌ Task not found"
        
        task_progress = self.task_progress[task_id]
        worker_summaries = await self.get_all_workers_summary()
        
        message_parts = [
            f"📊 **Task Monitoring: {task_id[:8]}...**",
            f"🎵 **Total Tracks:** {task_progress.total:,}",
            ""
        ]
        
        if worker_summaries:
            message_parts.append("👥 **Worker Status:**")
            for i, worker in enumerate(worker_summaries, 1):
                status_emoji = "✅" if worker["is_complete"] else "🔄"
                message_parts.append(
                    f"{status_emoji} **Worker {i}:** {worker['current']:,}/{worker['total']:,} "
                    f"(✅{worker['sent']} ❌{worker['failed']} ⏭️{worker['skipped']})"
                )
            message_parts.append("")
        
        # Overall progress
        completed = task_progress.sent + task_progress.skipped + task_progress.failed
        message_parts.extend([
            f"📈 **Overall Progress:** {completed:,}/{task_progress.total:,}",
            f"✅ **Total Sent:** {task_progress.sent:,}",
            f"⏭️ **Total Skipped:** {task_progress.skipped:,}",
            f"❌ **Total Failed:** {task_progress.failed:,}",
            f"⏳ **Time Elapsed:** {task_progress.format_time_taken()}"
        ])
        
        return "\n".join(message_parts)

# Global progress tracker instance
progress_tracker = ProgressTracker()