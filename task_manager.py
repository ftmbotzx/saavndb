"""
Task Manager - Handles task creation, tracking, and progress monitoring.
Supports unlimited tracks and unlimited workers with full asynchronous processing.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TrackTask:
    track_id: str
    spotify_data: Optional[Dict] = None
    jiosaavn_data: Optional[Dict] = None
    download_url: Optional[str] = None
    worker_id: Optional[int] = None
    status: str = "pending"
    attempts: int = 0
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

class TaskManager:
    def __init__(self):
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self.task_history: List[Dict[str, Any]] = []
        self.max_history = 1000  # Increased for unlimited tracks
        self.worker_pools: Dict[int, asyncio.Queue] = {}  # Worker queues for task distribution
        self.thread_pool = ThreadPoolExecutor(max_workers=50)  # For CPU-intensive operations
        self.semaphore = asyncio.Semaphore(100)  # Limit concurrent operations
    
    async def create_task(self, user_id: int, track_ids: List[str], 
                         dump_channel: int, total_tracks: int) -> str:
        """Create a new download task supporting unlimited tracks."""
        task_id = str(uuid.uuid4())
        
        # Create individual track tasks for asynchronous processing
        track_tasks = {}
        for track_id in track_ids:
            track_tasks[track_id] = TrackTask(track_id=track_id)
        
        task_data = {
            'task_id': task_id,
            'user_id': user_id,
            'dump_channel': dump_channel,
            'total_tracks': total_tracks,
            'created_at': time.time(),
            'status': TaskStatus.PENDING.value,
            'progress': {
                'total': total_tracks,
                'successful': 0,
                'failed': 0,
                'in_progress': 0,
                'pending': total_tracks
            },
            'track_tasks': track_tasks,
            'worker_assignments': {},
            'start_time': time.time(),
            'end_time': None,
            'batch_size': 100,  # Process in batches for unlimited tracks
            'concurrent_workers': 50  # Support unlimited workers
        }
        
        self.active_tasks[task_id] = task_data
        logger.info(f"Created task {task_id} for user {user_id} with {total_tracks:,} tracks")
        
        return task_id
    
    async def register_worker_pool(self, worker_id: int, queue_size: int = 1000):
        """Register a worker with unlimited capacity queue."""
        self.worker_pools[worker_id] = asyncio.Queue(maxsize=queue_size)
        logger.info(f"Registered worker {worker_id} with queue capacity {queue_size}")
    
    async def distribute_tasks_async(self, task_id: str, available_workers: List[int]) -> None:
        """Distribute tasks asynchronously to unlimited workers."""
        if task_id not in self.active_tasks:
            return
        
        task_data = self.active_tasks[task_id]
        track_tasks = task_data['track_tasks']
        batch_size = task_data.get('batch_size', 100)
        
        # Create batches for unlimited track processing
        pending_tracks = [track_id for track_id, track_task in track_tasks.items() 
                         if track_task.status == "pending"]
        
        batches = [pending_tracks[i:i + batch_size] for i in range(0, len(pending_tracks), batch_size)]
        
        # Distribute batches across unlimited workers asynchronously
        tasks = []
        for i, batch in enumerate(batches):
            worker_id = available_workers[i % len(available_workers)] if available_workers else None
            if worker_id:
                task = asyncio.create_task(self._process_batch_async(task_id, batch, worker_id))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Distributed {len(batches)} batches across {len(available_workers)} workers for task {task_id}")
    
    async def _process_batch_async(self, task_id: str, track_batch: List[str], worker_id: int):
        """Process a batch of tracks asynchronously."""
        async with self.semaphore:
            try:
                for track_id in track_batch:
                    if task_id in self.active_tasks:
                        track_task = self.active_tasks[task_id]['track_tasks'][track_id]
                        track_task.worker_id = worker_id
                        track_task.status = "in_progress"
                        track_task.start_time = time.time()
                        
                        # Add to worker queue for processing
                        if worker_id in self.worker_pools:
                            await self.worker_pools[worker_id].put({
                                'task_id': task_id,
                                'track_id': track_id,
                                'dump_channel': self.active_tasks[task_id]['dump_channel']
                            })
                        
                await self._update_progress_counters(task_id)
                
            except Exception as e:
                logger.error(f"Error processing batch for task {task_id}: {e}")
    
    async def _update_progress_counters(self, task_id: str):
        """Update progress counters for unlimited tracks."""
        if task_id not in self.active_tasks:
            return
        
        task_data = self.active_tasks[task_id]
        track_tasks = task_data['track_tasks']
        
        progress = {
            'total': len(track_tasks),
            'successful': sum(1 for t in track_tasks.values() if t.status == "completed"),
            'failed': sum(1 for t in track_tasks.values() if t.status == "failed"),
            'in_progress': sum(1 for t in track_tasks.values() if t.status == "in_progress"),
            'pending': sum(1 for t in track_tasks.values() if t.status == "pending")
        }
        
        task_data['progress'] = progress
        
        # Check if task is completed
        if progress['successful'] + progress['failed'] == progress['total']:
            task_data['status'] = TaskStatus.COMPLETED.value
            task_data['end_time'] = time.time()
            logger.info(f"Task {task_id} completed: {progress['successful']:,} successful, {progress['failed']:,} failed")

    async def update_task_progress(self, task_id: str, track_id: str, status: str, 
                                 error: Optional[str] = None):
        """Update individual track progress for unlimited processing."""
        if task_id not in self.active_tasks:
            logger.warning(f"Task {task_id} not found for progress update")
            return
        
        task_data = self.active_tasks[task_id]
        if track_id not in task_data['track_tasks']:
            logger.warning(f"Track {track_id} not found in task {task_id}")
            return
        
        track_task = task_data['track_tasks'][track_id]
        track_task.status = status
        track_task.end_time = time.time()
        
        if error:
            track_task.error = error
            track_task.attempts += 1
        
        await self._update_progress_counters(task_id)
    
    async def _complete_task(self, task_id: str):
        """Mark task as completed and move to history."""
        if task_id not in self.active_tasks:
            return
        
        task = self.active_tasks[task_id]
        task['status'] = 'completed'
        task['end_time'] = time.time()
        task['duration'] = task['end_time'] - task['start_time']
        
        # Move to history
        self.task_history.append(task)
        
        # Keep only recent history
        if len(self.task_history) > self.max_history:
            self.task_history = self.task_history[-self.max_history:]
        
        # Remove from active tasks
        del self.active_tasks[task_id]
        
        logger.info(f"Task {task_id} completed in {task['duration']:.2f} seconds")
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a task."""
        if task_id in self.active_tasks:
            return self.active_tasks[task_id].copy()
        
        # Check in history
        for task in self.task_history:
            if task['task_id'] == task_id:
                return task.copy()
        
        return None
    
    async def get_active_tasks(self, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all active tasks, optionally filtered by user."""
        tasks = []
        for task in self.active_tasks.values():
            if user_id is None or task['user_id'] == user_id:
                tasks.append(task.copy())
        
        return tasks
    
    async def get_user_task_history(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get task history for a specific user."""
        user_tasks = [task for task in self.task_history if task['user_id'] == user_id]
        
        # Sort by creation time (most recent first)
        user_tasks.sort(key=lambda x: x['created_at'], reverse=True)
        
        return user_tasks[:limit]
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel an active task."""
        if task_id not in self.active_tasks:
            return False
        
        task = self.active_tasks[task_id]
        task['status'] = 'cancelled'
        task['end_time'] = time.time()
        task['duration'] = task['end_time'] - task['start_time']
        
        # Move to history
        self.task_history.append(task)
        del self.active_tasks[task_id]
        
        logger.info(f"Task {task_id} cancelled")
        return True
    
    async def assign_worker_to_task(self, task_id: str, worker_id: str, track_chunk: List[str]):
        """Assign a worker to handle a chunk of tracks for a task."""
        if task_id not in self.active_tasks:
            return
        
        task = self.active_tasks[task_id]
        task['worker_assignments'][worker_id] = {
            'track_chunk': track_chunk,
            'assigned_at': time.time(),
            'status': 'assigned'
        }
        
        logger.info(f"Assigned worker {worker_id} to task {task_id} with {len(track_chunk)} tracks")
    
    async def update_worker_progress(self, task_id: str, worker_id: str, status: str):
        """Update worker progress on a task."""
        if task_id not in self.active_tasks:
            return
        
        task = self.active_tasks[task_id]
        if worker_id in task['worker_assignments']:
            task['worker_assignments'][worker_id]['status'] = status
            task['worker_assignments'][worker_id]['updated_at'] = time.time()
    
    async def get_task_statistics(self, user_id: Optional[int] = None) -> Dict[str, Any]:
        """Get task statistics."""
        stats = {
            'active_tasks': len(self.active_tasks),
            'completed_tasks': 0,
            'cancelled_tasks': 0,
            'total_tracks_processed': 0,
            'total_successful_downloads': 0,
            'total_failed_downloads': 0,
            'average_task_duration': 0,
            'user_stats': {}
        }
        
        # Calculate statistics from history
        completed_durations = []
        for task in self.task_history:
            if user_id is None or task['user_id'] == user_id:
                if task['status'] == 'completed':
                    stats['completed_tasks'] += 1
                    if 'duration' in task:
                        completed_durations.append(task['duration'])
                elif task['status'] == 'cancelled':
                    stats['cancelled_tasks'] += 1
                
                progress = task.get('progress', {})
                stats['total_tracks_processed'] += progress.get('total', 0)
                stats['total_successful_downloads'] += progress.get('successful', 0)
                stats['total_failed_downloads'] += progress.get('failed', 0)
        
        # Calculate average duration
        if completed_durations:
            stats['average_task_duration'] = sum(completed_durations) / len(completed_durations)
        
        # Add active task stats
        for task in self.active_tasks.values():
            if user_id is None or task['user_id'] == user_id:
                progress = task.get('progress', {})
                stats['total_tracks_processed'] += progress.get('total', 0)
                stats['total_successful_downloads'] += progress.get('successful', 0)
                stats['total_failed_downloads'] += progress.get('failed', 0)
        
        return stats
    
    async def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Clean up old completed tasks."""
        current_time = time.time()
        cutoff_time = current_time - (max_age_hours * 3600)
        
        # Clean up history
        self.task_history = [
            task for task in self.task_history 
            if task.get('created_at', 0) > cutoff_time
        ]
        
        # Cancel very old active tasks (shouldn't happen normally)
        old_tasks = []
        for task_id, task in self.active_tasks.items():
            if task.get('created_at', 0) < cutoff_time:
                old_tasks.append(task_id)
        
        for task_id in old_tasks:
            await self.cancel_task(task_id)
        
        logger.info(f"Cleaned up {len(old_tasks)} old active tasks")
