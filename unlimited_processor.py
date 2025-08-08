"""
Unlimited Processing Engine - Handles massive scale track processing with unlimited workers.
Implements full asynchronous processing with pyroutils integration.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import time
import pyroutils

logger = logging.getLogger(__name__)

@dataclass
class ProcessingStats:
    total_tracks: int = 0
    processed_tracks: int = 0
    failed_tracks: int = 0
    active_workers: int = 0
    tracks_per_second: float = 0.0
    start_time: Optional[float] = None
    
class UnlimitedProcessor:
    """Engine for processing unlimited tracks with unlimited workers asynchronously."""
    
    def __init__(self):
        self.processing_stats = ProcessingStats()
        self.batch_processors: Dict[str, asyncio.Task] = {}
        self.worker_pools: Dict[int, asyncio.Queue] = {}
        self.semaphore = asyncio.Semaphore(500)  # Global concurrency limit
        self.thread_pool = ThreadPoolExecutor(max_workers=100)
        self.shutdown_event = asyncio.Event()
        
    async def initialize_unlimited_processing(self, total_tracks: int, worker_ids: List[int]):
        """Initialize unlimited processing capabilities."""
        self.processing_stats.total_tracks = total_tracks
        self.processing_stats.active_workers = len(worker_ids)
        self.processing_stats.start_time = time.time()
        
        # Create unlimited worker pools
        for worker_id in worker_ids:
            # Validate worker using pyroutils
            if pyroutils.is_user_id(worker_id) or worker_id < 0:
                self.worker_pools[worker_id] = asyncio.Queue(maxsize=2000)  # Large queues
                logger.info(f"Initialized unlimited processing for worker {worker_id}")
        
        logger.info(f"Unlimited processing initialized: {total_tracks:,} tracks, {len(worker_ids)} workers")
        
    async def distribute_tracks_unlimited(self, tracks: List[Dict[str, Any]], 
                                        worker_ids: List[int], batch_size: int = 200):
        """Distribute unlimited tracks across unlimited workers asynchronously."""
        if not tracks or not worker_ids:
            return
            
        # Create mega-batches for unlimited processing
        mega_batches = [tracks[i:i + batch_size] for i in range(0, len(tracks), batch_size)]
        
        # Distribute mega-batches across unlimited workers
        tasks = []
        for i, batch in enumerate(mega_batches):
            worker_id = worker_ids[i % len(worker_ids)]
            
            # Create async task for unlimited processing
            task = asyncio.create_task(
                self._process_mega_batch_async(batch, worker_id, f"batch_{i}")
            )
            tasks.append(task)
            self.batch_processors[f"batch_{i}"] = task
        
        logger.info(f"Distributed {len(mega_batches)} mega-batches across {len(worker_ids)} unlimited workers")
        
        # Process all batches asynchronously for unlimited throughput
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in unlimited batch processing: {e}")
        
        # Update final stats
        await self._update_processing_stats()
        
    async def _process_mega_batch_async(self, batch: List[Dict[str, Any]], 
                                      worker_id: int, batch_id: str):
        """Process mega-batch asynchronously for unlimited workers."""
        async with self.semaphore:
            try:
                # Process tracks in parallel within the batch
                sub_tasks = []
                for track_data in batch:
                    if not self.shutdown_event.is_set():
                        task = asyncio.create_task(
                            self._process_single_track_unlimited(track_data, worker_id)
                        )
                        sub_tasks.append(task)
                
                # Execute all sub-tasks concurrently
                if sub_tasks:
                    results = await asyncio.gather(*sub_tasks, return_exceptions=True)
                    
                    # Count successful processing
                    successful = sum(1 for r in results if r is True)
                    failed = len(results) - successful
                    
                    self.processing_stats.processed_tracks += successful
                    self.processing_stats.failed_tracks += failed
                    
                    logger.info(f"Batch {batch_id} completed: {successful} successful, {failed} failed")
                    
            except Exception as e:
                logger.error(f"Error processing mega-batch {batch_id}: {e}")
                self.processing_stats.failed_tracks += len(batch)
                
    async def _process_single_track_unlimited(self, track_data: Dict[str, Any], worker_id: int) -> bool:
        """Process single track with unlimited worker capability."""
        try:
            # Validate using pyroutils
            dump_channel = track_data.get('dump_channel')
            if dump_channel and not pyroutils.is_channel_id(dump_channel):
                logger.warning(f"Invalid dump channel {dump_channel}: {pyroutils.format_peer_id(dump_channel)}")
                return False
            
            # Add to worker queue for unlimited processing
            if worker_id in self.worker_pools:
                await self.worker_pools[worker_id].put(track_data)
                return True
            else:
                logger.error(f"Worker {worker_id} not found in unlimited pools")
                return False
                
        except Exception as e:
            logger.error(f"Error processing track {track_data.get('track_id', 'unknown')}: {e}")
            return False
    
    async def _update_processing_stats(self):
        """Update processing statistics for unlimited processing."""
        if self.processing_stats.start_time:
            elapsed_time = time.time() - self.processing_stats.start_time
            if elapsed_time > 0:
                self.processing_stats.tracks_per_second = self.processing_stats.processed_tracks / elapsed_time
        
        logger.info(
            f"Unlimited Processing Stats: "
            f"{self.processing_stats.processed_tracks:,}/{self.processing_stats.total_tracks:,} processed, "
            f"{self.processing_stats.failed_tracks:,} failed, "
            f"{self.processing_stats.tracks_per_second:.2f} tracks/sec, "
            f"{self.processing_stats.active_workers} workers"
        )
    
    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get current unlimited processing statistics."""
        await self._update_processing_stats()
        return {
            'total_tracks': self.processing_stats.total_tracks,
            'processed_tracks': self.processing_stats.processed_tracks,
            'failed_tracks': self.processing_stats.failed_tracks,
            'remaining_tracks': self.processing_stats.total_tracks - self.processing_stats.processed_tracks - self.processing_stats.failed_tracks,
            'active_workers': self.processing_stats.active_workers,
            'tracks_per_second': self.processing_stats.tracks_per_second,
            'progress_percentage': (self.processing_stats.processed_tracks / max(self.processing_stats.total_tracks, 1)) * 100,
            'active_batches': len(self.batch_processors)
        }
    
    async def shutdown_unlimited_processing(self):
        """Shutdown unlimited processing gracefully."""
        self.shutdown_event.set()
        
        # Cancel all batch processors
        for batch_id, task in self.batch_processors.items():
            if not task.done():
                task.cancel()
                logger.info(f"Cancelled batch processor {batch_id}")
        
        # Clear worker pools
        self.worker_pools.clear()
        self.batch_processors.clear()
        
        logger.info("Unlimited processing shutdown completed")