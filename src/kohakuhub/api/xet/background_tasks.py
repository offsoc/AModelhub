"""Xet background tasks."""

import asyncio
from kohakuhub.db import File, XetFileLayout
from kohakuhub.api.xet.chunker import chunk_lfs_file
from kohakuhub.logger import get_logger
from kohakuhub.config import cfg
from kohakuhub.api.xet.shard_manager import generate_global_shard, compact_shards
from kohakuhub.utils.disk_cache import cleanup_cache
from kohakuhub.async_utils import run_in_s3_executor

logger = get_logger("XET_TASKS")


async def xet_background_worker():
    """Background worker to process Xet-related tasks."""
    logger.info("Xet background worker started.")
    while True:
        try:
            # 1. Process LFS files that need chunking
            # Find files that are LFS but don't have Xet layouts
            files_to_chunk = (File.select()
                              .where(File.lfs == True)
                              .where(File.is_deleted == False)
                              .where(~File.id.in_(XetFileLayout.select(XetFileLayout.file)))
                              .limit(10))
            
            for file_record in files_to_chunk:
                await chunk_lfs_file(file_record)
                
            # 2. Shard Generation (Periodic)
            # We'll use a simple counter or timestamp check
            # For simplicity, let's just run it every 10 loops (approx 10 mins)
            # In production, we'd use the config interval
            if not hasattr(xet_background_worker, "loop_count"):
                xet_background_worker.loop_count = 0
            
            xet_background_worker.loop_count += 1
            if xet_background_worker.loop_count % 10 == 0:
                await generate_global_shard()
                await compact_shards()
                await run_in_s3_executor(cleanup_cache)
                
            # Sleep before next scan
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Xet background worker error: {e}")
            await asyncio.sleep(60)
