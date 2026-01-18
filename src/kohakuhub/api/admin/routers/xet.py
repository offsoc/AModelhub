"""Xet administration and metrics endpoints for admin API."""

from fastapi import APIRouter, Depends
from peewee import fn

from kohakuhub.db import XetBlock, XetXorb, XetShard, XetFileLayout, File, Repository
from kohakuhub.logger import get_logger
from kohakuhub.api.admin.utils import verify_admin_token

logger = get_logger("ADMIN_XET")
router = APIRouter()


@router.get("/stats")
async def get_xet_stats(
    _admin: bool = Depends(verify_admin_token),
):
    """Get high-level Xet storage statistics."""
    
    total_blocks = XetBlock.select().count()
    total_logical_size = XetBlock.select(fn.SUM(XetBlock.size)).scalar() or 0
    
    total_xorbs = XetXorb.select().count()
    total_physical_size = XetXorb.select(fn.SUM(XetXorb.size)).scalar() or 0
    
    total_shards = XetShard.select().count()
    shard_size = XetShard.select(fn.SUM(XetShard.size)).scalar() or 0
    
    # Deduplication Ratio
    # If physical size is 0 but we have blocks, logical size might be greater than 0
    # ratio = logical / physical
    dedupe_ratio = 1.0
    if total_physical_size > 0:
        dedupe_ratio = total_logical_size / total_physical_size
    elif total_logical_size > 0:
        # Blocks exist but haven't been compacted into xorbs yet
        dedupe_ratio = 1.0 # Or potentially higher if we count duplicates in S3, but logically 1:1 for now
        
    return {
        "blocks": {
            "count": total_blocks,
            "logical_size_bytes": total_logical_size,
        },
        "xorbs": {
            "count": total_xorbs,
            "physical_size_bytes": total_physical_size,
        },
        "shards": {
            "count": total_shards,
            "size_bytes": shard_size,
        },
        "metrics": {
            "deduplication_ratio": round(dedupe_ratio, 2),
            "savings_bytes": max(0, total_logical_size - total_physical_size),
        }
    }


@router.get("/metrics/top-repos")
async def get_top_xet_repos(
    limit: int = 10,
    _admin: bool = Depends(verify_admin_token),
):
    """Get repositories with highest Xet usage."""
    
    # Count unique blocks per repository
    top_repos = (
        File.select(File.repository, fn.COUNT(XetBlock.id).alias("block_count"), fn.SUM(XetBlock.size).alias("logical_size"))
        .join(XetFileLayout, on=(File.id == XetFileLayout.file))
        .join(XetBlock, on=(XetFileLayout.block == XetBlock.id))
        .group_by(File.repository)
        .order_by(fn.SUM(XetBlock.size).desc())
        .limit(limit)
    )
    
    results = []
    for item in top_repos:
        repo = item.repository
        results.append({
            "repo_full_id": repo.full_id if repo else "unknown",
            "repo_type": repo.repo_type if repo else "unknown",
            "block_count": item.block_count,
            "logical_size_bytes": item.logical_size,
        })
        
    return results


@router.get("/metrics/distribution")
async def get_block_distribution(
    _admin: bool = Depends(verify_admin_token),
):
    """Get block size distribution."""
    
    # Simple buckets: <1MB, 1-4MB, 4-8MB, 8MB+
    ranges = [
        ("under_1mb", (0, 1024 * 1024)),
        ("1mb_4mb", (1024 * 1024, 4 * 1024 * 1024)),
        ("4mb_8mb", (4 * 1024 * 1024, 8 * 1024 * 1024)),
        ("over_8mb", (8 * 1024 * 1024, 1024 * 1024 * 1024 * 1024)),
    ]
    
    distribution = {}
    for name, (low, high) in ranges:
        count = XetBlock.select().where((XetBlock.size >= low) & (XetBlock.size < high)).count()
        distribution[name] = count
        
    return distribution
