"""Xet storage and caching utilities."""

import hashlib
from typing import Optional

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.utils.redis_client import get_redis_client

logger = get_logger("XET_UTILS")

# Redis key prefixes
XET_CACHE_PREFIX = "xet:block:"
XET_BLOCKS_SET = "xet:blocks:all"
XET_BLOOM_FILTER = "xet:bloom:blocks"


def get_xet_block_s3_key(block_hash: str) -> str:
    """Generate S3 key for a Xet block.
    
    Args:
        block_hash: SHA256 hex string of the block.
        
    Returns:
        S3 key string.
    """
    return f"cas/blocks/{block_hash[:2]}/{block_hash[2:4]}/{block_hash}"


def get_xet_xorb_s3_key(xorb_id: str) -> str:
    """Generate S3 key for a Xet xorb.
    
    Args:
        xorb_id: Unique xorb identifier.
        
    Returns:
        S3 key string.
    """
    return f"cas/xorbs/{xorb_id[:2]}/{xorb_id[2:4]}/{xorb_id}"


async def check_block_exists_fast(block_hash: str) -> bool:
    """Fast check for block existence using Redis.
    
    Falling back to False if Redis is disabled or fails.
    
    Args:
        block_hash: SHA256 hex string.
        
    Returns:
        True if the block is likely to exist.
    """
    redis = get_redis_client()
    if not redis:
        return False
        
    try:
        return await redis.sismember(XET_BLOCKS_SET, block_hash)
    except Exception as e:
        logger.warning(f"Redis sismember failed for {block_hash[:8]}: {e}")
        return False


async def mark_block_as_existing(block_hash: str):
    """Mark a block as existing in Redis for fast lookups.
    
    Args:
        block_hash: SHA256 hex string.
    """
    redis = get_redis_client()
    if not redis:
        return
        
    try:
        await redis.sadd(XET_BLOCKS_SET, block_hash)
    except Exception as e:
        logger.warning(f"Redis sadd failed for {block_hash[:8]}: {e}")


async def get_cached_block(block_hash: str) -> Optional[bytes]:
    """Retrieve block content from hot cache.
    
    Args:
        block_hash: SHA256 hex string.
        
    Returns:
        Block bytes or None if not cached.
    """
    redis = get_redis_client()
    if not redis:
        return None
        
    try:
        return await redis.get(XET_CACHE_PREFIX + block_hash)
    except Exception as e:
        logger.warning(f"Redis get failed for block {block_hash[:8]}: {e}")
        return None


async def cache_block(block_hash: str, content: bytes, ttl: int = 3600):
    """Cache a hot block in Redis.
    
    Args:
        block_hash: SHA256 hex string.
        content: Block data.
        ttl: Time-to-live in seconds.
    """
    redis = get_redis_client()
    if not redis:
        return
        
    try:
        await redis.setex(XET_CACHE_PREFIX + block_hash, ttl, content)
        # Also ensure it's in the membership set and bloom filter
        await mark_block_as_existing(block_hash)
    except Exception as e:
        logger.warning(f"Redis setex failed for block {block_hash[:8]}: {e}")


async def check_block_exists_bloom(block_hash: str) -> bool:
    """Check block existence using Bloom Filter.
    
    Falls back to normal set check if Bloom Filter is not initialized.
    
    Args:
        block_hash: SHA256 hex string.
        
    Returns:
        True if the block PROBABLY exists.
    """
    redis = get_redis_client()
    if not redis:
        return False
        
    try:
        # Check if RedisBloom module is available, otherwise use SET as fallback for now
        # Standard Redis doesn't have Bloom Filter unless module is loaded.
        # We'll try BF.EXISTS and handle error if not available.
        try:
            return await redis.execute_command("BF.EXISTS", XET_BLOOM_FILTER, block_hash)
        except Exception:
            # Fallback to standard set
            return await redis.sismember(XET_BLOCKS_SET, block_hash)
    except Exception as e:
        logger.warning(f"Bloom check failed for {block_hash[:8]}: {e}")
        return False


async def mark_block_in_bloom(block_hash: str):
    """Add block hash to Bloom Filter."""
    redis = get_redis_client()
    if not redis:
        return
        
    try:
        try:
            # Initialize Bloom Filter if it doesn't exist (64MB blocks, 0.01 error rate)
            # BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
            # We'll just try to add, if filter exists it works, otherwise we might need reserve
            await redis.execute_command("BF.ADD", XET_BLOOM_FILTER, block_hash)
        except Exception:
            # Fallback to standard set
            await redis.sadd(XET_BLOCKS_SET, block_hash)
    except Exception as e:
        logger.warning(f"Bloom add failed for {block_hash[:8]}: {e}")
