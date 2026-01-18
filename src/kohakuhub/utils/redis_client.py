"""Redis client utility for Kohaku Hub."""

import redis.asyncio as redis
from kohakuhub.config import cfg
from kohakuhub.logger import get_logger

logger = get_logger("REDIS")

_redis_client = None


def get_redis_client():
    """Get or create the global Redis client.
    
    Returns:
        redis.Redis or None: The Redis client if enabled, else None.
    """
    global _redis_client
    
    if not cfg.redis.enabled:
        return None
        
    if _redis_client is None:
        try:
            _redis_client = redis.Redis(
                host=cfg.redis.host,
                port=cfg.redis.port,
                db=cfg.redis.db,
                password=cfg.redis.password,
                decode_responses=False, # We want bytes for blocks
            )
            logger.info(f"Redis client initialized: {cfg.redis.host}:{cfg.redis.port}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")
            _redis_client = None
            
    return _redis_client
