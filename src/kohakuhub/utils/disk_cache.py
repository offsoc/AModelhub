"""Local disk cache for CAS blocks."""

import os
import shutil
from pathlib import Path
from kohakuhub.config import cfg
from kohakuhub.logger import get_logger

logger = get_logger("DISK_CACHE")

def _get_cache_path(block_hash: str) -> Path:
    """Get the local path for a cached block."""
    cache_dir = Path(cfg.xet.cas_cache_dir)
    return cache_dir / block_hash[:2] / block_hash[2:4] / block_hash

def get_from_cache(block_hash: str) -> bytes | None:
    """Retrieve block content from local disk cache."""
    path = _get_cache_path(block_hash)
    if path.exists():
        try:
            return path.read_bytes()
        except Exception as e:
            logger.warning(f"Failed to read from cache {block_hash[:8]}: {e}")
            return None
    return None

def save_to_cache(block_hash: str, content: bytes):
    """Save block content to local disk cache."""
    path = _get_cache_path(block_hash)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        
        # Periodic cleanup check (simple version)
        # In a real system, we'd use a background worker for this
    except Exception as e:
        logger.warning(f"Failed to save to cache {block_hash[:8]}: {e}")

def cleanup_cache():
    """Simple cache cleanup if size exceeds limit."""
    cache_dir = Path(cfg.xet.cas_cache_dir)
    if not cache_dir.exists():
        return
        
    # Get total size in bytes
    total_size = sum(f.stat().st_size for f in cache_dir.glob('**/*') if f.is_file())
    max_size = cfg.xet.cas_cache_max_size_gb * 1024 * 1024 * 1024
    
    if total_size > max_size:
        logger.info(f"Cache cleanup triggered: {total_size / (1024**3):.2f}GB / {cfg.xet.cas_cache_max_size_gb}GB")
        
        # Sort files by modification time (oldest first)
        files = sorted(
            (f for f in cache_dir.glob('**/*') if f.is_file()),
            key=lambda f: f.stat().st_mtime
        )
        
        # Delete until we are under 80% of max size
        target_size = max_size * 0.8
        bytes_to_delete = total_size - target_size
        deleted_count = 0
        
        for f in files:
            if bytes_to_delete <= 0:
                break
            size = f.stat().st_size
            f.unlink()
            bytes_to_delete -= size
            deleted_count += 1
            
        logger.info(f"Deleted {deleted_count} files from cache.")
