"""Xet shard management and generation."""

import hashlib
import json
import struct
from kohakuhub.db import XetBlock, XetBlockPlacement, XetShard, db
from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.utils.s3 import get_s3_client

logger = get_logger("XET_SHARD_MGR")

def get_xet_shard_s3_key(shard_id: str) -> str:
    """Generate S3 key for a Xet shard."""
    return f"cas/shards/{shard_id[:2]}/{shard_id[2:4]}/{shard_id}"

async def generate_global_shard():
    """Generates a global shard for all un-sharded blocks.
    
    A Xet shard is conceptually a mapping: block_hash -> (xorb_id, offset, length)
    """
    logger.info("Starting global shard generation...")
    
    # For this implementation, we'll bundle all current placements into a single shard
    # in a simple binary format: [magic][version][entry_count][(hash:32 bytes)(xorb_id:32 bytes)(offset:8 bytes)(length:8 bytes)...]
    
    placements = (XetBlockPlacement.select(XetBlockPlacement, XetBlock, XetXorb)
                  .join(XetBlock)
                  .switch(XetBlockPlacement)
                  .join(XetXorb))
    
    if placements.count() == 0:
        logger.debug("No placements found for shard generation.")
        return None

    # Binary Format:
    # Magic: 'XSHD' (4 bytes)
    # Version: 1 (4 bytes)
    # Entry Count: N (4 bytes)
    # Entry: [Hash (32b)][XorbID (32b)][Offset (8b)][Length (8b)]
    
    header = struct.pack(">4sII", b"XSHD", 1, placements.count())
    entries = []
    
    for p in placements:
        # Convert hex hashes to bytes
        try:
            block_hash_bytes = bytes.fromhex(p.block.hash)
            xorb_id_bytes = bytes.fromhex(p.xorb.xorb_id)
            entries.append(struct.pack(">32s32sQQ", block_hash_bytes, xorb_id_bytes, p.offset, p.length))
        except Exception as e:
            logger.error(f"Failed to process placement for block {p.block.hash[:8]}: {e}")
            continue

    shard_content = header + b"".join(entries)
    shard_id = hashlib.sha256(shard_content).hexdigest()
    
    # Upload to S3
    s3_key = get_xet_shard_s3_key(shard_id)
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=cfg.s3.bucket,
            Key=s3_key,
            Body=shard_content,
            ContentType="application/octet-stream"
        )
        
        # Register in DB
        XetShard.get_or_create(
            shard_id=shard_id,
            defaults={"storage_key": s3_key, "size": len(shard_content)}
        )
        
        logger.success(f"Generated shard {shard_id[:8]} with {len(entries)} entries.")
        return shard_id
    except Exception as e:
        logger.error(f"Failed to upload shard {shard_id}: {e}")
        return None

async def compact_shards():
    """Merges multiple small shards into larger ones."""
    logger.info("Starting shard compaction...")
    
    # Simple strategy: Find all shards smaller than 1MB and merge them
    small_shards = list(XetShard.select().where(XetShard.size < 1024 * 1024))
    
    if len(small_shards) < 2:
        logger.debug("Not enough small shards to compact.")
        return
        
    logger.info(f"Found {len(small_shards)} small shards for compaction.")
    
    all_entries = []
    s3 = get_s3_client()
    
    for shard in small_shards:
        try:
            # Download shard content
            response = s3.get_object(Bucket=cfg.s3.bucket, Key=shard.storage_key)
            content = response['Body'].read()
            
            # Skip header (4 + 4 + 4 bytes)
            # Entry: [Hash (32b)][XorbID (32b)][Offset (8b)][Length (8b)]
            header_size = 12
            entries_content = content[header_size:]
            all_entries.append(entries_content)
        except Exception as e:
            logger.error(f"Failed to read shard {shard.shard_id[:8]} for compaction: {e}")
            continue
            
    if not all_entries:
        return
        
    # Create new combined shard
    combined_entries_bytes = b"".join(all_entries)
    entry_count = len(combined_entries_bytes) // 80 # 32+32+8+8 = 80
    
    header = struct.pack(">4sII", b"XSHD", 1, entry_count)
    new_shard_content = header + combined_entries_bytes
    new_shard_id = hashlib.sha256(new_shard_content).hexdigest()
    
    # Upload new shard
    s3_key = get_xet_shard_s3_key(new_shard_id)
    try:
        s3.put_object(
            Bucket=cfg.s3.bucket,
            Key=s3_key,
            Body=new_shard_content,
            ContentType="application/octet-stream"
        )
        
        with db.atomic():
            # Register new shard
            XetShard.create(
                shard_id=new_shard_id,
                storage_key=s3_key,
                size=len(new_shard_content)
            )
            
            # Delete old shards from DB and S3
            for shard in small_shards:
                try:
                    s3.delete_object(Bucket=cfg.s3.bucket, Key=shard.storage_key)
                except Exception as e:
                    logger.warning(f"Failed to delete old shard {shard.shard_id[:8]} from S3: {e}")
                shard.delete_instance()
                
        logger.success(f"Compacted {len(small_shards)} shards into new shard {new_shard_id[:8]}")
    except Exception as e:
        logger.error(f"Shard compaction failed: {e}")
