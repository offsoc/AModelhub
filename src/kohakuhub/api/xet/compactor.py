"""Xet background compaction worker."""

import asyncio
import hashlib
import uuid
from typing import List

from kohakuhub.config import cfg
from kohakuhub.db import XetBlock, XetXorb, XetBlockPlacement, db
from kohakuhub.logger import get_logger
from kohakuhub.utils.s3 import get_s3_client, object_exists
from kohakuhub.utils.xet import get_xet_block_s3_key, get_xet_xorb_s3_key

logger = get_logger("XET_COMPACTOR")

# Target XORB size: 100MB
TARGET_XORB_SIZE = 100 * 1024 * 1024


async def compact_blocks():
    """Identifies and merges small unplaced blocks into XORBs."""
    # 1. Find blocks that don't have a placement (meaning they are individual files)
    # and have a known size > 0
    unplaced_blocks = (XetBlock.select()
                       .where(XetBlock.size > 0)
                       .where(~XetBlock.id.in_(XetBlockPlacement.select(XetBlockPlacement.block)))
                       .limit(1000))
    
    if not unplaced_blocks:
        logger.debug("No unplaced blocks found for compaction.")
        return

    current_batch: List[XetBlock] = []
    current_size = 0
    
    for block in unplaced_blocks:
        current_batch.append(block)
        current_size += block.size
        
        if current_size >= TARGET_XORB_SIZE:
            await _create_xorb_from_batch(current_batch)
            current_batch = []
            current_size = 0
            
    if current_batch:
        await _create_xorb_from_batch(current_batch)


async def _create_xorb_from_batch(blocks: List[XetBlock]):
    """Merges a batch of blocks into a single XORB on S3."""
    xorb_id = str(uuid.uuid4())
    logger.info(f"Creating Xorb {xorb_id} from {len(blocks)} blocks...")
    
    s3 = get_s3_client()
    xorb_content = bytearray()
    placements = []
    
    offset = 0
    for block in blocks:
        s3_key = get_xet_block_s3_key(block.hash)
        try:
            # Fetch individual block
            response = s3.get_object(Bucket=cfg.s3.bucket, Key=s3_key)
            data = response['Body'].read()
            
            xorb_content.extend(data)
            placements.append({
                "block": block,
                "offset": offset,
                "length": block.size
            })
            offset += block.size
        except Exception as e:
            logger.error(f"Failed to fetch block {block.hash} for compaction: {e}")
            continue

    if not xorb_content:
        return

    # Upload XORB
    xorb_s3_key = get_xet_xorb_s3_key(xorb_id)
    s3.put_object(
        Bucket=cfg.s3.bucket,
        Key=xorb_s3_key,
        Body=bytes(xorb_content),
        ContentType="application/octet-stream"
    )

    # Update Database
    with db.atomic():
        xorb = XetXorb.create(
            xorb_id=xorb_id,
            storage_key=xorb_s3_key,
            size=len(xorb_content)
        )
        
        for p in placements:
            XetBlockPlacement.create(
                block=p["block"],
                xorb=xorb,
                offset=p["offset"],
                length=p["length"]
            )
            
    logger.success(f"Compacted {len(blocks)} blocks into Xorb {xorb_id} ({len(xorb_content)} bytes)")
    
    # Optional: Cleanup individual blocks (in a real production app, we might wait 24h)
    # for block in blocks:
    #    s3.delete_object(Bucket=cfg.s3.bucket, Key=get_xet_block_s3_key(block.hash))
