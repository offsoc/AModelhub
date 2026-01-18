"""Xet chunking logic."""

import hashlib
from typing import List, Tuple

from kohakuhub.config import cfg
from kohakuhub.db import File, XetBlock, XetFileLayout, db
from kohakuhub.logger import get_logger
from kohakuhub.utils.s3 import get_s3_client
from kohakuhub.utils.xet import get_xet_block_s3_key, mark_block_as_existing, mark_block_in_bloom

logger = get_logger("XET_CHUNKER")

# Simulated CDC target chunk size: 4MB
CHUNK_TARGET_SIZE = 4 * 1024 * 1024


async def chunk_lfs_file(file_record: File) -> bool:
    """Chunks an LFS file and creates XetFileLayout.
    
    This allows LFS files to be deduplicated and reconstructed via the Xet CAS hub.
    """
    if not file_record.lfs:
        return False

    # Check if already chunked
    if XetFileLayout.select().where(XetFileLayout.file == file_record).exists():
        logger.debug(f"File {file_record.path_in_repo} already chunked.")
        return True

    logger.info(f"Chunking LFS file: {file_record.path_in_repo} (oid: {file_record.sha256[:8]})")

    s3 = get_s3_client()
    lfs_key = f"lfs/{file_record.sha256[:2]}/{file_record.sha256[2:4]}/{file_record.sha256}"
    
    try:
        response = s3.get_object(Bucket=cfg.s3.bucket, Key=lfs_key)
        content = response['Body'].read()
    except Exception as e:
        logger.error(f"Failed to fetch LFS object {file_record.sha256} for chunking: {e}")
        return False

    # Split into chunks (simulated CDC - just fixed size for now in this demo impl)
    # In production, we'd use a real CDC algorithm or call a Rust sidecar.
    chunks = []
    for i in range(0, len(content), CHUNK_TARGET_SIZE):
        chunk_data = content[i:i+CHUNK_TARGET_SIZE]
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
        chunks.append((chunk_hash, chunk_data))

    # Register blocks and layout
    try:
        with db.atomic():
            for seq, (chash, cdata) in enumerate(chunks):
                # 1. Register block
                block, created = XetBlock.get_or_create(
                    hash=chash,
                    defaults={"size": len(cdata)}
                )
                
                # 2. Upload block if new
                s3_key = get_xet_block_s3_key(chash)
                # Note: In a real high-perf system, we'd check existence first
                # But here we just upload to ensure it's there.
                s3.put_object(
                    Bucket=cfg.s3.bucket,
                    Key=s3_key,
                    Body=cdata,
                    ContentType="application/octet-stream"
                )
                
                # 3. Create layout entry
                XetFileLayout.create(
                    file=file_record,
                    block=block,
                    sequence_order=seq
                )
                
                # 4. Update Redis
                await mark_block_as_existing(chash)
                await mark_block_in_bloom(chash)

        logger.success(f"Successfully chunked {file_record.path_in_repo} into {len(chunks)} blocks.")
        return True
    except Exception as e:
        logger.error(f"Failed to complete chunking for {file_record.path_in_repo}: {e}")
        return False
