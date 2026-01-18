"""
CAS (Content Addressable Storage) reconstruction endpoint.

Returns xorb reconstruction information for hf-xet client.
"""

import asyncio
import hashlib
import json

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from kohakuhub.api.xet.metrics import metrics
from kohakuhub.config import cfg
from kohakuhub.db import File, User, XetBlock, XetFileLayout, XetBlockPlacement, XetXorb
from kohakuhub.logger import get_logger
from kohakuhub.utils.lakefs import get_lakefs_client, lakefs_repo_name
from kohakuhub.utils.s3 import (
    generate_download_presigned_url,
    get_object_metadata,
    get_s3_client,
    object_exists,
    parse_s3_uri,
)
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.async_utils import run_in_s3_executor
from kohakuhub.utils.disk_cache import get_from_cache as get_from_disk, save_to_cache as save_to_disk
from kohakuhub.api.xet.utils.file_lookup import (
    check_file_read_permission,
    lookup_file_by_sha256,
)
from kohakuhub.utils.xet import (
    cache_block,
    check_block_exists_bloom,
    check_block_exists_fast,
    get_cached_block,
    get_xet_block_s3_key,
    get_xet_xorb_s3_key,
    mark_block_as_existing,
    mark_block_in_bloom,
)

router = APIRouter()
logger = get_logger("XET_CAS")

# Maximum chunk size: 64MB
# Note: Must be less than u32::MAX (4GB) for Rust client compatibility
CHUNK_SIZE_BYTES = 64 * 1024 * 1024  # 64 MB


@router.head("/blocks/{block_hash}")
async def head_block(block_hash: str):
    """Check if a block exists in CAS."""
    # 1. Ultra-fast check (Bloom Filter)
    if await check_block_exists_bloom(block_hash):
        # We still double check with fast set or DB to avoid false positives in critical paths
        # but for HEAD it might be acceptable to return 200 if we are okay with rare duplicate uploads.
        # hf-xet is robust.
        return Response(status_code=200)

    # 2. Fast check (Redis Set)
    if await check_block_exists_fast(block_hash):
        return Response(status_code=200)

    # 2. Database check
    block = XetBlock.get_or_none(XetBlock.hash == block_hash)
    if block:
        await mark_block_as_existing(block_hash)
        metrics.record_dedup(hit=True, size=block.size)
        return Response(status_code=200)

    # 3. Storage check (Final fallback)
    s3_key = get_xet_block_s3_key(block_hash)
    if await object_exists(cfg.s3.bucket, s3_key):
        # Register in DB and Redis for future
        block, _ = XetBlock.get_or_create(hash=block_hash, defaults={"size": 0})
        await mark_block_as_existing(block_hash)
        metrics.record_dedup(hit=True, size=block.size)
        return Response(status_code=200)

    raise HTTPException(status_code=404)


@router.get("/blocks/{block_hash}")
async def get_block(block_hash: str):
    """Download a block from CAS."""
    # 1. Cache hit?
    cached = await get_cached_block(block_hash)
    if cached:
        return Response(content=cached, media_type="application/octet-stream")

    # 2. Local Disk Cache (SSD/LVMe)
    content = await run_in_s3_executor(get_from_disk, block_hash)
    if content:
        logger.debug(f"Block {block_hash[:8]} hit in disk cache")
        metrics.record_dedup(hit=True, size=len(content)) # It's a hit for the user
        return Response(content=content, media_type="application/octet-stream")

    # 3. S3 Fallback
    s3_key = get_xet_block_s3_key(block_hash)
    if not await object_exists(cfg.s3.bucket, s3_key):
        raise HTTPException(status_code=404)

    url = await generate_download_presigned_url(cfg.s3.bucket, s3_key)
    
    # We could download and cache here, but 307 is faster if client supports it.
    # hf-xet usually follows redirects.
    # However, if we want TRUE local cache benefits, we should proxy the download.
    # For now, let's stick to 307 but we'll download and cache in background if desired.
    
    return Response(
        status_code=307, headers={"Location": url}
    )


@router.put("/blocks/{block_hash}")
async def put_block(block_hash: str, request: Request):
    """Upload a block to CAS with integrity verification."""
    content = await request.body()
    size = len(content)

    # Verify integrity
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != block_hash:
        raise HTTPException(status_code=400, detail="Hash mismatch")

    # Upload to S3
    s3_key = get_xet_block_s3_key(block_hash)
    s3 = get_s3_client()
    s3.put_object(
        Bucket=cfg.s3.bucket,
        Key=s3_key,
        Body=content,
        ContentType="application/octet-stream"
    )

    # Save to DB
    XetBlock.get_or_create(
        hash=block_hash,
        defaults={"size": size}
    )

    # Record metric (this was a miss because it reached PUT)
    metrics.record_dedup(hit=False, size=size)
    if metrics.dedup_misses % 100 == 0:
        metrics.log_stats()

    # Disk cache
    await run_in_s3_executor(save_to_disk, block_hash, content)

    # Hot cache (Redis)
    await cache_block(block_hash, content)
    await mark_block_as_existing(block_hash)
    await mark_block_in_bloom(block_hash)

    return {"message": "Block uploaded", "hash": block_hash, "size": size}


@router.get("/reconstructions/{file_id}")
async def get_reconstruction(
    file_id: str,
    user: User | None = Depends(get_optional_user),
):
    """
    Get CAS reconstruction information for a file.

    Args:
        file_id: SHA256 hash of the file

    Returns:
        JSON with single xorb (no deduplication):
        {
          "xorbs": [{
            "url": "s3_presigned_url",
            "size": file_size,
            "sha256": file_id
          }],
          "reconstruction": [{
            "xorb_index": 0,
            "offset": 0,
            "length": file_size
          }]
        }
    """
    # Lookup file by SHA256
    repo, file_record = lookup_file_by_sha256(file_id)

    # Check read permission
    check_file_read_permission(repo, user)

    # Get LakeFS repository name
    lakefs_repo = lakefs_repo_name(repo.repo_type, repo.full_id)

    # Get file from LakeFS (use main branch to get latest physical address)
    client = get_lakefs_client()

    try:
        # Use path_in_repo to get the file from LakeFS
        obj_stat = await client.stat_object(
            repository=lakefs_repo,
            ref="main",  # Use main branch as default
            path=file_record.path_in_repo,
        )
    except Exception as e:
        # If file not found on main, it might be on a different branch
        # For now, return 404 - in future we could search all branches
        raise HTTPException(
            status_code=404, detail={"error": f"File not found in repository: {e}"}
        )

    # Parse S3 physical address
    physical_address = obj_stat["physical_address"]
    bucket, key = parse_s3_uri(physical_address)

    # Generate presigned S3 URL (7 days expiration)
    presigned_url = await generate_download_presigned_url(
        bucket=bucket,
        key=key,
        expires_in=604800,  # 7 days
        filename=file_record.path_in_repo.split("/")[-1],
    )

    # Get file size
    file_size = file_record.size
    file_hash = file_record.sha256

    logger.info(
        f"Generating reconstruction for file {file_hash} (size: {file_size} bytes)"
    )

    # 1. Check for real XetFileLayout
    layout = list(XetFileLayout.select(XetFileLayout, XetBlock)
                  .join(XetBlock)
                  .where(XetFileLayout.file == file_record)
                  .order_by(XetFileLayout.sequence_order))

    if layout:
        logger.info(f"Using real XetFileLayout for {file_hash} ({len(layout)} blocks)")
        result = await _generate_real_reconstruction(layout)
    else:
        # 2. Fallback to virtual chunking
        logger.info(f"Falling back to virtual reconstruction for {file_hash}")
        result = _generate_chunked_reconstruction(file_hash, file_size, presigned_url)

    logger.debug(f"Generated {len(result['terms'])} terms for file {file_hash}")

    return Response(
        content=json.dumps(result, ensure_ascii=False), media_type="application/json"
    )


def _generate_chunked_reconstruction(
    file_id: str, file_size: int, presigned_url: str
) -> dict:
    """
    Generate QueryReconstructionResponse with chunking support.

    Splits large files into 64MB chunks to avoid u32 overflow issues in the Rust client.
    Each chunk uses the same S3 presigned URL with different byte ranges.

    Args:
        file_id: SHA256 hash of the file
        file_size: Total file size in bytes
        presigned_url: S3 presigned URL for the file

    Returns:
        Dictionary with terms and fetch_info for reconstruction
    """
    # Calculate number of chunks needed
    if file_size == 0:
        # Empty file - single chunk
        num_chunks = 1
        chunks = [(0, 0)]
    else:
        num_chunks = (file_size + CHUNK_SIZE_BYTES - 1) // CHUNK_SIZE_BYTES
        chunks = []
        for i in range(num_chunks):
            start = i * CHUNK_SIZE_BYTES
            end = min(start + CHUNK_SIZE_BYTES, file_size)
            chunks.append((start, end))

    logger.debug(
        f"Splitting {file_size} bytes into {num_chunks} chunks of {CHUNK_SIZE_BYTES} bytes"
    )

    # Build terms and fetch_info
    terms = []
    fetch_info = {}

    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_size = chunk_end - chunk_start

        # Generate valid 64-char hex merkle hash for this chunk
        # Use deterministic hash based on file_id + chunk index for consistency
        if num_chunks == 1:
            # Single chunk: use original file hash
            chunk_hash = file_id
        else:
            # Multiple chunks: generate deterministic hash per chunk
            # Hash format: sha256(file_id + chunk_index)
            chunk_data = f"{file_id}-chunk{chunk_idx}".encode("utf-8")
            chunk_hash = hashlib.sha256(chunk_data).hexdigest()

        # Add term for this chunk
        terms.append(
            {
                "hash": chunk_hash,
                "unpacked_length": chunk_size,
                "range": {"start": chunk_idx, "end": chunk_idx + 1},
            }
        )

        # Add fetch_info for this chunk
        # Client will make HTTP Range request to same presigned URL
        fetch_info[chunk_hash] = [
            {
                "range": {"start": chunk_idx, "end": chunk_idx + 1},
                "url": presigned_url,
                "url_range": {
                    "start": chunk_start,
                    "end": (
                        chunk_end - 1 if chunk_size > 0 else 0
                    ),  # HTTP Range is inclusive
                },
            }
        ]

    return {"offset_into_first_range": 0, "terms": terms, "fetch_info": fetch_info}


async def _generate_real_reconstruction(layout: list[XetFileLayout]) -> dict:
    """Generate QueryReconstructionResponse from real XetFileLayout.
    
    Args:
        layout: List of XetFileLayout records.
        
    Returns:
        Dictionary with terms and fetch_info.
    """
    terms = []
    fetch_info = {}

    # Parallel URL generation
    async def get_term_and_info(idx, item):
        block = item.block
        block_hash = block.hash
        
        # Check if block is in a XORB
        placement = XetBlockPlacement.get_or_none(XetBlockPlacement.block == block)
        
        if placement:
            xorb = placement.xorb
            s3_key = xorb.storage_key
            # Generate range URL
            url = await generate_download_presigned_url(
                cfg.s3.bucket, 
                s3_key,
                range_header=f"bytes={placement.offset}-{placement.offset + placement.length - 1}"
            )
            logger.debug(f"Block {block_hash[:8]} found in Xorb {xorb.xorb_id} at offset {placement.offset}")
        else:
            # Individual block
            s3_key = get_xet_block_s3_key(block_hash)
            url = await generate_download_presigned_url(cfg.s3.bucket, s3_key)
        
        term = {
            "hash": block_hash,
            "unpacked_length": block.size,
            "range": {"start": idx, "end": idx + 1}
        }
        
        info = {
            "range": {"start": idx, "end": idx + 1},
            "url": url
        }
        return term, block_hash, info

    tasks = [get_term_and_info(idx, item) for idx, item in enumerate(layout)]
    results = await asyncio.gather(*tasks)

    for term, block_hash, info in results:
        terms.append(term)
        fetch_info[block_hash] = [info]

    return {"offset_into_first_range": 0, "terms": terms, "fetch_info": fetch_info}
