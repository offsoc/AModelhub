"""
Xet Shard (metadata) router.

Handles storage and retrieval of Xet shards for git-xet and hf-xet.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from kohakuhub.config import cfg
from kohakuhub.db import XetShard
from kohakuhub.logger import get_logger
from kohakuhub.utils.s3 import (
    generate_download_presigned_url,
    get_s3_client,
    object_exists,
)

router = APIRouter()
logger = get_logger("XET_SHARD")


def get_xet_shard_s3_key(shard_id: str) -> str:
    """Generate S3 key for a Xet shard."""
    return f"cas/shards/{shard_id[:2]}/{shard_id[2:4]}/{shard_id}"


@router.head("/shards/{shard_id}")
async def head_shard(shard_id: str):
    """Check if a shard exists."""
    # Check DB
    if XetShard.select().where(XetShard.shard_id == shard_id).exists():
        return Response(status_code=200)

    # Check S3 fallback
    s3_key = get_xet_shard_s3_key(shard_id)
    if await object_exists(cfg.s3.bucket, s3_key):
        XetShard.get_or_create(shard_id=shard_id, defaults={"storage_key": s3_key, "size": 0})
        return Response(status_code=200)

    raise HTTPException(status_code=404)


@router.get("/shards/{shard_id}")
async def get_shard(shard_id: str):
    """Download a shard."""
    shard = XetShard.get_or_none(shard_id=shard_id)
    if not shard:
        # Final S3 check
        s3_key = get_xet_shard_s3_key(shard_id)
        if not await object_exists(cfg.s3.bucket, s3_key):
            raise HTTPException(status_code=404)
        storage_key = s3_key
    else:
        storage_key = shard.storage_key

    url = await generate_download_presigned_url(cfg.s3.bucket, storage_key)
    return Response(status_code=307, headers={"Location": url})


@router.put("/shards/{shard_id}")
async def put_shard(shard_id: str, request: Request):
    """Upload a shard."""
    content = await request.body()
    size = len(content)

    # Upload to S3
    s3_key = get_xet_shard_s3_key(shard_id)
    s3 = get_s3_client()
    s3.put_object(
        Bucket=cfg.s3.bucket,
        Key=s3_key,
        Body=content,
        ContentType="application/octet-stream"
    )

    # Save to DB
    XetShard.get_or_create(
        shard_id=shard_id,
        defaults={"storage_key": s3_key, "size": size}
    )

    return {"message": "Shard uploaded", "shard_id": shard_id, "size": size}
