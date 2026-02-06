"""Git push handler for AModelhub (FastAPI + LakeFS).

This module provides a dedicated endpoint to receive Git pushes, parse the pack data,
and commit the changes to LakeFS.
"""

import base64
import hashlib
import json
from typing import Dict, List, Tuple, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from kohakuhub.config import cfg
from kohakuhub.db import User, Repository, File
from kohakuhub.db_operations import get_repository, create_file, track_lfs_object
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.utils.lakefs import get_lakefs_client, lakefs_repo_name
from kohakuhub.api.git.utils.pack import GitPackParser
from kohakuhub.logger import get_logger

logger = get_logger("GIT_PUSH")
router = APIRouter()

def is_lfs_pointer(content: bytes) -> bool:
    """Check if blob content is a Git LFS pointer."""
    return content.startswith(b"version https://git-lfs.github.com/spec/v1")

def parse_lfs_pointer(content: bytes) -> Tuple[str, int]:
    """Parse OID and size from LFS pointer."""
    lines = content.decode("utf-8").splitlines()
    oid = ""
    size = 0
    for line in lines:
        if line.startswith("oid sha256:"):
            oid = line.replace("oid sha256:", "").strip()
        elif line.startswith("size "):
            size = int(line.replace("size ", "").strip())
    return oid, size

@router.post("/git/{namespace}/{name}/push")
async def git_push_endpoint(
    namespace: str,
    name: str,
    request: Request,
    user: User = Depends(get_current_user)
):
    """Bridge Git push to LakeFS commit API.
    
    Receives a Git pack file, extracts blobs, handles LFS pointers,
    and creates a LakeFS commit.
    """
    repo_id = f"{namespace}/{name}"
    repo_row = get_repository("model", namespace, name) # Default to model
    if not repo_row:
        raise HTTPException(404, detail="Repository not found")

    # Read pack data
    pack_data = await request.body()
    if not pack_data:
        raise HTTPException(400, detail="Empty pack data")

    try:
        # 1. Parse pack
        parser = GitPackParser(pack_data)
        objects = parser.parse()
        
        lakefs_repo = lakefs_repo_name(repo_row.repo_type, repo_id)
        client = get_lakefs_client()
        
        # 2. Extract and process blobs
        blobs_processed = 0
        lfs_pointers = 0
        
        # NOTE: A real push contains ref updates (old/new SHA).
        # For simplicity in this endpoint, we'll process all blobs in the pack.
        for sha1, (obj_type, content) in objects.items():
            if obj_type == 3: # Blob
                if is_lfs_pointer(content):
                    # Handle LFS
                    oid, size = parse_lfs_pointer(content)
                    logger.info(f"LFS pointer detected: {oid[:8]}, size: {size}")
                    lfs_pointers += 1
                    # In a real flow, the LFS file should already be in S3 (preupload)
                    # We just need to ensure it's tracked.
                    # TODO: Link path to pointer. In a pure git push, we'd need 
                    # tree data to know the path.
                else:
                    # Regular blob
                    blobs_processed += 1
                    # For a full implementation, we'd traverse trees to get paths.
                    # Here we assume a flat structure or specific metadata.
        
        # 3. Create LakeFS commit
        # Bridging Git push to LakeFS commit API
        await client.commit(
            repository=lakefs_repo,
            branch="main",
            message=f"Git push via API: {len(objects)} objects",
            metadata={"author": user.username, "git_push": "true"}
        )
        
        return {
            "status": "success",
            "objects_received": len(objects),
            "blobs": blobs_processed,
            "lfs_pointers": lfs_pointers,
            "repository": repo_id
        }

    except Exception as e:
        logger.exception(f"Git push failed for {repo_id}", e)
        raise HTTPException(500, detail=f"Push failed: {str(e)}")
