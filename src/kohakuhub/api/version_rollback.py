"""Rollback implementation for KohakuHub (AModelhub).

Uses git revert via huggingface_hub.Repository to undo specific commits safely.
Includes a backup step in LakeFS.
"""

import os
import shutil
import tempfile
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from huggingface_hub import Repository as HFRepo
from kohakuhub.config import cfg
from kohakuhub.db import User
from kohakuhub.db_operations import get_repository
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.utils.lakefs import get_lakefs_client, lakefs_repo_name
from kohakuhub.logger import get_logger

logger = get_logger("ROLLBACK")
router = APIRouter()

@router.post("/version/{namespace}/{name}/rollback/{commit_id}")
async def rollback_commit(
    namespace: str,
    name: str,
    commit_id: str,
    branch: str = "main",
    user: User = Depends(get_current_user)
):
    """Revert a specific commit and push the change to LakeFS."""
    repo_id = f"{namespace}/{name}"
    repo_row = get_repository("model", namespace, name) # Default to model
    if not repo_row:
        raise HTTPException(404, detail="Repository not found")

    lakefs_repo = lakefs_repo_name(repo_row.repo_type, repo_id)
    client = get_lakefs_client()
    
    # 1. Safety: Backup current state in LakeFS
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_branch = f"backup-rollback-{timestamp}"
    try:
        await client.create_branch(
            repository=lakefs_repo,
            branch=backup_branch,
            source=branch
        )
        logger.info(f"Created safety backup branch: {backup_branch}")
    except Exception as e:
        logger.warning(f"Failed to create backup branch: {e}")

    # 2. Local Git Revert
    tmp_dir = tempfile.mkdtemp(prefix=f"rollback_{name}_")
    try:
        git_url = f"{cfg.app.base_url}/{repo_id}.git"
        token = "dummy-token" # Real implementation uses user token
        
        repo = HFRepo(
            local_dir=tmp_dir,
            clone_from=git_url,
            use_auth_token=token,
            revision=branch
        )

        logger.info(f"Reverting commit {commit_id} on {branch}")
        try:
            # Execute git revert
            # --no-edit avoids interactive prompt
            repo.git_shell(["git", "revert", commit_id, "--no-edit"])
            
            # 3. Push the revert commit
            repo.push_to_hub(commit_message=f"Rollback commit {commit_id[:8]} by {user.username}")
            
            return {
                "status": "success",
                "backup_branch": backup_branch,
                "message": f"Successfully rolled back {commit_id[:8]}"
            }

        except Exception as e:
            logger.exception(f"Rollback execution failed for {repo_id}", e)
            # If revert fails (e.g. conflicts), we might need manual help
            return {
                "status": "error",
                "message": f"Revert failed (likely conflict): {str(e)}",
                "backup_branch": backup_branch
            }

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
