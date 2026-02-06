"""Version merge logic for KohakuHub (AModelhub).

Uses huggingface_hub.Repository to perform local git merges, 
detecting conflicts and bridging LakeFS branches.
"""

import os
import shutil
import tempfile
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from huggingface_hub import Repository as HFRepo
from kohakuhub.config import cfg
from kohakuhub.db import User
from kohakuhub.db_operations import get_repository
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.utils.lakefs import get_lakefs_client, lakefs_repo_name
from kohakuhub.logger import get_logger

logger = get_logger("VERSION_MERGE")
router = APIRouter()

@router.post("/version/{namespace}/{name}/merge/{source}/{target}")
async def merge_branches(
    namespace: str,
    name: str,
    source: str,
    target: str,
    user: User = Depends(get_current_user)
):
    """Merge source_branch into target_branch using local Git logic."""
    repo_id = f"{namespace}/{name}"
    repo_row = get_repository("model", namespace, name)
    if not repo_row:
        raise HTTPException(404, detail="Repository not found")

    # Use a temporary directory for the local git operation
    tmp_dir = tempfile.mkdtemp(prefix=f"merge_{name}_")
    
    try:
        # 1. Clone target branch (the one we merge INTO)
        # Using the local Git URL (port might vary, using config)
        git_url = f"{cfg.app.base_url}/{repo_id}.git"
        
        # We need a token for the clone
        token = "dummy-token-for-merge" # In reality, get or use user token
        
        repo = HFRepo(
            local_dir=tmp_dir,
            clone_from=git_url,
            use_auth_token=token,
            revision=target
        )

        # 2. Perform merge
        # Git merge source into target
        logger.info(f"Merging {source} into {target} for {repo_id}")
        
        try:
            # We use git directly via repo interface or subprocess
            repo.git_pull(rebase=False) # Ensure we are up to date
            
            # Fetch the source branch
            repo.git_shell(["git", "fetch", "origin", source])
            
            # Execute merge
            # --no-commit --no-ff allows us to check status before finalizing
            merge_cmd = ["git", "merge", f"origin/{source}", "--no-commit", "--no-ff"]
            process = repo.git_shell(merge_cmd)
            
            # 3. Check for conflicts
            status = repo.git_shell(["git", "status", "--porcelain"])
            conflicts = [line for line in status.splitlines() if line.startswith("UU")]
            
            if conflicts:
                logger.warning(f"Merge conflicts detected in {repo_id}: {conflicts}")
                # Get diff of conflicts
                diff = repo.git_shell(["git", "diff"])
                return {
                    "status": "conflict",
                    "conflicts": [c[3:] for c in conflicts],
                    "diff": diff,
                    "message": "Manual resolution required"
                }

            # 4. Success - Finalize and Push
            repo.git_shell(["git", "commit", "-m", f"Merge branch {source} into {target}"])
            repo.push_to_hub()
            
            # 5. Sync LakeFS (this happens automatically via push support implemented previously)
            
            return {
                "status": "success",
                "message": f"Merged {source} into {target} successfully",
                "repo": repo_id
            }

        except Exception as e:
            logger.exception(f"Merge execution failed for {repo_id}", e)
            raise HTTPException(500, detail=f"Merge failed: {str(e)}")

    finally:
        # Cleanup
        shutil.rmtree(tmp_dir, ignore_errors=True)
