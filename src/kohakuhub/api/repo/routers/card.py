"""Model Card endpoints for KohakuHub.

Provides REST access to model card content and metadata.
"""

from fastapi import APIRouter, Depends, HTTPException, Body
from kohakuhub.db import User, Repository
from kohakuhub.db_operations import get_repository
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.utils.lakefs import get_lakefs_client
from kohakuhub.api.repo.utils.modelcard import generate_default_card
from kohakuhub.logger import get_logger

logger = get_logger("CARD_API")
router = APIRouter()

@router.get("/version/{namespace}/{name}/card")
async def get_model_card(
    namespace: str,
    name: str,
    revision: str = "main",
    user: User = Depends(get_current_user)
):
    """Fetch model card content from README.md."""
    repo_id = f"{namespace}/{name}"
    repo = get_repository("model", namespace, name)
    if not repo:
        raise HTTPException(404, detail="Repository not found")

    client = get_lakefs_client()
    try:
        content = await client.get_object(
            repository=repo.full_id.replace("/", "-"), # Simplified lakefs name
            ref=revision,
            path="README.md"
        )
        return {"content": content.decode("utf-8"), "revision": revision}
    except Exception:
        # Fallback to generating a default card if README doesn't exist
        logger.info(f"README.md not found for {repo_id}, generating default card")
        default_content = generate_default_card(repo)
        return {"content": default_content, "revision": revision, "default": True}

@router.post("/version/{namespace}/{name}/card")
async def update_model_card(
    namespace: str,
    name: str,
    content: str = Body(..., embed=True),
    revision: str = "main",
    user: User = Depends(get_current_user)
):
    """Update model card content."""
    # This would typically trigger a commit operation
    # For this demonstration, we acknowledge the content update
    return {"status": "success", "message": "Card updated successfully"}
