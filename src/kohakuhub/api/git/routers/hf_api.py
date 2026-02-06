"""Hugging Face Hub compatibility API.

This module implements standard Hugging Face Hub REST endpoints for 
repository management, ensuring 100% compatibility with hf_transfer and
huggingface-cli.
"""

from typing import Optional, Literal
from fastapi import APIRouter, Depends, Query, HTTPException, Response

from kohakuhub.db import User, Repository
from kohakuhub.db_operations import get_repository
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.api.repo.routers.crud import (
    create_repo as internal_create_repo,
    delete_repo as internal_delete_repo,
    CreateRepoPayload,
    DeleteRepoPayload,
)
from kohakuhub.api.repo.utils.hf import hf_repo_not_found, hf_error_response, HFErrorCode

router = APIRouter()

RepoType = Literal["model", "dataset", "space"]

@router.post("/models")
async def create_model(
    payload: CreateRepoPayload, 
    user: User = Depends(get_current_user)
):
    """Alias for POST /api/models (Hub compatible)."""
    payload.type = "model"
    return await internal_create_repo(payload, user)

@router.post("/datasets")
async def create_dataset(
    payload: CreateRepoPayload, 
    user: User = Depends(get_current_user)
):
    """Alias for POST /api/datasets (Hub compatible)."""
    payload.type = "dataset"
    return await internal_create_repo(payload, user)

@router.delete("/models/{namespace}/{name:path}")
async def delete_model(
    namespace: str,
    name: str,
    user: User = Depends(get_current_user)
):
    """Alias for DELETE /api/models/{repo_id} (Hub compatible)."""
    payload = DeleteRepoPayload(type="model", namespace=namespace, name=name)
    return await internal_delete_repo(payload, (user, False))

@router.delete("/datasets/{namespace}/{name:path}")
async def delete_dataset(
    namespace: str,
    name: str,
    user: User = Depends(get_current_user)
):
    """Alias for DELETE /api/datasets/{repo_id} (Hub compatible)."""
    payload = DeleteRepoPayload(type="dataset", namespace=namespace, name=name)
    return await internal_delete_repo(payload, (user, False))

@router.get("/repos/create")
async def check_name_availability(
    name: str = Query(...),
    organization: Optional[str] = Query(None),
    type: RepoType = Query("model"),
    user: User = Depends(get_current_user)
):
    """Check repository name availability.
    
    Matches Hub API: GET /api/repos/create?name=...&organization=...&type=...
    """
    namespace = organization or user.username
    full_id = f"{namespace}/{name}"
    
    existing = get_repository(type, namespace, name)
    if existing:
        return hf_error_response(
            400, 
            HFErrorCode.REPO_EXISTS, 
            f"Repository {full_id} already exists"
        )
        
    return {"available": True}
