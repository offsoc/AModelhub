from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from kohakuhub.api.datasets import viewer, metadata
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.auth.permissions import check_repo_read_permission
from kohakuhub.db import User, Repository
from kohakuhub.datasetviewer.rate_limit import check_rate_limit_dependency
from kohakuhub.config import cfg
from .models import (
    DatasetMetadata, 
    DatasetRowResponse, 
    DatasetInfoResponse, 
    SplitStatisticsResponse,
    DatasetStatistics
)

router = APIRouter(prefix="/datasets", tags=["Dataset Viewer"])

async def _get_repo_with_read_access(namespace: str, repo_name: str, user: Optional[User]) -> Repository:
    """Helper to get a repository and verify read access."""
    full_id = f"{namespace}/{repo_name}"
    # Use repo_type="dataset" to ensure we are looking in the right place
    # Note: KohakuHub uses Repository model for models, datasets, and spaces
    repo = Repository.get_or_none(
        (Repository.namespace == namespace) & (Repository.name == repo_name)
    )
    
    if not repo:
        raise HTTPException(status_code=404, detail=f"Dataset '{full_id}' not found")
        
    # Verify read access
    check_repo_read_permission(repo, user)
    return repo


@router.get("/{namespace}/{repo}/viewer/info", response_model=DatasetInfoResponse)
async def get_dataset_info(
    namespace: str,
    repo: str,
    ref: str = Query("main", description="Git revision or branch"),
    user: Optional[User] = Depends(get_optional_user),
    identifier: str = Depends(check_rate_limit_dependency),
):
    """
    Get at-a-glance information about a dataset's configurations and splits.
    Requires read access to the repository.
    """
    await _get_repo_with_read_access(namespace, repo, user)
    
    # Use system token after verifying user permission
    system_token = cfg.admin.secret_token
    
    try:
        return await run_in_threadpool(
            viewer.get_dataset_info, 
            namespace, 
            repo, 
            ref=ref, 
            token=system_token
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch dataset info: {str(e)}")


@router.get("/{namespace}/{repo}/viewer/rows", response_model=DatasetRowResponse)
async def get_dataset_rows(
    namespace: str,
    repo: str,
    config: str = Query("default", description="Dataset configuration/subset"),
    split: str = Query("train", description="Dataset split name"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    ref: str = Query("main", description="Git revision or branch"),
    where: Optional[str] = Query(None, description="SQL-like filter expression"),
    user: Optional[User] = Depends(get_optional_user),
    identifier: str = Depends(check_rate_limit_dependency),
):
    """
    Stream and filter rows from a specific dataset split.
    Supports SQL-like filtering expressions (e.g., 'label == 1').
    """
    await _get_repo_with_read_access(namespace, repo, user)
    
    system_token = cfg.admin.secret_token
    
    try:
        return await run_in_threadpool(
            viewer.get_dataset_rows,
            namespace, 
            repo, 
            config=config, 
            split=split, 
            offset=offset, 
            limit=limit, 
            ref=ref, 
            token=system_token,
            where=where
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch dataset rows: {str(e)}")


@router.get("/{namespace}/{repo}/metadata", response_model=DatasetMetadata)
async def get_metadata(
    namespace: str,
    repo: str,
    config: Optional[str] = Query(None, description="Dataset configuration"),
    user: Optional[User] = Depends(get_optional_user),
    identifier: str = Depends(check_rate_limit_dependency),
):
    """
    Get comprehensive metadata including configs, splits, features, and statistics.
    Auto-detects properties using the datasets library.
    """
    await _get_repo_with_read_access(namespace, repo, user)
    
    system_token = cfg.admin.secret_token
    
    try:
        return await run_in_threadpool(
            metadata.get_dataset_metadata,
            namespace,
            repo,
            config=config,
            token=system_token
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract metadata: {str(e)}")


@router.get("/{namespace}/{repo}/configs")
async def get_configs(
    namespace: str,
    repo: str,
    user: Optional[User] = Depends(get_optional_user),
    identifier: str = Depends(check_rate_limit_dependency),
):
    """
    Get the list of available subsets/configurations for this dataset.
    """
    await _get_repo_with_read_access(namespace, repo, user)
    
    system_token = cfg.admin.secret_token
    
    try:
        configs = await run_in_threadpool(
            metadata.get_dataset_configs,
            namespace,
            repo,
            token=system_token
        )
        return {"configs": configs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get configs: {str(e)}")


@router.get("/{namespace}/{repo}/splits/{split}/statistics", response_model=SplitStatisticsResponse)
async def get_split_stats(
    namespace: str,
    repo: str,
    split: str,
    config: Optional[str] = Query(None, description="Dataset configuration"),
    sample_size: int = Query(1000, ge=10, le=10000),
    user: Optional[User] = Depends(get_optional_user),
    identifier: str = Depends(check_rate_limit_dependency),
):
    """
    Calculate detailed statistics for a specific split by sampling rows.
    Returns numeric (min, max, mean) and categorical (unique count, most common) stats.
    """
    await _get_repo_with_read_access(namespace, repo, user)
    
    system_token = cfg.admin.secret_token
    
    try:
        return await run_in_threadpool(
            metadata.get_split_statistics,
            namespace,
            repo,
            split,
            config=config,
            token=system_token,
            sample_size=sample_size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate split statistics: {str(e)}")

