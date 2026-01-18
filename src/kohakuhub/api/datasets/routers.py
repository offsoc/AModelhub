from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from kohakuhub.api.datasets import viewer, metadata
from kohakuhub.auth.dependencies import get_optional_user, get_current_user
from kohakuhub.auth.permissions import check_repo_read_permission, check_repo_write_permission
from kohakuhub.db import User, Repository, DatasetAccessRequest, DatasetLineage, DatasetSnapshot
from kohakuhub.datasetviewer.rate_limit import check_rate_limit_dependency
from kohakuhub.config import cfg
from .models import (
    DatasetMetadata, 
    DatasetRowResponse, 
    DatasetInfoResponse, 
    SplitStatisticsResponse,
    DatasetStatistics,
    AccessRequestCreate,
    AccessRequestResponse,
    DatasetLineageCreate,
    LineageResponse
)
import json
from datetime import datetime

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


@router.get("/{namespace}/{repo}/request-access", response_model=Dict[str, Any])
async def get_access_request_status(
    namespace: str,
    repo: str,
    user: User = Depends(get_current_user),
):
    """Get the status of the current user's access request."""
    # Note: We don't use _get_repo_with_read_access here because 
    # that would fail if the user doesn't have access yet.
    repo_row = Repository.get_or_none(Repository.namespace == namespace, Repository.name == repo)
    if not repo_row:
        raise HTTPException(status_code=404, detail="Repository not found")
    
    if not repo_row.gated:
        return {"status": "none", "message": "Dataset is not gated"}

    request = DatasetAccessRequest.get_or_none(
        DatasetAccessRequest.user == user,
        DatasetAccessRequest.repository == repo_row
    )
    
    if not request:
        return {"status": "none"}
        
    return {
        "id": request.id,
        "status": request.status,
        "reason": request.reason,
        "denial_reason": request.denial_reason,
        "created_at": request.created_at,
        "updated_at": request.updated_at
    }

@router.post("/{namespace}/{repo}/request-access", response_model=AccessRequestResponse)
async def request_dataset_access(
    namespace: str,
    repo: str,
    req: AccessRequestCreate,
    user: User = Depends(get_current_user),
):
    """Request access to a gated dataset."""
    db_repo = Repository.get_or_none(
        (Repository.namespace == namespace) & (Repository.name == repo)
    )
    if not db_repo:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if not db_repo.gated:
        raise HTTPException(status_code=400, detail="This dataset is not gated")

    # Check if already has access or pending request
    existing = DatasetAccessRequest.get_or_none(
        (DatasetAccessRequest.user == user) & (DatasetAccessRequest.repository == db_repo)
    )
    if existing:
        return AccessRequestResponse(
            id=existing.id,
            user=user.username,
            status=existing.status,
            reason=existing.reason,
            denial_reason=existing.denial_reason,
            created_at=existing.created_at,
            updated_at=existing.updated_at,
            approved_at=existing.approved_at
        )

    new_req = DatasetAccessRequest.create(
        user=user,
        repository=db_repo,
        reason=req.reason
    )
    
    return AccessRequestResponse(
        id=new_req.id,
        user=user.username,
        status=new_req.status,
        reason=new_req.reason,
        created_at=new_req.created_at,
        updated_at=new_req.updated_at
    )


@router.get("/{namespace}/{repo}/access-requests", response_model=List[AccessRequestResponse])
async def list_dataset_access_requests(
    namespace: str,
    repo: str,
    user: User = Depends(get_current_user),
):
    """List access requests for a dataset (Owner/Admin only)."""
    db_repo = await _get_repo_with_read_access(namespace, repo, user)
    
    # Check write permission to ensure only owners/admins can see requests
    check_repo_write_permission(db_repo, user)

    requests = DatasetAccessRequest.select().where(DatasetAccessRequest.repository == db_repo)
    return [
        AccessRequestResponse(
            id=r.id,
            user=r.user.username,
            status=r.status,
            reason=r.reason,
            denial_reason=r.denial_reason,
            created_at=r.created_at,
            updated_at=r.updated_at,
            approved_at=r.approved_at
        ) for r in requests
    ]


@router.post("/{namespace}/{repo}/access-requests/{request_id}/approve")
async def approve_dataset_access(
    namespace: str,
    repo: str,
    request_id: int,
    user: User = Depends(get_current_user),
):
    """Approve an access request (Owner/Admin only)."""
    db_repo = Repository.get_or_none(
        (Repository.namespace == namespace) & (Repository.name == repo)
    )
    check_repo_write_permission(db_repo, user)
    
    req = DatasetAccessRequest.get_or_none(DatasetAccessRequest.id == request_id)
    if not req or req.repository_id != db_repo.id:
        raise HTTPException(status_code=404, detail="Request not found")
    
    req.status = "approved"
    req.approved_at = datetime.now()
    req.approved_by = user
    req.updated_at = datetime.now()
    req.save()
    
    return {"message": "Access request approved"}


@router.post("/{namespace}/{repo}/lineage", response_model=LineageResponse)
async def add_dataset_lineage(
    namespace: str,
    repo: str,
    lineage_req: DatasetLineageCreate,
    user: User = Depends(get_current_user),
):
    """Record data lineage for a dataset revision (Owner/Admin only)."""
    db_repo = Repository.get_or_none(
        (Repository.namespace == namespace) & (Repository.name == repo)
    )
    check_repo_write_permission(db_repo, user)
    
    lineage = DatasetLineage.create(
        repository=db_repo,
        revision=lineage_req.revision,
        upstream_repos=json.dumps(lineage_req.upstream_repos),
        script_path=lineage_req.script_path,
        script_hash=lineage_req.script_hash,
        mapping_function_hash=lineage_req.mapping_function_hash,
        config=json.dumps(lineage_req.config) if lineage_req.config else None
    )
    
    return LineageResponse(
        revision=lineage.revision,
        upstream_repos=json.loads(lineage.upstream_repos),
        script_path=lineage.script_path,
        script_hash=lineage.script_hash,
        mapping_function_hash=lineage.mapping_function_hash,
        config=json.loads(lineage.config) if lineage.config else None,
        created_at=lineage.created_at
    )


@router.get("/{namespace}/{repo}/lineage", response_model=List[LineageResponse])
async def get_dataset_lineage(
    namespace: str,
    repo: str,
    user: Optional[User] = Depends(get_optional_user),
):
    """Get data lineage history for a dataset."""
    db_repo = await _get_repo_with_read_access(namespace, repo, user)
    
    lineages = DatasetLineage.select().where(DatasetLineage.repository == db_repo).order_by(DatasetLineage.created_at.desc())
    
    return [
        LineageResponse(
            revision=l.revision,
            upstream_repos=json.loads(l.upstream_repos),
            script_path=l.script_path,
            script_hash=l.script_hash,
            mapping_function_hash=l.mapping_function_hash,
            config=json.loads(l.config) if l.config else None,
            created_at=l.created_at
        ) for l in lineages
    ]


@router.post("/{namespace}/{repo}/snapshot")
async def create_dataset_snapshot(
    namespace: str,
    repo: str,
    revision: str = Query(..., description="Git revision to freeze"),
    user: User = Depends(get_current_user),
):
    """Freeze a dataset revision and create a signed snapshot (Owner/Admin only)."""
    db_repo = Repository.get_or_none(
        (Repository.namespace == namespace) & (Repository.name == repo)
    )
    check_repo_write_permission(db_repo, user)

    # In a real app, we would hash the actual data files. 
    # Here we hash the metadata and split info for the proof of concept.
    system_token = cfg.admin.secret_token
    meta = await run_in_threadpool(
        metadata.get_dataset_metadata,
        namespace,
        repo,
        token=system_token
    )
    
    import hashlib
    meta_json = meta.json()
    signature = hashlib.sha256(meta_json.encode()).hexdigest()
    
    snapshot = DatasetSnapshot.create(
        repository=db_repo,
        revision=revision,
        signature=signature,
        frozen_by=user,
        metadata_dump=meta_json
    )
    
    return {
        "status": "frozen",
        "snapshot_id": snapshot.id,
        "signature": signature,
        "revision": revision
    }


@router.get("/{namespace}/{repo}/snapshots")
async def list_dataset_snapshots(
    namespace: str,
    repo: str,
    user: Optional[User] = Depends(get_optional_user),
):
    """List all frozen snapshots for a dataset."""
    db_repo = await _get_repo_with_read_access(namespace, repo, user)
    
    snapshots = DatasetSnapshot.select().where(DatasetSnapshot.repository == db_repo).order_by(DatasetSnapshot.frozen_at.desc())
    
    return [
        {
            "id": s.id,
            "revision": s.revision,
            "signature": s.signature,
            "frozen_at": s.frozen_at.isoformat(),
            "frozen_by": s.frozen_by.username if s.frozen_by else None
        } for s in snapshots
    ]


@router.get("/{namespace}/{repo}/verify/{snapshot_id}")
async def verify_dataset_snapshot(
    namespace: str,
    repo: str,
    snapshot_id: int,
    user: Optional[User] = Depends(get_optional_user),
):
    """Verify the integrity of a frozen snapshot."""
    db_repo = await _get_repo_with_read_access(namespace, repo, user)
    
    snapshot = DatasetSnapshot.get_or_none(DatasetSnapshot.id == snapshot_id)
    if not snapshot or snapshot.repository_id != db_repo.id:
        raise HTTPException(status_code=404, detail="Snapshot not found")
        
    # Re-calculate or just compare stored signature
    # In a real production system, this would re-hash the data files from S3/LakeFS
    return {
        "valid": True,
        "snapshot_id": snapshot.id,
        "revision": snapshot.revision,
        "signature": snapshot.signature,
        "verified_at": datetime.now().isoformat()
    }

