import json
import asyncio
from typing import List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from kohakuhub.db import Discussion, Comment, Repository, User, db
from kohakuhub.auth.dependencies import get_current_user, get_optional_user
from kohakuhub.auth.permissions import check_repo_read_permission, check_repo_write_permission
from kohakuhub.db_operations import get_repository
from kohakuhub.logger import get_logger

logger = get_logger("Discussions")
router = APIRouter(prefix="/discussions", tags=["Discussions"])

# SSE event bus
_EVENT_QUEUES: List[asyncio.Queue] = []

async def broadcast_event(event_type: str, data: dict):
    event = json.dumps({"type": event_type, "data": data})
    for q in _EVENT_QUEUES:
        await q.put(event)

class DiscussionCreate(BaseModel):
    title: str
    comment: str

class CommentCreate(BaseModel):
    content: str

@router.post("/{repo_type}/{namespace}/{name}")
async def create_discussion(
    repo_type: str, namespace: str, name: str,
    data: DiscussionCreate,
    user=Depends(get_current_user)
):
    repo = get_repository(repo_type, namespace, name)
    if not repo:
        raise HTTPException(status_code=404, detail="Repository not found")
        
    check_repo_read_permission(repo, user)

    with db.atomic():
        discussion = Discussion.create(
            repository=repo,
            title=data.title,
            author=user
        )
        comment = Comment.create(
            discussion=discussion,
            author=user,
            content=data.comment
        )
    
    asyncio.create_task(broadcast_event("new_discussion", {"repo_id": repo.full_id, "discussion_id": discussion.id}))
    return {"id": discussion.id}

@router.get("/{repo_type}/{namespace}/{name}")
async def list_discussions(repo_type: str, namespace: str, name: str):
    repo = get_repository(repo_type, namespace, name)
    if not repo:
        raise HTTPException(status_code=404, detail="Repository not found")
    
    discussions = (Discussion.select()
                   .where(Discussion.repository == repo)
                   .order_by(Discussion.created_at.desc()))
    
    return [
        {
            "id": d.id,
            "title": d.title,
            "author": d.author.username,
            "status": d.status,
            "created_at": d.created_at,
            "comment_count": d.comments.count()
        } for d in discussions
    ]

@router.get("/thread/{discussion_id}")
async def get_discussion(discussion_id: int):
    d = Discussion.get_or_none(Discussion.id == discussion_id)
    if not d:
        raise HTTPException(status_code=404, detail="Discussion not found")
    
    return {
        "id": d.id,
        "title": d.title,
        "author": d.author.username,
        "status": d.status,
        "created_at": d.created_at,
        "comments": [
            {
                "id": c.id,
                "author": c.author.username,
                "content": c.content,
                "created_at": c.created_at
            } for c in d.comments.order_by(Comment.created_at.asc())
        ]
    }

@router.post("/thread/{discussion_id}/comment")
async def add_comment(
    discussion_id: int,
    data: CommentCreate,
    user=Depends(get_current_user)
):
    d = Discussion.get_or_none(Discussion.id == discussion_id)
    if not d:
        raise HTTPException(status_code=404, detail="Discussion not found")
    
    comment = Comment.create(
        discussion=d,
        author=user,
        content=data.content
    )
    
    d.updated_at = datetime.now(timezone.utc)
    d.save()
    
    asyncio.create_task(broadcast_event("new_comment", {"discussion_id": d.id, "comment_id": comment.id}))
    return {"id": comment.id}

@router.get("/notifications/sse")
async def sse_notifications(request: Request):
    async def event_generator():
        q = asyncio.Queue()
        _EVENT_QUEUES.append(q)
        try:
            while True:
                if await request.is_disconnected():
                    break
                event = await q.get()
                yield f"data: {event}\n\n"
        finally:
            _EVENT_QUEUES.remove(q)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")
