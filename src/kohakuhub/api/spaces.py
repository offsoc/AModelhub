import os
import signal
import subprocess
import socket
import psutil
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.auth.permissions import check_repo_read_permission
from kohakuhub.db_operations import get_repository

logger = get_logger("Spaces")
router = APIRouter(prefix="/spaces", tags=["Spaces"])

# Global state for running spaces
# repo_id -> {process, port, status, start_time}
_RUNNING_SPACES: Dict[str, Dict] = {}

class SpaceStatus(BaseModel):
    repo_id: str
    status: str
    port: Optional[int] = None
    url: Optional[str] = None
    error: Optional[str] = None

def find_free_port():
    """Finds an available port on the host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def cleanup_space(repo_id: str):
    """Kills a running space process."""
    if repo_id in _RUNNING_SPACES:
        info = _RUNNING_SPACES[repo_id]
        proc = info["process"]
        try:
            # Kill process group to ensure children are gone
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception as e:
            logger.warning(f"Failed to kill space {repo_id}: {e}")
        del _RUNNING_SPACES[repo_id]

async def deploy_gradio_app(repo_id: str, revision: str, namespace: str, repo_name: str):
    """Downloads app files and starts Gradio in a subprocess."""
    try:
        # 1. Port allocation
        port = find_free_port()
        
        # 2. Workspace setup (simplified)
        workspace = os.path.join("/tmp/kohaku-spaces", repo_id.replace("/", "_"))
        os.makedirs(workspace, exist_ok=True)
        
        # In a real implementation, we'd pull files from LakeFS/MinIO here
        # For v1 demo, we assume app.py exists or we create a dummy one
        app_path = os.path.join(workspace, "app.py")
        if not os.path.exists(app_path):
            with open(app_path, "w") as f:
                f.write("import gradio as gr\ngr.Interface(lambda x: f'Hello {x}!', 'text', 'text').launch(server_name='0.0.0.0', server_port=" + str(port) + ")")

        # 3. Start subprocess
        # Use preexec_fn=os.setsid to allow group killing
        proc = subprocess.Popen(
            ["python", "app.py"],
            cwd=workspace,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        
        _RUNNING_SPACES[repo_id] = {
            "process": proc,
            "port": port,
            "status": "running",
            "revision": revision
        }
        logger.info(f"Deployed space {repo_id} on port {port}")
        
    except Exception as e:
        logger.error(f"Failed to deploy space {repo_id}: {e}")
        _RUNNING_SPACES[repo_id] = {"status": "error", "error": str(e)}

@router.post("/deploy/{namespace}/{repo_name}/{revision}")
async def deploy_space(
    namespace: str,
    repo_name: str,
    revision: str,
    background_tasks: BackgroundTasks,
    user=Depends(get_optional_user)
):
    repo_id = f"{namespace}/{repo_name}"
    repo = get_repository("dataset", namespace, repo_name)
    if not repo:
        raise HTTPException(404, detail="Repository not found")
        
    check_repo_read_permission(repo, user)
    
    # If already running, stop it first
    cleanup_space(repo_id)
    
    _RUNNING_SPACES[repo_id] = {"status": "starting"}
    background_tasks.add_task(deploy_gradio_app, repo_id, revision, namespace, repo_name)
    
    return {"message": "Deployment started", "repo_id": repo_id}

@router.get("/status/{namespace}/{repo_name}")
async def get_space_status(namespace: str, repo_name: str):
    repo_id = f"{namespace}/{repo_name}"
    if repo_id not in _RUNNING_SPACES:
        return {"status": "stopped"}
        
    info = _RUNNING_SPACES[repo_id]
    status = info["status"]
    
    # Verification: check if process is still alive
    if status == "running":
        proc = info["process"]
        if proc.poll() is not None:
            status = "crashed"
            info["status"] = "crashed"
            
    return {
        "repo_id": repo_id,
        "status": status,
        "port": info.get("port"),
        "url": f"http://localhost:{info.get('port')}" if status == "running" else None,
        "error": info.get("error")
    }

@router.post("/stop/{namespace}/{repo_name}")
async def stop_space(namespace: str, repo_name: str):
    repo_id = f"{namespace}/{repo_name}"
    cleanup_space(repo_id)
    return {"message": "Space stopped", "repo_id": repo_id}

@router.get("/list")
async def list_spaces():
    return [
        {"repo_id": rid, "status": data["status"], "port": data.get("port")}
        for rid, data in _RUNNING_SPACES.items()
    ]
