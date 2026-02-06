import pytest
import os
import time
from unittest.mock import MagicMock, patch
from kohakuhub.api import spaces

@pytest.fixture
def mock_subprocess():
    with patch("subprocess.Popen") as mock:
        proc = MagicMock()
        proc.pid = 1234
        proc.poll.return_value = None
        mock.return_value = proc
        yield mock

@pytest.fixture
def mock_os():
    with patch("os.makedirs") as mock_mkdir:
        with patch("os.killpg") as mock_kill:
            with patch("os.getpgid") as mock_pgid:
                mock_pgid.return_value = 5678
                yield mock_mkdir, mock_kill

@pytest.mark.asyncio
async def test_deploy_space_lifecycle(mock_subprocess, mock_os):
    """Test the full lifecycle of a space deployment."""
    from kohakuhub.db import Repository
    from fastapi import BackgroundTasks
    
    # Reset internal state
    spaces._RUNNING_SPACES = {}
    
    repo_id = "test/space-repo"
    namespace, repo_name = "test", "space-repo"
    
    with patch("kohakuhub.api.spaces.get_repository") as mock_repo_get:
        mock_repo = MagicMock(spec=Repository)
        mock_repo_get.return_value = mock_repo
        
        with patch("kohakuhub.api.spaces.check_repo_read_permission"):
            bg_tasks = BackgroundTasks()
            
            # 1. Trigger deployment
            resp = await spaces.deploy_space(namespace, repo_name, "main", bg_tasks, user=None)
            assert resp["repo_id"] == repo_id
            assert spaces._RUNNING_SPACES[repo_id]["status"] == "starting"
            
            # 2. Simulate background task execution
            await spaces.deploy_gradio_app(repo_id, "main", namespace, repo_name)
            assert spaces._RUNNING_SPACES[repo_id]["status"] == "running"
            assert spaces._RUNNING_SPACES[repo_id]["port"] is not None
            
            # 3. Check status
            status_resp = await spaces.get_space_status(namespace, repo_name)
            assert status_resp["status"] == "running"
            assert "http://localhost:" in status_resp["url"]
            
            # 4. Stop space
            stop_resp = await spaces.stop_space(namespace, repo_name)
            assert stop_resp["message"] == "Space stopped"
            assert repo_id not in spaces._RUNNING_SPACES

def test_find_free_port():
    port = spaces.find_free_port()
    assert isinstance(port, int)
    assert 1024 <= port <= 65535
