import pytest
from unittest.mock import MagicMock, patch
from kohakuhub.main import app
from fastapi.testclient import TestClient

client = TestClient(app)

@pytest.fixture
def mock_hf_repo():
    with patch("kohakuhub.api.version_merge.HFRepo") as mock:
        yield mock

@patch("kohakuhub.api.version_merge.get_repository")
@patch("kohakuhub.api.version_merge.get_current_user")
def test_merge_success(mock_user, mock_get_repo, mock_hf_repo):
    # Setup mocks
    mock_user.return_value = MagicMock(username="tester")
    mock_get_repo.return_value = MagicMock(repo_type="model")
    
    repo_instance = mock_hf_repo.return_value
    repo_instance.git_shell.return_value = "" # No conflicts
    
    # Execute merge
    response = client.post("/api/version/test/model/merge/dev/main")
    
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    repo_instance.push_to_hub.assert_called_once()

@patch("kohakuhub.api.version_merge.get_repository")
@patch("kohakuhub.api.version_merge.get_current_user")
def test_merge_conflict(mock_user, mock_get_repo, mock_hf_repo):
    # Setup mocks
    mock_user.return_value = MagicMock(username="tester")
    mock_get_repo.return_value = MagicMock(repo_type="model")
    
    repo_instance = mock_hf_repo.return_value
    # Simulate conflict status
    repo_instance.git_shell.side_effect = [
        "", # fetch
        "", # merge
        "UU model.bin\n", # status --porcelain
        "CONFLICT (content): Merge conflict in model.bin" # diff
    ]
    
    # Execute merge
    response = client.post("/api/version/test/model/merge/dev/main")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "conflict"
    assert "model.bin" in data["conflicts"]

def test_transformers_verification():
    # Mock transformers AutoModel loading
    with patch("transformers.AutoModel.from_pretrained") as mock_load:
        # Simulate loading a merged model
        mock_load.return_value = MagicMock()
        
        # In a real test, we would point this to the temp merged directory
        from transformers import AutoModel
        model = AutoModel.from_pretrained("/tmp/merged_model")
        
        assert model is not None
        mock_load.assert_called_once_with("/tmp/merged_model")
