import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from kohakuhub.main import app

client = TestClient(app)

@pytest.fixture
def mock_git_pack():
    # A dummy Git pack header (PACK version 2, 1 object)
    pack = b"PACK" + (2).to_bytes(4, "big") + (1).to_bytes(4, "big")
    # A tiny zlib compressed blob "hello world"
    import zlib
    content = b"hello world"
    header = b"\x30" + (len(content)).to_bytes(1, "big") # type 3 (blob), size
    pack += header + zlib.compress(content)
    # 20-byte dummy checksum
    pack += b"\0" * 20
    return pack

@patch("kohakuhub.api.git.routers.git_push.get_repository")
@patch("kohakuhub.api.git.routers.git_push.get_lakefs_client")
@patch("kohakuhub.api.git.routers.git_push.get_current_user")
def test_mock_git_push(mock_user, mock_lakefs, mock_get_repo, mock_git_pack):
    # Setup mocks
    mock_user.return_value = MagicMock(username="testuser")
    mock_get_repo.return_value = MagicMock(repo_type="model", full_id="test/repo")
    mock_lakefs.return_value.commit = MagicMock()
    
    # Execute push
    response = client.post(
        "/api/git/test/repo/push",
        content=mock_git_pack,
        headers={"Authorization": "Bearer dummy-token"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["objects_received"] == 1
    assert data["blobs"] == 1
    assert data["lfs_pointers"] == 0

def test_lfs_detection():
    from kohakuhub.api.git.routers.git_push import is_lfs_pointer, parse_lfs_pointer
    
    lfs_content = b"version https://git-lfs.github.com/spec/v1\noid sha256:12345678\nsize 100\n"
    assert is_lfs_pointer(lfs_content) is True
    
    oid, size = parse_lfs_pointer(lfs_content)
    assert oid == "12345678"
    assert size == 100
