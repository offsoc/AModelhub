import pytest
from unittest.mock import MagicMock, patch
from kohakuhub.api import discussions

@pytest.fixture
def mock_db():
    with patch("kohakuhub.api.discussions.db.atomic") as mock_atomic:
        mock_atomic.return_value.__enter__.return_value = None
        yield mock_atomic

@pytest.mark.asyncio
async def test_create_discussion(mock_db):
    """Test creating a new discussion thread."""
    from kohakuhub.db import Repository, User, Discussion, Comment
    
    mock_user = MagicMock(spec=User)
    mock_user.username = "test_user"
    
    mock_repo = MagicMock(spec=Repository)
    mock_repo.full_id = "test/repo"
    
    with patch("kohakuhub.api.discussions.get_repository") as mock_get_repo:
        mock_get_repo.return_value = mock_repo
        
        with patch("kohakuhub.api.discussions.check_repo_read_permission"):
            with patch("kohakuhub.db.Discussion.create") as mock_d_create:
                with patch("kohakuhub.db.Comment.create") as mock_c_create:
                    mock_d = MagicMock(spec=Discussion)
                    mock_d.id = 1
                    mock_d_create.return_value = mock_d
                    
                    data = discussions.DiscussionCreate(title="Test Title", comment="Test Comment")
                    resp = await discussions.create_discussion(
                        "model", "test", "repo", data, user=mock_user
                    )
                    
                    assert resp["id"] == 1
                    mock_d_create.assert_called_once()
                    mock_c_create.assert_called_once()

@pytest.mark.asyncio
async def test_add_comment():
    """Test adding a comment to an existing thread."""
    from kohakuhub.db import User, Discussion, Comment
    
    mock_user = MagicMock(spec=User)
    mock_user.username = "commenter"
    
    mock_d = MagicMock(spec=Discussion)
    mock_d.id = 1
    
    with patch("kohakuhub.db.Discussion.get_or_none") as mock_get_d:
        mock_get_d.return_value = mock_d
        
        with patch("kohakuhub.db.Comment.create") as mock_c_create:
            mock_c = MagicMock(spec=Comment)
            mock_c.id = 101
            mock_c_create.return_value = mock_c
            
            data = discussions.CommentCreate(content="New comment content")
            resp = await discussions.add_comment(1, data, user=mock_user)
            
            assert resp["id"] == 101
            mock_c_create.assert_called_once()
            mock_d.save.assert_called_once()
