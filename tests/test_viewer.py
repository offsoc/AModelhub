import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import datasets
from kohakuhub.api import viewer_v2

@pytest.fixture
def mock_lakefs():
    with patch("kohakuhub.api.viewer_v2.get_lakefs_client") as mock:
        client = MagicMock()
        client.list_objects.return_value = {
            "results": [
                {"path": "data/sample.parquet", "path_type": "object", "size_bytes": 1024}
            ],
            "pagination": {"has_more": False}
        }
        mock.return_value = client
        yield client

@pytest.fixture
def mock_s3():
    with patch("kohakuhub.api.viewer_v2.generate_download_presigned_url") as mock:
        mock.return_value = "http://minio:9000/test-bucket/data/sample.parquet?token=mock"
        yield mock

@pytest.fixture
def mock_dataset():
    data = {"text": ["hello", "world"], "label": [0, 1]}
    df = pd.DataFrame(data)
    ds = datasets.Dataset.from_pandas(df)
    
    with patch("datasets.load_dataset") as mock:
        mock.return_value = {"train": ds} # datasets usually returns a dict if split is specified or streaming
        yield mock

def test_create_hf_dataset_viewer(mock_lakefs, mock_s3, mock_dataset):
    """Test that the 100% HF-stack Gradio Blocks app can be initialized."""
    repo_id = "test/dataset"
    revision = "main"
    data_files = ["http://mock-url/sample.parquet"]
    
    demo = viewer_v2.create_hf_dataset_viewer(repo_id, revision, data_files=data_files)
    assert demo is not None
    assert len(demo.blocks) > 0

def test_create_hf_model_widget():
    """Test that the HF-stack model widget can be initialized."""
    with patch("transformers.pipeline") as mock_pipe:
        # Mocking gr.Interface.from_pipeline which is used in create_hf_model_widget
        with patch("gradio.Interface.from_pipeline") as mock_iface:
            mock_iface.return_value = MagicMock()
            demo = viewer_v2.create_hf_model_widget("test/model")
            assert demo is not None

@pytest.mark.asyncio
async def test_get_dataset_viewer_endpoint_refined(mock_lakefs, mock_s3):
    """Test the refined FastAPI endpoint for dataset viewer."""
    from kohakuhub.db import Repository
    
    with patch("kohakuhub.api.viewer_v2.get_repository") as mock_repo_get:
        mock_repo = MagicMock(spec=Repository)
        mock_repo_get.return_value = mock_repo
        
        with patch("kohakuhub.api.viewer_v2.check_repo_read_permission"):
            resp = await viewer_v2.get_dataset_viewer("test", "dataset", "main", request=MagicMock(), user=None)
            assert resp["mode"] == "local"
            assert resp["files_found"] == 1
