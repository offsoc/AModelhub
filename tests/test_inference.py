import pytest
from unittest.mock import MagicMock, patch
from kohakuhub.api import inference

@pytest.fixture
def mock_pipeline():
    with patch("transformers.pipeline") as mock:
        pipe = MagicMock()
        pipe.side_effect = lambda x, **kwargs: [{"label": "POSITIVE", "score": 0.99}] if "text" in str(x) else [{"generated_text": "Sample text"}]
        mock.return_value = pipe
        yield pipe

@pytest.fixture
def mock_accelerate():
    with patch("accelerate.init_empty_weights") as mock_init:
        with patch("accelerate.infer_auto_device_map") as mock_map:
            mock_map.return_value = {"": 0}
            yield mock_map

def test_get_pipeline(mock_pipeline, mock_accelerate):
    """Test that the pipeline is loaded with auto device mapping."""
    repo_id = "test/model"
    # Clear cache for deterministic test
    inference._MODEL_CACHE = {}
    
    pipe = inference.get_pipeline(repo_id)
    assert pipe is not None
    assert repo_id in inference._MODEL_CACHE

@pytest.mark.asyncio
async def test_run_inference_endpoint(mock_pipeline):
    """Test the FastAPI endpoint for inference."""
    from kohakuhub.db import Repository
    from fastapi import Request
    
    mock_req = MagicMock(spec=Request)
    mock_req.client.host = "127.0.0.1"
    
    with patch("kohakuhub.api.inference.get_repository") as mock_repo_get:
        mock_repo = MagicMock(spec=Repository)
        mock_repo_get.return_value = mock_repo
        
        with patch("kohakuhub.api.inference.check_repo_read_permission"):
            req_data = inference.InferenceRequest(inputs="Hello world")
            resp = await inference.run_inference("test", "model", "main", req_data, mock_req, user=None)
            assert resp["source"] == "local"
            assert len(resp["results"]) > 0

def test_create_inference_gradio():
    """Test that the inference Gradio Blocks app can be initialized."""
    demo = inference.create_inference_gradio("test/model")
    assert demo is not None
    assert len(demo.blocks) > 0
