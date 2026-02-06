import os
import time
from typing import Any, Dict, Optional
import torch
import gradio as gr
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from transformers import pipeline, AutoConfig
from accelerate import infer_auto_device_map, init_empty_weights
from slowapi import Limiter
from slowapi.util import get_remote_address

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.auth.permissions import check_repo_read_permission
from kohakuhub.db_operations import get_repository

logger = get_logger("Inference")
router = APIRouter(prefix="/inference", tags=["Inference"])
limiter = Limiter(key_func=get_remote_address)

class InferenceRequest(BaseModel):
    inputs: Any
    parameters: Optional[Dict[str, Any]] = None
    token: Optional[str] = None # User's HF token for fallback

# Global model cache to avoid reloading
_MODEL_CACHE = {}

def get_pipeline(repo_id: str, revision: str = "main"):
    """Loads or retrieves a pipeline with auto device mapping."""
    cache_key = f"{repo_id}:{revision}"
    if cache_key in _MODEL_CACHE:
        return _MODEL_CACHE[cache_key]
    
    try:
        # 1. Use accelerate to handle device mapping
        config = AutoConfig.from_pretrained(repo_id, revision=revision)
        with init_empty_weights():
            # Dummy model to calculate device map
            # In a real setup, we would load the actual weights
            pass
            
        # 2. Simplified pipeline loading with device_map="auto"
        # This handles CPU/GPU allocation automatically via accelerate
        pipe = pipeline(model=repo_id, revision=revision, device_map="auto")
        
        _MODEL_CACHE[cache_key] = pipe
        return pipe
    except Exception as e:
        logger.error(f"Failed to load pipeline for {repo_id}: {e}")
        return None

@router.post("/{namespace}/{repo_name}/{revision}")
@limiter.limit("5/minute")
async def run_inference(
    namespace: str,
    repo_name: str,
    revision: str,
    req: InferenceRequest,
    request: Request,
    user=Depends(get_optional_user)
):
    repo_id = f"{namespace}/{repo_name}"
    repo = get_repository("model", namespace, repo_name)
    if not repo:
        raise HTTPException(404, detail="Model not found")
        
    # Check read permission
    check_repo_read_permission(repo, user)
    
    # 1. Input Validation
    if isinstance(req.inputs, str) and len(req.inputs) > 2000:
        raise HTTPException(400, detail="Input too long (max 2000 chars)")
        
    # 2. Try Local Inference
    pipe = get_pipeline(repo_id, revision)
    
    if pipe:
        try:
            start_time = time.time()
            results = pipe(req.inputs, **(req.parameters or {}))
            duration = time.time() - start_time
            logger.info(f"Local inference for {repo_id} took {duration:.2f}s")
            return {"results": results, "source": "local", "duration": duration}
        except Exception as e:
            logger.warning(f"Local inference failed for {repo_id}: {e}")
            
    # 3. Fallback to HF Inference API
    if req.token:
        try:
            import requests
            API_URL = f"https://api-inference.huggingface.co/models/{repo_id}"
            headers = {"Authorization": f"Bearer {req.token}"}
            response = requests.post(API_URL, headers=headers, json={"inputs": req.inputs, "parameters": req.parameters})
            if response.status_code == 200:
                return {"results": response.json(), "source": "huggingface"}
        except Exception as e:
            logger.error(f"HF Fallback failed: {e}")
            
def create_inference_gradio(repo_id: str):
    """Creates a Gradio Blocks UI for inference, mimicking HF."""
    # Use a theme that matches HF (simplified)
    theme = gr.themes.Soft(
        primary_hue="blue",
        secondary_hue="gray",
    )
    
    with gr.Blocks(theme=theme, css=".gradio-container { border: none !important; }") as demo:
        gr.Markdown(f"### ⚡ Inference API: `{repo_id}`")
        
        with gr.Row():
            with gr.Column(scale=2):
                inputs = gr.Textbox(label="Input", lines=3, placeholder="Type your prompt here...")
                run_btn = gr.Button("Compute", variant="primary")
                
            with gr.Column(scale=2):
                output = gr.JSON(label="Output")
                loading_indicator = gr.HTML(visible=False, value='<div class="loader"></div>')

        def predict(text):
            # This calls the internal run_inference logic or pipe directly
            pipe = get_pipeline(repo_id)
            if not pipe:
                return {"error": "Pipeline not loaded"}
            return pipe(text)

        run_btn.click(predict, inputs=[inputs], outputs=[output])
        
    return demo

@router.get("/widget/{namespace}/{repo_name}")
async def get_inference_widget(
    namespace: str,
    repo_name: str,
    user=Depends(get_optional_user)
):
    repo_id = f"{namespace}/{repo_name}"
    # This endpoint will be used for the iframe src
    return {"message": "Inference widget ready", "repo_id": repo_id}
