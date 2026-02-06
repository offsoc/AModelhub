import os
import pandas as pd
import gradio as gr
from fastapi import APIRouter, Depends, HTTPException, Request
from transformers import pipeline
import datasets
from huggingface_hub import list_repo_files, get_repo_type
from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.utils.lakefs import get_lakefs_client, lakefs_repo_name
from kohakuhub.utils.s3 import generate_download_presigned_url
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.auth.permissions import check_repo_read_permission
from kohakuhub.db_operations import get_repository

logger = get_logger("ViewerV2")
router = APIRouter(prefix="/viewer", tags=["Viewer"])

async def resolve_local_data_files(repo_type: str, repo_id: str, revision: str):
    """List LakeFS files and generate presigned URLs for compatible data files."""
    try:
        lakefs_repo = lakefs_repo_name(repo_type, repo_id)
        client = get_lakefs_client()
        
        # Paginate through all objects
        all_results = []
        after = ""
        has_more = True
        while has_more:
            res = await client.list_objects(lakefs_repo, revision, after=after)
            all_results.extend(res["results"])
            has_more = res["pagination"].get("has_more", False)
            after = res["pagination"].get("next_offset", "") if has_more else ""

        data_files = []
        for obj in all_results:
            path = obj["path"]
            if any(path.endswith(ext) for ext in [".parquet", ".csv", ".json", ".jsonl"]):
                # Construct file key for S3
                # In KohakuHub, objects are stored in S3 as {bucket}/{lakefs_repo}/{revision}/{path}
                url = await generate_download_presigned_url(cfg.s3.bucket, f"{lakefs_repo}/{revision}/{path}")
                data_files.append(url)
        
        return data_files
    except Exception as e:
        logger.warning(f"Failed to resolve local paths for {repo_id}: {e}")
        return []

def create_hf_dataset_viewer(repo_id: str, revision: str, data_files: list = None):
    """Creates a 100% HF-stack Gradio Blocks app for dataset viewing."""
    with gr.Blocks(css=".gradio-container {background-color: #f9fafb}") as demo:
        gr.Markdown(f"### 📂 Dataset Viewer: `{repo_id}`")
        
        with gr.Row():
            with gr.Column(scale=4):
                if data_files:
                    try:
                        # Load using datasets library with local presigned URLs
                        # Deterministic format detection based on first file
                        ds_type = "parquet" if data_files[0].split('?')[0].endswith(".parquet") else "csv"
                        ds = datasets.load_dataset(ds_type, 
                                                 data_files={"train": data_files}, 
                                                 streaming=True)
                        sample_ds = ds["train"].take(50)
                        df = pd.DataFrame(list(sample_ds))
                        gr.DataTable(value=df, label="Preview (First 50 rows)", interactive=False)
                    except Exception as e:
                        gr.Error(f"Failed to load local data: {e}")
                        gr.Markdown(f"⚠️ **Fallback**: Attempting to load from HF Hub...")
                else:
                    gr.Warning("No local data files found. Falling back to HF Hub if possible.")

        with gr.Accordion("Dataset Card", open=False):
            gr.Markdown(f"Full dataset card for `{repo_id}` would be rendered here using markdown from README.md.")

    return demo

def create_hf_model_widget(repo_id: str):
    """Creates a transformers-based Gradio Interface for model inference."""
    try:
        # Auto-detect task or default to text-classification
        pipe = pipeline(model=repo_id, device_map="auto")
        
        return gr.Interface.from_pipeline(pipe)
    except Exception as e:
        logger.error(f"Error loading model widget for {repo_id}: {e}")
        with gr.Blocks() as error_demo:
            gr.Markdown(f"### ❌ Failed to load model widget\nError: `{str(e)}`")
        return error_demo

@router.get("/dataset/{namespace}/{repo_name}/{revision}")
async def get_dataset_viewer(
    namespace: str,
    repo_name: str,
    revision: str,
    request: Request,
    user=Depends(get_optional_user)
):
    repo_id = f"{namespace}/{repo_name}"
    repo = get_repository("dataset", namespace, repo_name)
    
    # Try local first
    if repo:
        check_repo_read_permission(repo, user)
        local_files = await resolve_local_data_files("dataset", repo_id, revision)
        if local_files:
            return {"mode": "local", "files_found": len(local_files)}
            
    # Fallback: Check HF Hub if allowed or local failed
    try:
        # Check if repo exists on HF Hub
        hf_files = list_repo_files(repo_id, revision=revision, repo_type="dataset")
        return {"mode": "fallback_hf", "files_found": len(hf_files)}
    except Exception as e:
        logger.error(f"Fallback check failed for {repo_id}: {e}")
        raise HTTPException(404, detail="Dataset not found locally or on HF Hub")

@router.get("/model/{namespace}/{repo_name}")
async def get_model_viewer(
    namespace: str,
    repo_name: str,
    user=Depends(get_optional_user)
):
    repo_id = f"{namespace}/{repo_name}"
    repo = get_repository("model", namespace, repo_name)
    if repo:
        check_repo_read_permission(repo, user)
    
    return {"message": "Model widget ready", "repo_id": repo_id}
