import os
import io
import base64
import datetime
from typing import Any, Dict, List, Optional

import numpy as np
from PIL import Image
from functools import lru_cache

import datasets
from datasets import load_dataset_builder, load_dataset, get_dataset_config_names, get_dataset_split_names

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from .models import DatasetInfoResponse, DatasetRowResponse

logger = get_logger("DatasetsViewer")

# Initialize HF_ENDPOINT globally
_BASE_URL = cfg.app.base_url or "http://localhost:48888"
os.environ["HF_ENDPOINT"] = _BASE_URL.rstrip("/")

# Production limits
MAX_IMG_SIZE_BYTES = 2 * 1024 * 1024  # 2MB limit for base64 inline images
MAX_AUDIO_DURATION_SEC = 300  # 5 minutes limit for inline audio

@lru_cache(maxsize=128)
def get_dataset_info(namespace: str, repo: str, ref: str = "main", token: Optional[str] = None) -> DatasetInfoResponse:
    """
    Get dataset information (configs, splits, features).
    """
    repo_id = f"{namespace}/{repo}"
    
    try:
        # Get all configs
        try:
            configs = get_dataset_config_names(repo_id, token=token, trust_remote_code=True, revision=ref)
        except (ValueError, Exception) as e:
            logger.warning(f"Failed to get configs for {repo_id}: {e}")
            configs = ["default"]

        info_data = {}
        for config in configs:
            try:
                builder = load_dataset_builder(repo_id, config, token=token, trust_remote_code=True, revision=ref)
                builder_info = builder.info
                
                info_data[config] = {
                    "description": builder_info.description,
                    "citation": builder_info.citation,
                    "homepage": builder_info.homepage,
                    "license": builder_info.license,
                    "features": builder_info.features.to_dict() if builder_info.features else None,
                    "splits": {
                        split_name: {
                            "num_examples": split_info.num_examples,
                            "dataset_name": split_info.dataset_name
                        } for split_name, split_info in builder_info.splits.items()
                    } if builder_info.splits else {}
                }
                
                # If splits are empty, try manual fetch
                if not info_data[config]["splits"]:
                     split_names = get_dataset_split_names(repo_id, config, token=token, trust_remote_code=True, revision=ref)
                     info_data[config]["splits"] = {s: {} for s in split_names}

            except Exception as e:
                logger.error(f"Error loading builder for config {config}: {e}")
                info_data[config] = {"error": str(e)}

        return DatasetInfoResponse(configs=list(configs), info=info_data)
    except Exception as e:
        logger.error(f"Global error in get_dataset_info for {repo_id}: {e}")
        raise


def get_dataset_rows(
    namespace: str, 
    repo: str, 
    config: str, 
    split: str, 
    offset: int = 0, 
    limit: int = 100, 
    ref: str = "main", 
    token: Optional[str] = None,
    where: Optional[str] = None
) -> DatasetRowResponse:
    """
    Stream rows from the dataset.
    """
    repo_id = f"{namespace}/{repo}"
    
    try:
        # Load dataset in streaming mode
        ds = load_dataset(
            repo_id, 
            name=config if config != "default" else None, 
            split=split, 
            streaming=True, 
            token=token, 
            trust_remote_code=True,
            revision=ref
        )
        
        # Apply filter if provided
        if where:
            try:
                # Restrict globals for safety
                safe_globals = {"__builtins__": {}}
                ds = ds.filter(lambda x: eval(where, safe_globals, x))
            except Exception as e:
                logger.warning(f"Filter error in {repo_id}: {e}")
                return DatasetRowResponse(
                    rows=[], offset=offset, limit=limit, split=split, config=config,
                    error=f"Filter error: {e}"
                )
        
        rows = []
        features = ds.features
        
        # We must iterate for offset
        for i, sample in enumerate(ds):
            if i < offset:
                continue
            if len(rows) >= limit:
                break
            
            serialized_sample = {
                k: _serialize_value(v, features.get(k) if features else None) 
                for k, v in sample.items()
            }
            rows.append(serialized_sample)
        
        return DatasetRowResponse(
            rows=rows,
            offset=offset,
            limit=limit,
            split=split,
            config=config
        )
        
    except Exception as e:
        logger.error(f"Error fetching rows for {repo_id}: {e}")
        return DatasetRowResponse(
            rows=[], offset=offset, limit=limit, split=split, config=config,
            error=str(e)
        )


def _serialize_value(value: Any, feature: Any = None) -> Any:
    """Serialize values for JSON response."""
    if value is None:
        return None

    # Handle ClassLabel
    if feature and isinstance(feature, datasets.ClassLabel):
        try:
             if isinstance(value, int):
                 return feature.int2str(value)
        except Exception:
             pass
    
    # Handle PIL Image
    if isinstance(value, Image.Image):
        try:
            # Check dimensions for safety
            if value.width * value.height > 10000 * 10000:
                return "<Image Too Large>"
                
            buffered = io.BytesIO()
            fmt = value.format or "PNG"
            value.save(buffered, format=fmt)
            
            data_bytes = buffered.getvalue()
            if len(data_bytes) > MAX_IMG_SIZE_BYTES:
                return "<Image Exceeds Inline Limit>"
                
            img_str = base64.b64encode(data_bytes).decode("utf-8")
            return f"data:image/{fmt.lower()};base64,{img_str}"
        except Exception as e:
            logger.warning(f"Image serialization failed: {e}")
            return f"<Image Error: {type(e).__name__}>"

    # Handle Audio
    if isinstance(value, dict) and "array" in value and "sampling_rate" in value:
        try:
            ary = value["array"]
            sr = value["sampling_rate"]
            
            if not isinstance(ary, np.ndarray):
                ary = np.array(ary)
                
            duration = len(ary) / sr if sr > 0 else 0
            if duration > MAX_AUDIO_DURATION_SEC:
                return {"error": "Audio too long for inline preview", "duration": duration}

            # Normalize and convert to wav
            if ary.dtype.kind == 'f':
                ary = np.clip(ary, -1.0, 1.0)
                ary = (ary * 32767).astype(np.int16)
            
            buffered = io.BytesIO()
            import wave
            with wave.open(buffered, 'wb') as wav_file:
                wav_file.setnchannels(ary.shape[1] if len(ary.shape) > 1 else 1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sr)
                wav_file.writeframes(ary.tobytes())
                
            wav_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
            return {
                "src": f"data:audio/wav;base64,{wav_str}",
                "sampling_rate": sr,
                "duration": duration
            }
        except Exception as e:
             logger.warning(f"Audio serialization failed: {e}")
             return f"<Audio Error: {type(e).__name__}>"

    # Generic types
    if isinstance(value, (np.ndarray, np.generic)):
        # Limit array size for JSON serialization
        if value.size > 1000:
            return f"<Array size {value.size} suppressed>"
        return value.tolist()
        
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
        
    if isinstance(value, bytes):
        return f"<Binary data: {len(value)} bytes>"
        
    return value

