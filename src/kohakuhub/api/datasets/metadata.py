"""
Dataset metadata extraction and statistics module.
Uses the HuggingFace datasets library to extract comprehensive metadata.
"""

import os
import threading
from typing import Any, Dict, List, Optional
from functools import lru_cache

import datasets
from datasets import load_dataset, get_dataset_config_names, DatasetInfo
from datasets.features import Features, Value, ClassLabel, Sequence

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from .models import DatasetMetadata, DatasetFeature, DatasetSplitInfo, DatasetStatistics, SplitStatisticsResponse, ColumnStatistics

logger = get_logger("DatasetsMetadata")

# Initialize HF_ENDPOINT globally to avoid race conditions in threads
# This ensures that all dataset operations point to the local instance
_BASE_URL = cfg.app.base_url or "http://localhost:48888"
os.environ["HF_ENDPOINT"] = _BASE_URL.rstrip("/")

def _serialize_feature_type(feature) -> Dict[str, Any]:
    """Serialize a single feature type to a JSON-compatible dict."""
    if isinstance(feature, Value):
        return {
            "type": "Value",
            "dtype": str(feature.dtype),
        }
    elif isinstance(feature, ClassLabel):
        return {
            "type": "ClassLabel",
            "num_classes": feature.num_classes,
            "names": feature.names if feature.names else [],
        }
    elif isinstance(feature, Sequence):
        return {
            "type": "Sequence",
            "feature": _serialize_feature_type(feature.feature),
        }
    elif isinstance(feature, (dict, Features)):
        fields = {}
        target_dict = feature if isinstance(feature, dict) else feature.items()
        for k, v in (target_dict.items() if isinstance(target_dict, dict) else target_dict):
            fields[k] = _serialize_feature_type(v)
        return {
            "type": "Struct" if isinstance(feature, dict) else "Features",
            "fields": fields,
        }
    else:
        # Fallback for unrecognized types (Image, Audio, etc.)
        return {
            "type": str(type(feature).__name__),
            "value": str(feature),
        }


def _calculate_feature_type_distribution(features: Features) -> Dict[str, int]:
    """Calculate distribution of feature types."""
    distribution = {}
    
    def count_type(feature):
        key = 'other'
        if isinstance(feature, Value):
            dtype = str(feature.dtype)
            if 'int' in dtype or 'float' in dtype:
                key = 'numeric'
            elif 'string' in dtype:
                key = 'text'
            elif 'bool' in dtype:
                key = 'boolean'
        elif isinstance(feature, ClassLabel):
            key = 'categorical'
        elif isinstance(feature, Sequence):
            inner_type = type(feature.feature).__name__
            if 'Image' in inner_type:
                key = 'image'
            elif 'Audio' in inner_type:
                key = 'audio'
            else:
                key = 'sequence'
        elif 'Image' in type(feature).__name__:
            key = 'image'
        elif 'Audio' in type(feature).__name__:
            key = 'audio'
        
        distribution[key] = distribution.get(key, 0) + 1
    
    for feature in features.values():
        count_type(feature)
    
    return distribution


@lru_cache(maxsize=128)
def get_dataset_configs(namespace: str, repo: str, token: Optional[str] = None) -> List[str]:
    """Get available configuration names for a dataset."""
    repo_id = f"{namespace}/{repo}"
    try:
        configs = get_dataset_config_names(repo_id, token=token, trust_remote_code=True)
        return configs if configs else ["default"]
    except Exception as e:
        logger.warning(f"Could not get configs for {repo_id}: {e}")
        return ["default"]


@lru_cache(maxsize=128)
def get_dataset_metadata(
    namespace: str, 
    repo: str, 
    config: Optional[str] = None,
    token: Optional[str] = None
) -> DatasetMetadata:
    """
    Extract comprehensive metadata from a dataset using the datasets library.
    """
    repo_id = f"{namespace}/{repo}"
    configs = get_dataset_configs(namespace, repo, token)
    
    # Use provided config or first available
    current_config = config if config else (configs[0] if configs else "default")
    
    try:
        logger.info(f"Loading metadata for {repo_id} with config {current_config}")
        
        # Load dataset builder (lightweight)
        try:
            builder = datasets.load_dataset_builder(
                repo_id,
                name=current_config if current_config != "default" else None,
                token=token,
                trust_remote_code=True
            )
            info = builder.info
        except Exception as e:
            logger.warning(f"Builder failed for {repo_id}, config={current_config}: {e}")
            # Fallback: load with streaming to get info
            dataset = load_dataset(
                repo_id,
                name=current_config if current_config != "default" else None,
                streaming=True,
                token=token,
                trust_remote_code=True
            )
            first_split = next(iter(dataset.keys())) if dataset else None
            if first_split:
                info = dataset[first_split].info
            else:
                raise ValueError(f"No splits found for {repo_id}")
        
        # Extract metadata
        splits_data = {}
        total_rows = 0
        total_bytes = 0
        
        if info.splits:
            for split_name, split_info in info.splits.items():
                rows = getattr(split_info, 'num_examples', 0)
                bytes_size = getattr(split_info, 'num_bytes', 0)
                splits_data[split_name] = DatasetSplitInfo(
                    name=split_name,
                    num_rows=rows,
                    num_bytes=bytes_size,
                    num_columns=len(info.features) if info.features else 0
                )
                total_rows += rows
                total_bytes += bytes_size

        statistics = DatasetStatistics(
            total_rows=total_rows,
            total_bytes=total_bytes,
            num_splits=len(info.splits) if info.splits else 0,
            num_features=len(info.features) if info.features else 0
        )

        features_data = {}
        if info.features:
            features_data = {
                name: DatasetFeature(**_serialize_feature_type(feature))
                for name, feature in info.features.items()
            }

        return DatasetMetadata(
            configs=configs,
            current_config=current_config,
            splits=splits_data,
            features=features_data,
            statistics=statistics,
            type_distribution=_calculate_feature_type_distribution(info.features) if info.features else {},
            description=getattr(info, 'description', None),
            homepage=getattr(info, 'homepage', None),
            license=getattr(info, 'license', None)
        )
        
    except Exception as e:
        logger.error(f"Failed to get metadata for {repo_id}: {e}", exc_info=True)
        return DatasetMetadata(
            current_config=current_config,
            configs=configs,
            statistics=DatasetStatistics(),
            error=str(e)
        )


def get_split_statistics(
    namespace: str,
    repo: str,
    split: str,
    config: Optional[str] = None,
    token: Optional[str] = None,
    sample_size: int = 1000
) -> SplitStatisticsResponse:
    """
    Calculate statistics for a specific split by sampling data.
    """
    repo_id = f"{namespace}/{repo}"
    current_config = config if config and config != "default" else None
    
    try:
        dataset = load_dataset(
            repo_id,
            name=current_config,
            split=split,
            streaming=True,
            token=token,
            trust_remote_code=True
        )
        
        # Take sample with a safety limit
        sample = []
        for i, item in enumerate(dataset.take(sample_size)):
            sample.append(item)
            if i >= sample_size - 1:
                break
                
        if not sample:
            raise ValueError("Empty split")
        
        column_stats = {}
        features = dataset.features
        
        for key in sample[0].keys():
            values = [row[key] for row in sample if key in row]
            valid_values = [v for v in values if v is not None]
            
            stats_dict = {
                "count": len(valid_values),
                "null_count": len(values) - len(valid_values),
            }
            
            # Numeric stats
            numeric_values = []
            for v in valid_values:
                if isinstance(v, (int, float)):
                    numeric_values.append(float(v))
            
            if numeric_values:
                stats_dict.update({
                    "min": min(numeric_values),
                    "max": max(numeric_values),
                    "mean": sum(numeric_values) / len(numeric_values),
                })
            
            # Categorical stats
            if valid_values and isinstance(valid_values[0], (str, int, bool)):
                unique_values = set(valid_values)
                stats_dict.update({
                    "unique_count": len(unique_values),
                    "most_common": max(unique_values, key=valid_values.count) if valid_values else None,
                })
                
            column_stats[key] = ColumnStatistics(**stats_dict)
        
        return SplitStatisticsResponse(
            split=split,
            sample_size=len(sample),
            column_statistics=column_stats
        )
        
    except Exception as e:
        logger.error(f"Failed stats for {repo_id}/{split}: {e}")
        raise

