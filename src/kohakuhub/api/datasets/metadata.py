import os
import threading
import numpy as np
import re
import json
from typing import Any, Dict, List, Optional
from functools import lru_cache
from kohakuhub.db import Repository, DatasetLineage, DatasetSnapshot

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


def _scan_for_pii_and_sensitive(text: str) -> Dict[str, Any]:
    """Basic PII and sensitive content scanner."""
    report = {"pii_found": False, "sensitive_found": False, "matches": []}
    
    # Simple regex for common PII
    patterns = {
        "email": r"[\w\.-]+@[\w\.-]+\.\w+",
        "phone": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
        "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
        "ipv4": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
        "chinese_id": r"\b\d{17}[\dXx]\b",
        "credit_card": r"\b(?:\d[ -]*?){13,16}\b",
    }
    
    # Sensitive words (example list)
    sensitive_words = ["confidential", "password", "secret", "private_key", "internal_only"]
    
    for label, pattern in patterns.items():
        if re.search(pattern, text):
            report["pii_found"] = True
            report["matches"].append(label)
            
    for word in sensitive_words:
        if word.lower() in text.lower():
            report["sensitive_found"] = True
            report["matches"].append(f"sensitive_word:{word}")
            
    return report

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
def validate_schema_consistency(sample: List[Dict[str, Any]], expected_features: Features) -> List[str]:
    """Validate that sample rows match the expected schema and check for None values."""
    warnings = []
    if not sample:
        return warnings

    for key, feature in expected_features.items():
        null_count = sum(1 for row in sample if row.get(key) is None)
        null_ratio = null_count / len(sample)
        
        if null_ratio > 0.1:
            warnings.append(f"Column '{key}' has high null ratio: {null_ratio:.1%}")
            
        # Basic type checking
        if isinstance(feature, Value):
            for i, row in enumerate(sample[:10]): # Check first 10
                val = row.get(key)
                if val is not None:
                    # Very basic check
                    if 'int' in str(feature.dtype) and not isinstance(val, (int, np.integer)):
                        warnings.append(f"Type mismatch in '{key}': expected {feature.dtype}, found {type(val).__name__} at row {i}")
                        break
    return warnings


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

        # Get Lineage using the Repository model
        repo_obj = Repository.get_or_none(Repository.namespace == namespace, Repository.name == repo)
        lineage_data = []
        if repo_obj:
            lineages = DatasetLineage.select().where(DatasetLineage.repository == repo_obj).order_by(DatasetLineage.created_at.desc()).limit(5)
            lineage_data = [
                {
                    "revision": l.revision,
                    "created_at": l.created_at.isoformat(),
                    "script_path": l.script_path
                } for l in lineages
            ]

        # Initial compliance status
        compliance_status = "verified"
        compliance_report = None

        # Check for frozen status
        is_frozen = False
        snapshot_id = None
        signature = None
        if repo_obj:
            snapshot = DatasetSnapshot.get_or_none(
                DatasetSnapshot.repository == repo_obj,
                DatasetSnapshot.revision == info.download_checksum if hasattr(info, 'download_checksum') else None
            )
            # If revision matches builder info or we just check latest frozen for this repo
            # Actually better to look for exact revision match
            if snapshot:
                is_frozen = True
                snapshot_id = snapshot.id
                signature = snapshot.signature

        return DatasetMetadata(
            configs=configs,
            current_config=current_config,
            splits=splits_data,
            features=features_data,
            statistics=statistics,
            type_distribution=_calculate_feature_type_distribution(info.features) if info.features else {},
            description=getattr(info, 'description', None),
            homepage=getattr(info, 'homepage', None),
            license=getattr(info, 'license', None),
            lineage=lineage_data,
            compliance_status=compliance_status,
            compliance_report=compliance_report,
            is_frozen=is_frozen,
            snapshot_id=snapshot_id,
            signature=signature
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
                    if not np.isnan(v) and not np.isinf(v):
                        numeric_values.append(float(v))
            
            if numeric_values:
                arr = np.array(numeric_values)
                mean = float(np.mean(arr))
                std = float(np.std(arr))
                median = float(np.median(arr))
                
                # Outliers (simple Z-score > 3)
                outliers = 0
                if std > 0:
                    outliers = int(np.sum(np.abs(arr - mean) > 3 * std))
                
                # Skewness (simple calculation)
                skew = 0
                if std > 0:
                    skew = float(np.mean((arr - mean) ** 3) / (std ** 3))

                stats_dict.update({
                    "min": float(np.min(arr)),
                    "max": float(np.max(arr)),
                    "mean": mean,
                    "median": median,
                    "std_dev": std,
                    "skew": skew,
                    "outlier_count": outliers,
                })
            
            # Categorical / Text stats
            if valid_values:
                if isinstance(valid_values[0], str):
                    # Text specific stats
                    lengths = [len(v) for v in valid_values]
                    stats_dict["avg_text_length"] = sum(lengths) / len(lengths)
                    
                    # PII scan on a small sample of text
                    pii_report = _scan_for_pii_and_sensitive(" ".join(valid_values[:10]))
                    if pii_report["pii_found"] or pii_report["sensitive_found"]:
                        stats_dict["most_common"] = f"[FLAGGED: PII/Sensitive - {', '.join(pii_report['matches'])}]"
                
                if isinstance(valid_values[0], (str, int, bool)):
                    unique_values = set(valid_values)
                    stats_dict.update({
                        "unique_count": len(unique_values),
                    })
                    if not stats_dict.get("most_common"):
                         stats_dict["most_common"] = max(unique_values, key=valid_values.count) if valid_values else None
                
                # Label distribution
                if isinstance(features.get(key), ClassLabel):
                    dist = {}
                    for v in valid_values:
                        label_name = features[key].int2str(v) if hasattr(features[key], 'int2str') else str(v)
                        dist[label_name] = dist.get(label_name, 0) + 1
                    stats_dict["label_distribution"] = dist

                # Image stats
                if 'Image' in type(features.get(key)).__name__:
                    widths = []
                    heights = []
                    for img in valid_values:
                        if hasattr(img, 'size'): # PIL image
                            widths.append(img.size[0])
                            heights.append(img.size[1])
                    if widths:
                        stats_dict["image_stats"] = {
                            "avg_width": sum(widths) / len(widths),
                            "avg_height": sum(heights) / len(heights),
                            "min_size": [min(widths), min(heights)],
                            "max_size": [max(widths), max(heights)]
                        }
            
            # Schema warnings
            stats_dict["schema_warnings"] = validate_schema_consistency(sample, {key: features[key]})
            
            column_stats[key] = ColumnStatistics(**stats_dict)
        
        return SplitStatisticsResponse(
            split=split,
            sample_size=len(sample),
            column_statistics=column_stats
        )
        
    except Exception as e:
        logger.error(f"Failed stats for {repo_id}/{split}: {e}")
        raise

