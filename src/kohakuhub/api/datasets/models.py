from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class DatasetFeature(BaseModel):
    type: str
    dtype: Optional[str] = None
    num_classes: Optional[int] = None
    names: Optional[List[str]] = None
    feature: Optional[Any] = None  # For sequence/nested
    fields: Optional[Dict[str, Any]] = None  # For structs

class DatasetSplitInfo(BaseModel):
    name: str
    num_rows: int = 0
    num_bytes: int = 0
    num_columns: Optional[int] = None

class DatasetStatistics(BaseModel):
    total_rows: int = 0
    total_bytes: int = 0
    num_splits: int = 0
    num_features: int = 0

class DatasetMetadata(BaseModel):
    configs: List[str] = Field(default_factory=list)
    current_config: str
    splits: Dict[str, DatasetSplitInfo] = Field(default_factory=dict)
    features: Dict[str, DatasetFeature] = Field(default_factory=dict)
    statistics: DatasetStatistics
    type_distribution: Dict[str, int] = Field(default_factory=dict)
    description: Optional[str] = None
    homepage: Optional[str] = None
    license: Optional[str] = None
    compliance_status: Optional[str] = "pending" # pending, clean, flagged
    compliance_report: Optional[Dict[str, Any]] = None
    lineage: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None

class ColumnStatistics(BaseModel):
    count: int
    null_count: int
    nan_count: Optional[int] = 0
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    unique_count: Optional[int] = None
    most_common: Optional[Any] = None
    skew: Optional[float] = None
    outlier_count: Optional[int] = None
    avg_text_length: Optional[float] = None

class SplitStatisticsResponse(BaseModel):
    split: str
    sample_size: int
    column_statistics: Dict[str, ColumnStatistics]

class DatasetRowResponse(BaseModel):
    rows: List[Dict[str, Any]]
    offset: int
    limit: int
    split: str
    config: str
    error: Optional[str] = None

class DatasetInfoResponse(BaseModel):
    configs: List[str]
    info: Dict[str, Any]  # Complex nested info from builder


class AccessRequestCreate(BaseModel):
    reason: str


class AccessRequestResponse(BaseModel):
    id: int
    user: str
    status: str
    reason: str
    denial_reason: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    approved_at: Optional[datetime] = None


class DatasetLineageCreate(BaseModel):
    revision: str
    source_repos: List[str]
    script_path: str
    script_hash: str
    mapping_function_hash: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class LineageResponse(BaseModel):
    revision: str
    source_repos: List[str]
    script_path: str
    script_hash: str
    mapping_function_hash: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    created_at: datetime
