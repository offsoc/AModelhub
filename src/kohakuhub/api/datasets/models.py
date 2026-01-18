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
    error: Optional[str] = None

class ColumnStatistics(BaseModel):
    count: int
    null_count: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    unique_count: Optional[int] = None
    most_common: Optional[Any] = None

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
