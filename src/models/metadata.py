from pydantic import BaseModel
from typing import List

class MetricMetadata(BaseModel):
    name: str
    description: str
    scope: str
    freq: str
    period: str

class MetricMetadataList(BaseModel):
    metrics: List[MetricMetadata] 