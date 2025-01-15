from dataclasses import dataclass
from typing import Optional

@dataclass
class MetricDefinition:
    name: str
    value_path: str
    entity_path: Optional[str] = None
    timestamp_path: Optional[str] = None

@dataclass
class TimeSeriesPoint:
    entity: str
    metric: str
    value: float
    timestamp: int
    source: str 