from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from models.metrics import TimeSeriesPoint

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, raw_data: Dict[str, Any]) -> List[TimeSeriesPoint]:
        """Extract time series points from raw data"""
        pass

    @staticmethod
    @abstractmethod
    def extract_sample_item(data_sample: Dict[str, Any], entity_identifier: str) -> Tuple[Dict[str, Any], str]:
        """Extract a single item from the data sample for metadata generation"""
        pass 