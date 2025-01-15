from typing import Dict, Any, Optional
import structlog
from .base import BaseExtractor
from .generic import GenericExtractor
from utils.formatters import MetricNameFormat
from utils.formatters import format_metric_name

logger = structlog.get_logger(__name__)

class ExtractorRegistry:
    def __init__(self):
        self.extractors = {}

    def register(self, data_type: str, extractor: BaseExtractor):
        self.extractors[data_type] = extractor

    def get_extractor(self, data_type: str) -> Optional[BaseExtractor]:
        return self.extractors.get(data_type)

    def register_from_sample(self, data_type: str, data_sample: Dict[str, Any], 
                           entity_identifier: str, timestamp_field: str = "created_at",
                           name_format: MetricNameFormat = MetricNameFormat.SLUG):
        """Register a new extractor by analyzing a data sample"""
        sample_item, base_path = GenericExtractor.extract_sample_item(data_sample, entity_identifier)
        
        metrics = []
        for key, value in sample_item.items():
            if isinstance(value, (int, float)) and key != entity_identifier:
                metrics.append({
                    "name": format_metric_name(key, name_format),
                    "value_path": key,
                })

        mapping = {
            "entity_base_path": f"$.{base_path}",
            "default_entity_field": entity_identifier,
            "default_timestamp_field": timestamp_field,
            "metrics": metrics,
        }

        self.extractors[data_type] = GenericExtractor(mapping)
        return mapping 