from typing import Dict, Any, List, Optional, Tuple
import structlog
from .base import BaseExtractor
from models.metrics import MetricDefinition, TimeSeriesPoint
from utils.json_path import traverse_json
from utils.time import convert_iso_to_timestamp
import jsonpath_ng.ext as jsonpath

logger = structlog.get_logger(__name__)

class GenericExtractor(BaseExtractor):
    def __init__(self, mapping_rules: Dict[str, Any]):
        self.mapping_rules = mapping_rules
        self.metrics = self._compile_paths()

    def _compile_paths(self) -> List[MetricDefinition]:
        metrics = []
        for metric in self.mapping_rules["metrics"]:
            metrics.append(
                MetricDefinition(
                    name=metric["name"],
                    value_path=metric["value_path"],
                    entity_path=self.mapping_rules["default_entity_field"],
                    timestamp_path=self.mapping_rules["default_timestamp_field"]
                )
            )
        return metrics

    @staticmethod
    def extract_sample_item(data_sample: Dict[str, Any], entity_identifier: str) -> Tuple[Dict[str, Any], str]:
        """Extract a single item from the raw data structure and return with its base path"""
        # If the input is already a single item containing the entity_identifier
        if isinstance(data_sample, dict) and entity_identifier in data_sample:
            return data_sample, ""

        # Otherwise, search for nested array data
        base_paths = []
        for path, value in traverse_json(data_sample):
            if isinstance(value, list) and value:
                first_item = value[0]
                if isinstance(first_item, dict) and entity_identifier in first_item:
                    base_paths.append(path)

        if not base_paths:
            available_paths = [p for p, _ in traverse_json(data_sample)]
            raise ValueError(f"Could not find {entity_identifier} in data sample. Available paths: {available_paths}")

        base_path = min(base_paths, key=len) if base_paths else ""
        logger.debug(f"Base Path: {base_path}")
        
        sample_array = jsonpath.parse(f"$.{base_path}").find(data_sample)[0].value
        if not sample_array or not isinstance(sample_array, list):
            raise ValueError("No array data found at the specified path")
            
        return sample_array[0], base_path

    def extract(self, raw_data: Dict[str, Any]) -> List[TimeSeriesPoint]:
        results = []
        base_matches = jsonpath.parse(self.mapping_rules["entity_base_path"]).find(raw_data)
        
        for match in base_matches:
            for base_obj in match.value:
                source = base_obj.get('source', '')
                for metric in self.metrics:
                    value = base_obj.get(metric.value_path)
                    entity = base_obj.get(metric.entity_path)
                    timestamp_str = base_obj.get(metric.timestamp_path)
                    
                    if timestamp_str:
                        timestamp = convert_iso_to_timestamp(timestamp_str)
                    else:
                        continue

                    if all(x is not None for x in [value, entity, timestamp]):
                        results.append(
                            TimeSeriesPoint(
                                entity=entity,
                                metric=metric.name,
                                value=float(value),
                                timestamp=timestamp,
                                source=source
                            )
                        )
        return results 