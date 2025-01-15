from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import jsonpath_ng.ext as jsonpath  # Using jsonpath-ng extended for better syntax
import structlog

logger = structlog.get_logger(__name__)


class MetricNameFormat(Enum):
    RAW = "raw"
    TITLE = "title"
    UPPER = "upper"


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


def convert_iso_to_timestamp(iso_date_str):
    """Convert ISO date string to Unix timestamp"""
    try:
        dt = datetime.fromisoformat(iso_date_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception as e:
        logger.error(f"Failed to convert date: {str(e)}")
        return None


@dataclass
class MappingGenerator:
    """Helper class to generate mapping rules from data samples"""

    @staticmethod
    def format_metric_name(
        name: str, format_style: MetricNameFormat = MetricNameFormat.RAW
    ) -> str:
        if format_style == MetricNameFormat.RAW:
            return name
        elif format_style == MetricNameFormat.TITLE:
            return name.replace("_", " ").title()
        elif format_style == MetricNameFormat.UPPER:
            return name.upper()
        return name

    @staticmethod
    def infer_mapping(
        data_sample: Dict[str, Any],
        entity_identifier: str,
        timestamp_field: str = "created_at",
        name_format: MetricNameFormat = MetricNameFormat.TITLE,
    ) -> Dict[str, Any]:
        """
        Dynamically generate mapping rules by analyzing a data sample
        """
        # Find potential base paths containing arrays with the entity identifier
        base_paths = []
        for path, value in MappingGenerator._traverse_json(data_sample):
            if isinstance(value, list) and value:  # Check if it's a non-empty list
                first_item = value[0]
                if isinstance(first_item, dict) and entity_identifier in first_item:
                    base_paths.append(path)

        if not base_paths:
            available_paths = [
                p for p, _ in MappingGenerator._traverse_json(data_sample)
            ]
            raise ValueError(
                f"Could not find {entity_identifier} in data sample. Available paths: {available_paths}"
            )

        # Select the most specific path that contains our entities
        base_path = min(base_paths, key=len) if base_paths else ""
        logger.debug(f"Base Path: {base_path}")
        # Get a sample entity from the array
        sample_array = jsonpath.parse(f"$.{base_path}").find(data_sample)[0].value
        if not sample_array or not isinstance(sample_array, list):
            raise ValueError("No array data found at the specified path")

        sample_entity = sample_array[0]
        metrics = []

        for key, value in sample_entity.items():
            if isinstance(value, (int, float)) and key != entity_identifier:
                metrics.append(
                    {
                        "name": MappingGenerator.format_metric_name(key, name_format),
                        "value_path": key,
                    }
                )

        return {
            "entity_base_path": f"$.{base_path}",
            "default_entity_field": entity_identifier,
            "default_timestamp_field": timestamp_field,
            "metrics": metrics,
        }

    @staticmethod
    def _traverse_json(obj: Any, path: str = "") -> List[tuple]:
        """Helper method to traverse JSON and find all paths"""
        paths = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                paths.append((new_path, value))
                paths.extend(MappingGenerator._traverse_json(value, new_path))
        elif isinstance(obj, list):
            # For lists, look at the first item and continue traversing
            if obj:  # only if list is not empty
                # Don't add array index to path, just traverse into first element
                paths.extend(MappingGenerator._traverse_json(obj[0], path))

        return paths


class GenericExtractor:
    def __init__(self, mapping_rules: Dict[str, Any]):
        """Initialize extractor with mapping rules"""
        self.mapping_rules = mapping_rules  # Store the rules
        self.metrics = []  # Initialize metrics as None
        self._compile_paths()  # Compile the paths and create metrics

    def _compile_paths(self):
        """Compile the mapping rules into metric definitions"""
        # self.metrics = []  # Start fresh with empty list

        for metric in self.mapping_rules["metrics"]:
            self.metrics.append(
                MetricDefinition(
                    name=metric["name"],
                    value_path=metric["value_path"],
                    entity_path=self.mapping_rules["default_entity_field"],
                    timestamp_path=self.mapping_rules["default_timestamp_field"],
                )
            )

    def extract(self, raw_data: Dict[str, Any]) -> List[TimeSeriesPoint]:
        """
        Extract time series points using the configured mapping
        """
        results = []
        source = raw_data.get("url", "")

        # Use the mapping rules to find data
        base_matches = jsonpath.parse(self.mapping_rules["entity_base_path"]).find(
            raw_data
        )
        # logger.debug(f"Base Object: {base_matches}")

        for match in base_matches:
            # match.value is a list of entities, so we need to iterate through it
            for base_obj in match.value:
                # For each metric defined in the mapping
                for metric in self.metrics:
                    # Extract values using the paths from mapping
                    # logger.debug(f"Metric: {metric}")
                    value = base_obj.get(metric.value_path)
                    entity = base_obj.get(metric.entity_path)

                    # Get timestamp (either from mapping or use default)
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
                                source=source,
                            )
                        )

        return results


class ExtractorRegistry:
    def __init__(self):
        self.extractors = {}
        self.mapping_generator = MappingGenerator()

    def register(self, data_type: str, extractor: GenericExtractor):
        self.extractors[data_type] = extractor

    def register_from_sample(
        self, data_type: str, data_sample: Dict[str, Any], entity_identifier: str
    ):
        """Register a new extractor by analyzing a data sample"""
        mapping = self.mapping_generator.infer_mapping(data_sample, entity_identifier)

        self.extractors[data_type] = GenericExtractor(mapping)
        return mapping  # Return for inspection if needed

    def get_extractor(self, data_type: str) -> Optional[GenericExtractor]:
        return self.extractors.get(data_type)


def process_data(
    raw_data: Dict[str, Any],
    registry: ExtractorRegistry,
    entity_id: str,
    timestamp_field: str = "created_at",
) -> List[TimeSeriesPoint]:
    """
    Process incoming data using registered extractors
    """
    data_type = raw_data.get("type")
    extractor = registry.get_extractor(data_type)
    if not extractor:
        mapping = MappingGenerator.infer_mapping(raw_data, entity_id, timestamp_field)
        logger.debug(f"Mapping: {mapping}")
        registry.register(data_type, GenericExtractor(mapping))
        extractor = registry.get_extractor(data_type)

    return extractor.extract(raw_data)


# Example usage:
if __name__ == "__main__":
    # Initialize registry
    registry = ExtractorRegistry()

    # Process actual data directly
    actual_data = {
        "type": "gmgn_wallet_data",
        "url": "https://gmgn.ai/api/...",
        "data": {
            "rank": [
                {
                    "wallet_address": "abc123",
                    "realized_profit": 150.0,
                    "pnl_7d": 75.0,
                    "created_at": "2024-01-02T00:00:00Z",
                },
                {
                    "wallet_address": "def456",
                    "realized_profit": 200.0,
                    "pnl_7d": 100.0,
                    "created_at": "2024-01-02T00:00:00Z",
                },
            ]
        },
    }

    actual_data = {
        "type": "gmgn_wallet_data",
        "url": "https://gmgn.ai/api/...",
        "data": [
            {
                "wallet_address": "abc123",
                "realized_profit": 150.0,
                "pnl_7d": 75.0,
                "created_at": "2024-01-02T00:00:00Z",
            },
            {
                "wallet_address": "def456",
                "realized_profit": 200.0,
                "pnl_7d": 100.0,
                "created_at": "2024-01-02T00:00:00Z",
            },
        ],
    }

    # Process the data - it will automatically create the extractor if needed
    entity_id = "wallet_address"
    results = process_data(actual_data, registry, entity_id)
    logger.info(f"Results: {results}")

    # Example 2: Generic protocol data
    actual_data = {
        "type": "defillama_chain_data",
        "url": "https://defillama.com/chains",
        "data": {
            "chains": [
                {
                    "chain": "Ethereum",
                    "tvl": 1000000.0,
                    "volume_24h": 500000.0,
                    "timestamp": "2024-01-02T00:00:00Z",
                },
                {
                    "chain": "Base",
                    "tvl": 300000.0,
                    "volume_24h": 100000.0,
                    "timestamp": "2024-01-02T00:00:00Z",
                },
            ]
        },
    }
    entity_id = "chain"
    timestamp_field = "timestamp"
    results = process_data(actual_data, registry, entity_id, timestamp_field)
    logger.info(f"Results: {results}")
