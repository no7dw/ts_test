from typing import Dict, Any, List, Tuple
import structlog
from extractors.registry import ExtractorRegistry
from services.metadata_generator import generate_metadata_schema
from models.metrics import TimeSeriesPoint
from models.metadata import MetricMetadata
import llm  

logger = structlog.get_logger(__name__)

async def process_data(
    raw_data: Dict[str, Any],
    registry: ExtractorRegistry,
    entity_id: str,
    timestamp_field: str = "created_at",
    llm_client = None
) -> Tuple[List[TimeSeriesPoint], List[MetricMetadata]]:
    """Process data and generate metadata schema"""
    
    data_type = raw_data.get('type')
    extractor = registry.get_extractor(data_type)
    
    if not extractor:
        # Register new extractor if none exists
        mapping = registry.register_from_sample(data_type, raw_data, entity_id, timestamp_field)
        logger.debug(f"Created new mapping: {mapping}")
        extractor = registry.get_extractor(data_type)

    # Generate metadata schema if LLM client is provided
    schema = []
    if llm_client:
        sample_item, _ = extractor.extract_sample_item(raw_data, entity_id)
        schema = await generate_metadata_schema(sample_item, llm_client)
    
    # Extract time series data
    results = extractor.extract(raw_data)
    
    return results, schema

# Example usage
if __name__ == "__main__":
    import asyncio
    
    registry = ExtractorRegistry()
    
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
    
    results, schema = asyncio.run(process_data(
        actual_data, 
        registry, 
        entity_id="chain",
        timestamp_field="timestamp",
        llm_client=llm
    ))
    
    logger.info(f"Results: {results}")
    logger.info(f"Schema: {schema}")

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

    results, schema = asyncio.run(process_data(
        actual_data, 
        registry, 
        entity_id="wallet_address",
        timestamp_field="created_at",
        llm_client=llm
    ))

    logger.info(f"Results: {results}")
    logger.info(f"Schema: {schema}")
    

