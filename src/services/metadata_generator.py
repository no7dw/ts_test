from typing import Dict, List
import json
import structlog
from models.metadata import MetricMetadata, MetricMetadataList
from utils.utils import extract_json
from slugify import slugify

logger = structlog.get_logger(__name__)

METADATA_EXAMPLE = {
    "name": "tvl",
    "description": "Total Value Locked",
    "scope": "chain",
    "freq": "daily",
    "period": "1d"
}

PROMPT_GENERATE_METADATA_SCHEMA = """
    you're a helpful assistant that can convert the raw crawl data and given metadata schema to a time series metric metadata schema.    
    return in json format with a list of objects under a 'metrics' key.

    ## example metadata schema:
    {{"metrics": [{metadata}]}}

    ## example raw data:
    {sample_data}

    ## your answer:
"""

async def generate_metadata_schema(sample_data: Dict, llm_client) -> List[MetricMetadata]:
    metrics_template = PROMPT_GENERATE_METADATA_SCHEMA.format(
            sample_data=json.dumps(sample_data),
            metadata=json.dumps(METADATA_EXAMPLE),
        )
    logger.debug(f"llm metadata schema prompt: {metrics_template}")
    response = await llm_client.chat(metrics_template, model="gpt-4o-mini") 
    logger.debug(f"llm metadata schema response: {response}")
    
    if schema_list := extract_json(response, model=MetricMetadataList):
        # Normalize metric names using slugify
        for metric in schema_list.metrics:
            metric.name = slugify(metric.name)
        return schema_list.metrics
    return [] 