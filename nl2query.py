import os
from datetime import datetime
from typing import Any, Dict, List

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

import llm
from utils import extract_json

logger = structlog.get_logger(__name__)


class EntityFilter(BaseModel):
    entity: str
    filter: Dict[str, Any]


METADATA_EXAMPLE = [
    {
        "name": "TVL",
        "description": "Total Value Locked",
        "sources": "webiste url",
        "scope": "chain",
        "freq": "daily",
        "period": "1d",
        "vector": [0.1, 0.2, 0.3],  # Example vector embedding
    },
]


DATA_SCHEMA = """
{
    _id: ObjectId
    entity: str, (which is the value of the entity )
    metric: str,
    value: float,
    timestamp: int,
    source: str
}
"""

EXTRACT_METADATA_TEMPLATE = """
        you're a helpful assistant that can answer questions about metrics for query database .
        you should choose the key and minimal entity and filter according to the question and metadata accross multiple data sources.
        return in json format .

        
        # metadata
        {metadata}

        # schema of the data is:
        {schema}
        
        # question
        {question}

        #output example (dont add any common here) for asking ethereum tvl since yyyy-mm-dd
        {{
            "entity": "Ethereum",
            "filter": {{
                "metric": "TVL",
                "timestamp": {{
                    "$gte": 1736215200,
                }}
            }}
        }}

        # your answer is:

        """

METRICS_TEMPLATE = """
        you're a helpful assistant that can answer questions about metrics.
        you should choose more trusted data source and common sense to answer the question.
        
        # metadata
        {metadata}
        
        # context
        {db_response}
        
        current time: {current_time}

        # question
        {question}
        """


class NL2Query:
    def __init__(self):
        pass

    async def generate_query(self, question: str):
        raise NotImplementedError("method not implemented")

    async def execute_query(
        self, query: Dict[str, Any], default_limit: int = 5
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("method not implemented")
    
    async def get_schema_info(self) -> str:
        raise NotImplementedError("method not implemented")

    async def get_metrics(self, entity: str, filters: Dict[str, Any]):
        raise NotImplementedError("method not implemented")

    async def insert_metrics(self, metrics: List[Dict[str, Any]]) -> List[Any]:
        raise NotImplementedError("method not implemented")

    async def get_response(self, question: str) -> str:
        raise NotImplementedError("method not implemented")


class NL2QueryEngine(NL2Query):
    def __init__(self, uri: str = None):
        """Initialize database connection"""
        self.uri = uri or os.getenv("MAIN_STORE_URI", "mongodb://localhost:27017/")
        self.client = AsyncIOMotorClient(self.uri)
        self.db = self.client["metrics_store"]
        self.metrics = self.db["metric_data"]
        self.metadata = self.db["metric_metadata"]

    async def get_schema_info(self) -> str:
        """Currently returns static schema, but prepared for future DB implementation."""
        return DATA_SCHEMA

    async def get_response(self, question: str) -> str:
        # Get entity and filters from the question
        entity, filters = await self.generate_query(question)

        if entity is None or filters.get("metric") is None:
            return f"No entity or metric found for {entity} {filters}"
        # Query metadata from database
        entity_metadata = await self.metadata.find_one({"name": filters.get("metric")})

        if entity_metadata is None:
            return f"No metadata found for entity: {entity}"

        # Query metrics data
        db_response = await self.execute_query(
            query={entity: entity, "filter": filters}
        )
        if db_response is None:
            return "No data found for the given query"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metrics_template = METRICS_TEMPLATE.format(
            question=question,
            db_response=db_response,
            current_time=now,
            metadata=entity_metadata,
        )
        logger.debug(metrics_template)
        return await llm.chat(metrics_template, model="gpt-4o-mini")

    async def generate_query(self, question: str):
        # Get available metadata from database for the prompt
        metadata_examples = await self.metadata.find().to_list(length=None)

        extract_metadata_template = EXTRACT_METADATA_TEMPLATE.format(
            metadata=metadata_examples,  # Use actual metadata from database
            schema=await self.get_schema_info(),
            question=question,
        )
        logger.debug(extract_metadata_template)
        response = await llm.chat(extract_metadata_template, model="gpt-4o-mini")
        data_dict = extract_json(response, model=EntityFilter)
        logger.debug(f"Extracted entity and filters: {data_dict}")
        return data_dict.entity, data_dict.filter

    async def execute_query(
        self, query: Dict[str, Any], default_limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get latest metrics matching the filters"""
        try:
            # Initialize the MongoDB query
            mongo_query = {}

            # Extract entity and filter from input
            entity = query.get("entity")
            filters = query.get("filter", {})

            # Add entity to query if present
            if entity:
                mongo_query["entity"] = entity

            # Add filters with case-insensitive regex matching
            # Exclude special filter keys like 'timestamp' that need different handling
            for key, value in filters.items():
                if key == "timestamp":
                    mongo_query["timestamp"] = (
                        value  # Preserve timestamp comparison operators
                    )
                else:
                    mongo_query[key] = {"$regex": f"^{value}$", "$options": "i"}

            logger.debug("executing_query", query=mongo_query)

            cursor = (
                self.metrics.find(mongo_query)
                .sort("timestamp", -1)
                .limit(default_limit)
            )
            documents = await cursor.to_list(length=None)

            if not documents:
                logger.warning(f"No documents found for query: {mongo_query}")
                return None

            return documents
        except Exception as e:
            logger.error(f"Failed to query latest metrics: {str(e)}")
            raise

    async def insert_metrics(self, metrics: List[Dict[str, Any]]) -> List[Any]:
        """Insert multiple metric records with validation and error handling.

        Args:
            metrics: List of metric dictionaries to insert

        Returns:
            List of inserted document IDs

        Raises:
            ValueError: If metrics list is empty
            Exception: For database operation failures
        """
        if not metrics:
            raise ValueError("Cannot insert empty metrics list")

        try:
            # Validate required fields
            for metric in metrics:
                if not all(
                    key in metric for key in ["entity", "metric", "value", "timestamp"]
                ):
                    raise ValueError(f"Missing required fields in metric: {metric}")

                # Ensure timestamp is datetime
                if isinstance(metric["timestamp"], str):
                    metric["timestamp"] = datetime.fromisoformat(
                        metric["timestamp"].replace("Z", "+00:00")
                    )

            # Perform bulk insert
            result = await self.metrics.insert_many(metrics, ordered=False)

            inserted_count = len(result.inserted_ids)
            logger.info(
                "metrics_insertion_completed",
                inserted_count=inserted_count,
                total_records=len(metrics),
            )

            return result.inserted_ids

        except Exception as e:
            logger.error(
                "metrics_insertion_failed", error=str(e), metrics_count=len(metrics)
            )
            raise
