import os
import json
from motor.motor_asyncio import AsyncIOMotorClient
import logging
from typing import Dict, Any, List
import llm   
import asyncio
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

class MetricStore:
    def __init__(self, uri: str = "mongodb://localhost:27017/"):
        """Initialize database connection and metadata"""
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client['metrics_store']
        self.metrics = self.db['metric_data']
        self.metadata = self.db['metric_metadata']
        
    async def init_metadata(self, metadata: Dict = None):
        """Initialize and store metadata"""
        if metadata is None:
            metadata = {
                "entities": [
                    {
                        "name": "TVL",
                        "description": "Total Value Locked",
                        "sources": ["DefiLlama", "Dune", "Custom"],
                        "attributes": ["value"],
                        "metadata_fields": {
                            "chain": "Blockchain name",
                            "protocol": "Protocol name"
                        },
                        "tags": ["defi", "tvl", "blockchain"],
                        "vector": [0.1, 0.2, 0.3]  # Example vector embedding
                    },
                    # Can add more entities here
                ]
            }
        
        try:
            # Create index for name-based queries
            await self.metadata.create_index("entities.name")
            
            # If using vector search, create vector index
            # await self.metadata.create_index([("entities.vector", "2d")])
            
            existing = await self.metadata.find_one({"type": "entity_definitions"})
            if not existing:
                await self.metadata.insert_one({
                    "type": "entity_definitions",
                    **metadata
                })
                logger.info("Metadata initialized successfully")
            else:
                logger.info("Metadata already exists")
            return metadata
        except Exception as e:
            logger.error(f"Failed to initialize metadata: {str(e)}")
            raise

    async def init_indexes(self):
        """Create necessary indexes"""
        try:
            # Create compound index for quick queries
            await self.metrics.create_index([
                ("entity", 1),
                ("timestamp", -1),
                ("source", 1)
            ])

            # Create index for tags and common metadata fields
            await self.metrics.create_index("tags")
            await self.metrics.create_index("metadata")
            
            logger.info("Indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create indexes: {str(e)}")
            raise

    async def load_data(self, file_path: str) -> List[Dict]:
        """Load metric data from JSON file"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data file not found: {file_path}")
                
            with open(file_path, 'r') as file:
                data = json.load(file)
                logger.info(f"Loaded {len(data)} records from {file_path}")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to load data from {file_path}: {str(e)}")
            raise

    async def insert_metrics(self, metrics: List[Dict[str, Any]]):
        """Insert multiple metric records"""
        try:
            result = await self.metrics.insert_many(metrics)
            logger.info(f"Inserted {len(result.inserted_ids)} documents")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Failed to insert metrics: {str(e)}")
            raise

    async def query_latest(self, entity: str = None, limit: int = 5, **filters) -> List[Dict[str, Any]]:
        """Get latest metrics matching the filters"""
        try:
            query = {}
            if entity:
                query["entity"] = entity
            
            # Add any additional filters from metadata
            for key, value in filters.items():
                query[f"metadata.{key}"] = value

            # Find all matching documents, sorted by timestamp
            cursor = self.metrics.find(query).sort("timestamp", -1).limit(limit)
            documents = await cursor.to_list(length=None)
            
            if not documents:
                logger.info(f"No documents found for query: {query}")
                return []
                
            return documents
        except Exception as e:
            logger.error(f"Failed to query latest metrics: {str(e)}")
            raise

    async def compare_sources(self, entity: str = None) -> list:
        """Compare metrics across different sources"""
        try:
            pipeline = [
                {"$match": {"entity": entity} if entity else {}},
                {"$group": {
                    "_id": "$source",
                    "latest_value": {"$last": "$value"},
                    "latest_timestamp": {"$last": "$timestamp"}
                }},
                {"$sort": {"latest_timestamp": -1}}
            ]
            
            cursor = self.metrics.aggregate(pipeline)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Failed to compare sources: {str(e)}")
            raise

    async def query_metrics(self, 
                          entity: str = None,
                          start_time: str = None,
                          end_time: str = None,
                          **filters) -> List[Dict]:
        """Query metrics with time range and filters"""
        try:
            query = {}
            if entity:
                query["entity"] = entity
            
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gte"] = start_time
                if end_time:
                    query["timestamp"]["$lte"] = end_time
            
            for key, value in filters.items():
                query[f"metadata.{key}"] = value
            cursor = self.metrics.find(query).sort("timestamp", -1)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Failed to query metrics: {str(e)}")
            raise



async def main():
    store = MetricStore()
    entity = "TVL"
    
    question = "What is the total value locked on Base?"
    filters = {"chain": "Base"}
    
    # Get metadata for context with entity filter
    metadata_doc = await store.metadata.find_one(
        {"type": "entity_definitions"},
        {"entities": {"$elemMatch": {"name": entity}}}
    )
    
    # Extract entity metadata from the document
    entity_metadata = metadata_doc["entities"][0] if metadata_doc and metadata_doc["entities"] else None
    
    # Get latest metrics data
    db_response = await store.query_latest(entity=entity, **filters)
    if db_response is None:
        print("No data found for the given query")
        return
        
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metrics_template = METRICS_TEMPLATE.format(
        question=question,
        db_response=db_response,
        current_time=now,
        metadata=entity_metadata
    )
    response = await llm.chat(metrics_template, model="gpt-4o-mini")
    print(response)

if __name__ == "__main__":
    asyncio.run(main())