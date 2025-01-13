import os
import json
from motor.motor_asyncio import AsyncIOMotorClient
import logging
from typing import Dict, Any, List, TypeVar
from pydantic import BaseModel, ValidationError
import llm   
import asyncio
from datetime import datetime
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
ModelT = TypeVar("ModelT", bound="BaseModel")

class EntityFilter(BaseModel):
    entity: str
    filter: Dict[str, Any]


def extract_json_str(s: str) -> str:
    if match := re.search(r"```(?:json)?(.+)```", s, re.DOTALL):
        return match[1]

    if match := re.search(r"`(.+)`", s):
        return match[1]

    return s.strip("`")

def extract_json(s: str, *, model: type[ModelT]) -> ModelT | None:
    if json_str := extract_json_str(s):
        try:
            return model.model_validate_json(json_str)
        except ValidationError as e:
            logger.exception(
                "Failed to validate JSON", json_str=json_str, model=model, exc_info=e
            )
            return None

    return None

def load_data(self, file_path: str) -> List[Dict]:
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


METADATA_EXAMPLE = [ 
                    {
                        "name": "TVL",
                        "description": "Total Value Locked",
                        "sources": "webiste url",
                        "metadata": {
                            "chain": "Blockchain name",
                            "protocol": "Protocol name"
                        },
                        "vector": [0.1, 0.2, 0.3]  # Example vector embedding
                    },
]

EXTRACT_METADATA_TEMPLATE = """
        you're a helpful assistant that can answer questions about metrics for query database .
        you should choose the key and minimal entity and filter according to the question and metadata accross multiple data sources.
        return in json format .
        
        # metadata
        {metadata}
        
        # question
        {question}

        #example
        {{
            "entity": "TVL",
            "filter": {{
                "chain": "Ethereum"
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

class MetricStore:
    def __init__(self, uri: str = None):
        """Initialize database connection"""
        self.uri = uri or os.getenv('MAIN_STORE_URI', "mongodb://localhost:27017/")
        self.client = AsyncIOMotorClient(self.uri)
        self.db = self.client['metrics_store']
        self.metrics = self.db['metric_data']
        self.metadata = self.db['metric_metadata']
        
    async def init_metadata(self, metadata: List = None):
        """Initialize and store metadata in main store"""
        if metadata is None:
            metadata = [{
                "name": "TVL",
                "description": "Total Value Locked",
                "sources": ["defillama", "footprint analytics"],  # Track original sources
                "metadata": {
                    "chain": "Blockchain name",
                    "protocol": "Protocol name"
                },
                "vector": [0.1, 0.2, 0.3]
            }]
        
        try:
            # Use main store URI from env
            main_uri = os.getenv('MAIN_STORE_URI', "mongodb://localhost:27017/")
            self.client = AsyncIOMotorClient(main_uri)
            self.db = self.client['metrics_store']
            self.metadata = self.db['metric_metadata']
            
            # Create index for name-based queries
            await self.metadata.create_index("name")
            await self.metadata.create_index("sources")  # Add index for sources
            
            existing = await self.metadata.find_one({"name": "TVL"})
            if not existing:
                # Insert each metadata document separately
                result = await self.metadata.insert_many(metadata)
                logger.info(f"Metadata initialized successfully: {len(result.inserted_ids)} documents")
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
                query[f"metadata.{key}"] = {"$regex": f"^{value}$", "$options": "i"}

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


async def main():
    store = MetricStore()
    
    import sys
    question = sys.argv[1] if len(sys.argv) > 1 else "What is the total value locked on Base?"

    extract_metadata_template = EXTRACT_METADATA_TEMPLATE.format(
        metadata=METADATA_EXAMPLE,
        question=question
    )
    response = await llm.chat(extract_metadata_template, model="gpt-4o-mini")
    print(response)
    data_dict = extract_json(response, model=EntityFilter)
    entity = data_dict.entity
    filters = data_dict.filter

    if entity is None:
        print("No entity found")
        return
    
    # Extract entity metadata from the document
    entity_metadata = await store.metadata.find_one({"name": entity})
    print(entity_metadata)
    # # Get latest metrics data
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