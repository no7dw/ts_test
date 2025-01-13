import json
from appm import MetricStore
import asyncio
import logging
from typing import Dict, List
from motor.motor_asyncio import AsyncIOMotorClient
import os
from urllib.parse import urlparse

# Initialize logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_raw_data(file_uri: str) -> List[Dict]:
    """Load data from raw JSON file, handling both file:// URIs and regular paths"""
    try:
        # Parse the URI
        parsed = urlparse(file_uri)
        
        # Convert URI to local path
        if parsed.scheme == 'file':
            # Remove 'file://' and handle potential triple slashes
            file_path = os.path.abspath(parsed.path)
        else:
            file_path = file_uri
            
        # Read the file
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        logger.info(f"Successfully loaded {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to load data from {file_uri}: {str(e)}")
        raise

async def init_metadata():
    """Initialize metadata in main store"""
    client = None
    try:
        # Use main store URI from env
        main_uri = os.getenv('MAIN_STORE_URI', "mongodb://localhost:27017/")
        client = AsyncIOMotorClient(main_uri)
        db = client['metrics_store']
        metadata_collection = db['metric_metadata']
        
        # todo let LLM to generate metadata , with source is dynamic added once different sources are added
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
        
        # Create indexes
        await metadata_collection.create_index("name")
        await metadata_collection.create_index("sources")
        
        existing = await metadata_collection.find_one({"name": "TVL"})
        if not existing:
            result = await metadata_collection.insert_many(metadata)
            logger.info(f"Metadata initialized successfully: {len(result.inserted_ids)} documents")
        else:
            logger.info("Metadata already exists")
            
        return metadata
    except Exception as e:
        logger.error(f"Failed to initialize metadata: {str(e)}")
        raise
    finally:
        if client:
            client.close()  # No await needed for close()

async def setup_store():
    """ETL process: Load from raw files and insert into MongoDB"""
    try:
        # Load raw data configurations
        with open('config.file.json', 'r') as f:
            raw_configs = json.load(f)
            
        # Load MongoDB configurations
        with open('config.json', 'r') as f:
            mongo_configs = json.load(f)
            
        # Create mapping of source to MongoDB URI
        source_to_mongo = {config['source']: config['uri'] for config in mongo_configs}
        
        # Initialize main store for final data (27017)
        import os
        main_store_uri = os.environ.get('MAIN_STORE_URI')
        main_store = MetricStore(main_store_uri)  # Using defillama's URI as main
        
        # Process each raw data source
        all_data = []
        for config in raw_configs:
            source = config['source']
            raw_data = load_raw_data(config['uri'])
            
            
            all_data.extend(raw_data)
            logger.info(f"Processed {len(raw_data)} records from {source}")
        
        # Insert all data into main store
        if all_data:
            await main_store.insert_metrics(all_data)
            logger.info(f"Successfully inserted {len(all_data)} total records into main store")
        
            await init_metadata()
            logger.info("Metadata initialized successfully")
        return main_store
        
    except FileNotFoundError:
        logger.error("Configuration file not found")
        raise
    except json.JSONDecodeError:
        logger.error("Invalid JSON in configuration file")
        raise
    except Exception as e:
        logger.error(f"Setup failed: {str(e)}")
        raise

if __name__ == "__main__":
    store = asyncio.run(setup_store())
    logger.info("ETL process completed successfully")
    
   