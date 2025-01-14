import asyncio
import json
import logging
import os
from typing import Dict, List
from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient

from nl2query import METADATA_EXAMPLE, TSQuery

# Initialize logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_raw_data(file_uri: str) -> List[Dict]:
    """Load data from raw JSON file, handling both file:// URIs and regular paths"""
    try:
        # Parse the URI
        parsed = urlparse(file_uri)

        # Convert URI to local path
        if parsed.scheme == "file":
            # Remove 'file://' and handle potential triple slashes
            file_path = os.path.abspath(parsed.path)
        else:
            file_path = file_uri

        # Read the file
        with open(file_path, "r") as f:
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
        main_uri = os.getenv("MAIN_STORE_URI", "mongodb://localhost:27017/")
        client = AsyncIOMotorClient(main_uri)
        db = client["metrics_store"]
        metadata_collection = db["metric_metadata"]

        # todo let LLM to generate metadata , with source is dynamic added once different sources are added
        metadata = METADATA_EXAMPLE

        # Create indexes
        await metadata_collection.create_index("name")
        await metadata_collection.create_index("sources")

        existing = await metadata_collection.find_one({"name": metadata[0]["name"]})
        if not existing:
            result = await metadata_collection.insert_many(metadata)
            logger.debug(
                f"Metadata initialized successfully: {len(result.inserted_ids)} documents"
            )
        else:
            logger.debug("Metadata already exists")

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
        with open("config.file.json", "r") as f:
            raw_configs = json.load(f)

        # Initialize TSQuery engine
        query_engine = TSQuery()  # Replace MetricStore initialization

        # Process each raw data source
        all_data = []
        for config in raw_configs:
            source = config["source"]
            raw_data = load_raw_data(config["uri"])
            all_data.extend(raw_data)
            logger.debug(f"Processed {len(raw_data)} records from {source}")

        # Insert all data using TSQuery
        if all_data:
            # Assuming TSQuery has a method for inserting data
            await query_engine.insert_metrics(
                all_data
            )  # Update method name as per TSQuery implementation
            logger.info(
                f"Successfully inserted {len(all_data)} total records into main store"
            )

            await init_metadata()
            logger.info("Metadata initialized successfully")
        return query_engine

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
