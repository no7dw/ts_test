import json
from appm import MetricStore
import asyncio
import logging

# Initialize logger at the module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def setup_store():
    """Initialize and setup the metric store"""
    try:
        # Load configurations
        with open('config.json', 'r') as f:
            configs = json.load(f)

        stores = []
        for config in configs:
            # Initialize store with configuration
            store = MetricStore(config['mongodb_uri'])
            
            # One-time setup operations
            await store.init_metadata()  # Initialize metadata structure
            await store.init_indexes()   # Create necessary indexes
            
            # Load and insert data
            data = await store.load_data(config['table_path'])
            if data:
                await store.insert_metrics(data)
                logger.info(f"Successfully loaded and inserted {len(data)} records from {config['table_path']}")
            
            stores.append(store)
            
        return stores
        
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
    stores = asyncio.run(setup_store())
    logger.info(f"Successfully initialized {len(stores)} stores")
    
   