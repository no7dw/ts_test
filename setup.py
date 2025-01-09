import json
from appm import MetricStore
import asyncio


async def setup_store():
    """Initialize and setup the metric store"""
    try:
        # Load configuration
        with open('config.json', 'r') as f:
            config = json.load(f)

        # Initialize store with configuration
        store = MetricStore(config['mongodb_uri'])
        
        # One-time setup operations
        await store.init_metadata()  # Initialize metadata structure
        await store.init_indexes()   # Create necessary indexes
        
        # Load and insert data
        data = await store.load_data(config['data_path'])
        await store.insert_metrics(data)
        
        return store
        
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        raise

if __name__ == "__main__":
    import asyncio
    store = asyncio.run(setup_store())
    
   