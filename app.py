import os
import json
from typing import Dict, Any, List, TypeVar
import asyncio
from nl2query import TSQuery
import sys
import structlog

logger = structlog.get_logger(__name__)



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

async def main():
    engine = TSQuery()
    question = sys.argv[1] if len(sys.argv) > 1 else "What is the total value locked on Base since 2025-01-01?"
    print(question)
    response = await engine.get_response(question)
    # response = await engine.generate_query(question)
    print(response)

if __name__ == "__main__":
    asyncio.run(main())