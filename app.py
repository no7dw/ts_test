import asyncio
import json
import os
import sys
from typing import Dict, List

import structlog

from nl2query import NL2QueryEngine

logger = structlog.get_logger(__name__)


def load_data(self, file_path: str) -> List[Dict]:
    """Load metric data from JSON file"""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")

        with open(file_path, "r") as file:
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
    engine = NL2QueryEngine()
    default_questions = [
        "what is wallet CWvdyvKHEu8Z6QqGraJT3sLPyp9bJfFhoXcxUYRKC8ou realized profit",
        "what is wallet CWvdyvKHEu8Z6QqGraJT3sLPyp9bJfFhoXcxUYRKC8ou pnl",
        "what is the wallet CWvdyvKHEu8Z6QqGraJT3sLPyp9bJfFhoXcxUYRKC8ou profit",
        "what is 45yBcpnzFTqLYQJtjxsa1DdZkgrTYponCg6yLQ6LQPu6 profit",
        # "what is Optimism tvl",
        # "what is Base tvl since 2025-01-01",
    ]
    input_questions = sys.argv[1] if len(sys.argv) > 1 else default_questions
    if isinstance(input_questions, str):
        input_questions = [input_questions]
    for question in input_questions:
        response = await engine.get_response(question)
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
