from datetime import datetime
import structlog
from typing import Optional
logger = structlog.get_logger(__name__)

def convert_iso_to_timestamp(iso_date_str: str) -> Optional[int]:
    """Convert ISO date string to Unix timestamp"""
    try:
        dt = datetime.fromisoformat(iso_date_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception as e:
        logger.error(f"Failed to convert date: {str(e)}")
        return None