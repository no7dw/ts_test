from enum import Enum
from slugify import slugify

class MetricNameFormat(Enum):
    RAW = "raw"
    TITLE = "title"
    UPPER = "upper"
    SLUG = "slug"

def format_metric_name(name: str, format_style: MetricNameFormat = MetricNameFormat.RAW) -> str:
    if format_style == MetricNameFormat.RAW:
        return name
    elif format_style == MetricNameFormat.TITLE:
        return name.replace('_', ' ').title()
    elif format_style == MetricNameFormat.UPPER:
        return name.upper()
    elif format_style == MetricNameFormat.SLUG:
        return slugify(name)
    return name 