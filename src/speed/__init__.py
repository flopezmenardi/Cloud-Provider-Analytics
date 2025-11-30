"""
Speed Layer module for real-time streaming processing.
Implements Lambda Architecture speed layer with windowed aggregations.
"""

from src.speed.streaming_aggregations import (
    StreamingAggregator,
    run_streaming_aggregations
)
from src.speed.streaming_enrichment import (
    StreamingEnricher,
    run_streaming_enrichment
)

__all__ = [
    "StreamingAggregator",
    "run_streaming_aggregations",
    "StreamingEnricher",
    "run_streaming_enrichment"
]


