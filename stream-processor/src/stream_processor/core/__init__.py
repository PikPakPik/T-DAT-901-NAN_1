"""Core processing package for Stream Processor."""

# Nouvelle architecture modulaire
from .stream_orchestrator import StreamOrchestrator
from .topic_router import TopicRouter

# Anciens modules (compatibility)
from .processor import StreamProcessor
from .aggregator import TradeAggregator

__all__ = [
    # Nouvelle architecture
    "StreamOrchestrator", "TopicRouter",
    # Anciens modules (compatibility)
    "StreamProcessor", "TradeAggregator"
]
