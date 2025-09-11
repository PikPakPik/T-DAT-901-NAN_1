"""Core processing package for Stream Processor."""

from .processor import StreamProcessor
from .aggregator import TradeAggregator

__all__ = ["StreamProcessor", "TradeAggregator"]
