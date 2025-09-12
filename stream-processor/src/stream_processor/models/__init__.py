"""Models package for all data structures."""

# Import des modèles de base
from .base_models import BaseRecord, BaseMetrics

# Import des modèles spécialisés par topic
from .aggtrade_models import AggTradeRecord, AggTradeMetrics
from .kline_models import KlineRecord, KlineMetrics

# Import des anciens modèles pour compatibilité
from .trade_models import TradeRecord, WindowMetrics, AggregatedMetrics

__all__ = [
    # Modèles de base
    'BaseRecord', 'BaseMetrics',
    # Modèles aggTrade
    'AggTradeRecord', 'AggTradeMetrics', 
    # Modèles kline
    'KlineRecord', 'KlineMetrics',
    # Anciens modèles (compatibility)
    'TradeRecord', 'WindowMetrics', 'AggregatedMetrics'
]
