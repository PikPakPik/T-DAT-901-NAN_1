"""Models package for all data structures."""

# Import des modèles de base
from .base_models import BaseRecord, BaseMetrics

# Import des modèles spécialisés par topic
from .aggtrade_models import TradeRecord, TradeMetrics
from .kline_models import KlineRecord, KlineMetrics

# Import des anciens modèles pour compatibilité
from .trade_models import WindowMetrics, AggregatedMetrics

__all__ = [
    # Modèles de base
    'BaseRecord', 'BaseMetrics',
    # Modèles trade
    'TradeRecord', 'TradeMetrics', 
    # Modèles kline
    'KlineRecord', 'KlineMetrics',
    # Anciens modèles (compatibility)
    'WindowMetrics', 'AggregatedMetrics'
]
