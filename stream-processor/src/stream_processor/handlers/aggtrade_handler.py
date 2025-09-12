"""
AggTrade Handler
=================

Handler spécialisé pour le topic aggTrade (trades agrégés).
"""

from typing import Dict, Any, Optional

from .base_handler import BaseHandler
from ..models.aggtrade_models import AggTradeRecord, AggTradeMetrics
from ..aggregators.trade_aggregator import TradeAggregator
from ..aggregators.price_aggregator import PriceAggregator
from ..aggregators.volume_aggregator import VolumeAggregator
from ..utils.logger import get_logger

logger = get_logger(__name__)


class AggTradeHandler(BaseHandler[AggTradeRecord, AggTradeMetrics]):
    """Handler spécialisé pour les trades agrégés."""
    
    def __init__(self, window_seconds: int = 1, max_memory_windows: int = 1000):
        """
        Initialise le handler AggTrade.
        
        Args:
            window_seconds: Taille des fenêtres temporelles
            max_memory_windows: Nombre maximum de fenêtres en mémoire
        """
        super().__init__("aggTrade")
        
        # Initialise les agrégateurs spécialisés
        self._init_aggregators(window_seconds, max_memory_windows)
        
        logger.info(f"AggTradeHandler initialized with {len(self.aggregators)} aggregators")
    
    def _init_aggregators(self, window_seconds: int, max_memory_windows: int):
        """Initialise tous les agrégateurs pour les trades."""
        # Agrégateur complet de trades
        trade_agg = TradeAggregator(
            window_seconds=window_seconds,
            max_memory_windows=max_memory_windows
        )
        self.add_aggregator(trade_agg)
        
        # Agrégateur spécialisé pour les prix
        price_agg = PriceAggregator(
            window_seconds=window_seconds,
            max_memory_windows=max_memory_windows
        )
        self.add_aggregator(price_agg)
        
        # Agrégateur spécialisé pour les volumes
        volume_agg = VolumeAggregator(
            window_seconds=window_seconds,
            max_memory_windows=max_memory_windows
        )
        self.add_aggregator(volume_agg)
    
    def _is_valid_message(self, message: Dict[str, Any]) -> bool:
        """Vérifie si un message aggTrade est valide."""
        if not message.get("value"):
            return False
        
        value = message["value"]
        required_fields = ["p", "q", "T", "s"]  # price, quantity, timestamp, symbol
        
        return all(field in value for field in required_fields)
    
    def _create_record_from_message(self, message: Dict[str, Any]) -> Optional[AggTradeRecord]:
        """Crée un AggTradeRecord depuis un message Kafka."""
        try:
            return AggTradeRecord.from_dict(message["value"])
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Failed to create AggTradeRecord: {e}")
            return None
    
    def get_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des performances du handler."""
        stats = self.get_stats()
        
        return {
            "handler": "AggTradeHandler",
            "topic": self.topic_name,
            "total_messages": stats["messages_processed"],
            "valid_records": stats["records_created"],
            "metrics_generated": stats["metrics_generated"],
            "active_windows": self.get_active_windows_count(),
            "aggregators": [agg.__class__.__name__ for agg in self.aggregators],
            "success_rate": (
                stats["records_created"] / max(stats["messages_processed"], 1) * 100
                if stats["messages_processed"] > 0 else 0
            )
        }
