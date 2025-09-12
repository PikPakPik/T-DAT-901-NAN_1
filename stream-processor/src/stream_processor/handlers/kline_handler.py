"""
Kline Handler
==============

Handler spécialisé pour le topic kline (candlesticks).
"""

from typing import Dict, Any, Optional

from .base_handler import BaseHandler
from ..models.kline_models import KlineRecord, KlineMetrics
from ..utils.logger import get_logger

logger = get_logger(__name__)


class KlineHandler(BaseHandler[KlineRecord, KlineMetrics]):
    """Handler spécialisé pour les klines/candlesticks."""
    
    def __init__(self, window_seconds: int = 1, max_memory_windows: int = 1000):
        """
        Initialise le handler Kline.
        
        Args:
            window_seconds: Taille des fenêtres temporelles
            max_memory_windows: Nombre maximum de fenêtres en mémoire
        """
        super().__init__("kline")
        
        # TODO: Initialiser les agrégateurs spécifiques aux klines
        # Pour l'instant, on garde la structure de base
        
        logger.info(f"KlineHandler initialized (TODO: add kline-specific aggregators)")
    
    def _is_valid_message(self, message: Dict[str, Any]) -> bool:
        """Vérifie si un message kline est valide."""
        if not message.get("value"):
            return False
        
        value = message["value"]
        kline_data = value.get("k")
        if not kline_data:
            return False
        
        required_fields = ["s", "t", "T", "o", "h", "l", "c", "v"]  # symbol, times, OHLCV
        
        return all(field in kline_data for field in required_fields)
    
    def _create_record_from_message(self, message: Dict[str, Any]) -> Optional[KlineRecord]:
        """Crée un KlineRecord depuis un message Kafka."""
        try:
            return KlineRecord.from_dict(message["value"])
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Failed to create KlineRecord: {e}")
            return None
    
    def get_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des performances du handler."""
        stats = self.get_stats()
        
        return {
            "handler": "KlineHandler",
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
