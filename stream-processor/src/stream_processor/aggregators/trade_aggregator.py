"""
Trade Aggregator
=================

Agrégateur spécialisé pour les données de trades (trade topic).
"""

from typing import List, Optional

from .base_aggregator import BaseAggregator, WindowData
from ..models.aggtrade_models import TradeRecord, TradeMetrics
from ..utils.logger import get_logger

logger = get_logger(__name__)


class TradeWindowData(WindowData):
    """Données d'une fenêtre de trades."""
    
    def __init__(self, symbol: str, window_start: str):
        super().__init__(symbol, window_start)
        
        # Métriques de base
        self.volume: float = 0.0
        
        # Métriques de prix
        self.min_price: Optional[float] = None
        self.max_price: Optional[float] = None
        self.prices: List[float] = []
        self.quantities: List[float] = []
        
        # Métriques d'ordre
        self.buyer_volume: float = 0.0
        self.seller_volume: float = 0.0
    
    def add_record(self, record: TradeRecord):
        """Ajoute un trade à la fenêtre."""
        # Métriques de base
        self.volume += record.quantity
        self.count += 1
        
        # Métriques de prix - initialise avec le premier prix de la fenêtre
        if self.min_price is None:
            self.min_price = record.price
            self.max_price = record.price
        else:
            # Met à jour min/max uniquement pour cette fenêtre
            self.min_price = min(self.min_price, record.price)
            self.max_price = max(self.max_price, record.price)
        
        self.prices.append(record.price)
        self.quantities.append(record.quantity)
        
        # Métriques d'ordre
        if record.is_buyer_maker:
            self.seller_volume += record.quantity
        else:
            self.buyer_volume += record.quantity
    
    def finalize(self) -> TradeMetrics:
        """Finalise les métriques de trades."""
        if self.count == 0:
            return TradeMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Calcul des métriques finales
        # avg_price = sum(price * quantity) / sum(quantity) = weighted average
        total_notional = sum(price * qty for price, qty in zip(self.prices, self.quantities)) if self.prices else 0.0
        avg_price = total_notional / self.volume if self.volume > 0 else 0.0
        
        return TradeMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            volume=self.volume,
            trade_count=self.count,
            min_price=self.min_price if self.min_price is not None else 0.0,
            max_price=self.max_price if self.max_price is not None else 0.0,
            avg_price=avg_price,
            buyer_volume=self.buyer_volume,
            seller_volume=self.seller_volume,
        )


class TradeAggregator(BaseAggregator[TradeRecord, TradeMetrics]):
    """Agrégateur optimisé pour les trades."""
    
    def _create_window_data(self, symbol: str, window_key: str) -> TradeWindowData:
        """Crée une nouvelle fenêtre de données pour les trades."""
        return TradeWindowData(symbol=symbol, window_start=window_key)
    
    def _should_process_record(self, record: TradeRecord) -> bool:
        """Détermine si un trade doit être traité."""
        # Vérifie la validité des données du trade
        return (
            record.symbol and 
            record.price > 0 and 
            record.quantity > 0 and 
            record.timestamp > 0
        )
