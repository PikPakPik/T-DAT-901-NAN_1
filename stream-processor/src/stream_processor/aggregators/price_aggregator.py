"""
Price Aggregator
================

Agrégateur spécialisé pour les métriques de prix uniquement.
"""

from typing import List, Optional

from .base_aggregator import BaseAggregator, WindowData
from ..models.aggtrade_models import TradeRecord, TradeMetrics
from ..utils.logger import get_logger

logger = get_logger(__name__)


class PriceWindowData(WindowData):
    """Données d'une fenêtre de prix."""
    
    def __init__(self, symbol: str, window_start: str):
        super().__init__(symbol, window_start)
        
        # Prix OHLC
        self.open_price: Optional[float] = None
        self.high_price: Optional[float] = None
        self.low_price: Optional[float] = None
        self.close_price: Optional[float] = None
        
        # Historique des prix
        self.prices: List[float] = []
    
    def add_record(self, record: TradeRecord):
        """Ajoute un trade pour les métriques de prix."""
        self.count += 1
        
        # Prix OHLC - initialise avec le premier prix de la fenêtre
        if self.open_price is None:
            self.open_price = record.price
            self.high_price = record.price
            self.low_price = record.price
        else:
            # Met à jour min/max uniquement pour cette fenêtre
            self.high_price = max(self.high_price, record.price)
            self.low_price = min(self.low_price, record.price)
        
        self.close_price = record.price
        
        # Historique
        self.prices.append(record.price)
    
    def finalize(self) -> TradeMetrics:
        """Finalise les métriques de prix."""
        if self.count == 0:
            return TradeMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Calculs de prix
        avg_price = sum(self.prices) / len(self.prices) if self.prices else 0.0
        
        return TradeMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            trade_count=self.count,
            min_price=self.low_price if self.low_price is not None else 0.0,
            max_price=self.high_price if self.high_price is not None else 0.0,
            avg_price=avg_price,
        )


class PriceAggregator(BaseAggregator[TradeRecord, TradeMetrics]):
    """Agrégateur spécialisé pour les métriques de prix."""
    
    def _create_window_data(self, symbol: str, window_key: str) -> PriceWindowData:
        """Crée une nouvelle fenêtre de données pour les prix."""
        return PriceWindowData(symbol=symbol, window_start=window_key)
    
    def _should_process_record(self, record: TradeRecord) -> bool:
        """Détermine si un trade doit être traité pour les prix."""
        return (
            record.symbol and 
            record.price > 0 and 
            record.timestamp > 0
        )
