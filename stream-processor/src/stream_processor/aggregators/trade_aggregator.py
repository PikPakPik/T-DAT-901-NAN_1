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
        self.notional: float = 0.0
        
        # Métriques de prix
        self.min_price: float = float('inf')
        self.max_price: float = 0.0
        self.prices: List[float] = []
        self.quantities: List[float] = []
        self.price_changes: List[float] = []
        
        # Métriques d'ordre
        self.buyer_volume: float = 0.0
        self.seller_volume: float = 0.0
        
        # Métriques temporelles
        self.first_trade_time: Optional[int] = None
        self.last_trade_time: Optional[int] = None
    
    def add_record(self, record: TradeRecord):
        """Ajoute un trade à la fenêtre."""
        # Métriques de base
        self.volume += record.quantity
        self.notional += record.notional_value
        self.count += 1
        
        # Métriques de prix
        self.min_price = min(self.min_price, record.price)
        self.max_price = max(self.max_price, record.price)
        self.prices.append(record.price)
        self.quantities.append(record.quantity)
        
        # Métriques d'ordre
        if record.is_buyer_maker:
            self.seller_volume += record.quantity
        else:
            self.buyer_volume += record.quantity
        
        # Métriques temporelles
        if self.first_trade_time is None:
            self.first_trade_time = record.timestamp
        self.last_trade_time = record.timestamp
        
        # Métriques de volatilité
        if len(self.prices) > 1:
            price_change = record.price - self.prices[-2]
            self.price_changes.append(price_change)
    
    def finalize(self) -> TradeMetrics:
        """Finalise les métriques de trades."""
        if self.count == 0:
            return TradeMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Calcul des métriques finales
        avg_price = self.notional / self.volume if self.volume > 0 else 0.0
        price_range = self.max_price - self.min_price
        
        # Calculs statistiques
        median_price = self._calculate_median(self.prices) if self.prices else 0.0
        
        # Métriques d'ordre
        buyer_maker_ratio = self.buyer_volume / self.volume if self.volume > 0 else 0.0
        seller_maker_ratio = self.seller_volume / self.volume if self.volume > 0 else 0.0
        
        return TradeMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            volume=self.volume,
            notional=self.notional,
            trade_count=self.count,
            min_price=self.min_price if self.min_price != float('inf') else 0.0,
            max_price=self.max_price,
            avg_price=avg_price,
            median_price=median_price,
            price_range=price_range,
            buyer_volume=self.buyer_volume,
            seller_volume=self.seller_volume,
            buyer_maker_ratio=buyer_maker_ratio,
            seller_maker_ratio=seller_maker_ratio,
        )
    
    def _calculate_median(self, values: List[float]) -> float:
        """Calcule la médiane d'une liste de valeurs."""
        sorted_values = sorted(values)
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n//2 - 1] + sorted_values[n//2]) / 2
        else:
            return sorted_values[n//2]
    
    def _calculate_std(self, values: List[float]) -> float:
        """Calcule l'écart-type d'une liste de valeurs."""
        if len(values) <= 1:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5


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
