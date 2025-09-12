"""
Volume Aggregator
==================

Agrégateur spécialisé pour les métriques de volume et de trading.
"""

from typing import List

from .base_aggregator import BaseAggregator, WindowData
from ..models.aggtrade_models import AggTradeRecord, AggTradeMetrics
from ..utils.logger import get_logger

logger = get_logger(__name__)


class VolumeWindowData(WindowData):
    """Données d'une fenêtre de volume."""
    
    def __init__(self, symbol: str, window_start: str):
        super().__init__(symbol, window_start)
        
        # Volumes
        self.total_volume: float = 0.0
        self.total_notional: float = 0.0
        self.buy_volume: float = 0.0
        self.sell_volume: float = 0.0
        
        # Compteurs de trades
        self.buy_count: int = 0
        self.sell_count: int = 0
        self.buyer_maker_count: int = 0
        self.seller_maker_count: int = 0
        
        # Distribution des tailles de trades
        self.trade_sizes: List[float] = []
        self.notional_values: List[float] = []
    
    def add_record(self, record: AggTradeRecord):
        """Ajoute un trade pour les métriques de volume."""
        self.count += 1
        
        # Volumes totaux
        self.total_volume += record.quantity
        self.total_notional += record.notional_value
        
        # Classification buy/sell
        if record.is_buy:
            self.buy_volume += record.quantity
            self.buy_count += 1
        else:
            self.sell_volume += record.quantity
            self.sell_count += 1
        
        # Classification maker
        if record.is_buyer_maker:
            self.buyer_maker_count += 1
        else:
            self.seller_maker_count += 1
        
        # Distribution des tailles
        self.trade_sizes.append(record.quantity)
        self.notional_values.append(record.notional_value)
    
    def finalize(self) -> AggTradeMetrics:
        """Finalise les métriques de volume."""
        if self.count == 0:
            return AggTradeMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Ratios de trading
        buy_pressure = self.buy_volume / self.total_volume if self.total_volume > 0 else 0.0
        sell_pressure = self.sell_volume / self.total_volume if self.total_volume > 0 else 0.0
        
        buyer_maker_ratio = self.buyer_maker_count / self.count if self.count > 0 else 0.0
        seller_maker_ratio = self.seller_maker_count / self.count if self.count > 0 else 0.0
        
        # Métriques moyennes
        avg_trade_size = self.total_volume / self.count if self.count > 0 else 0.0
        avg_notional = self.total_notional / self.count if self.count > 0 else 0.0
        
        return AggTradeMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            volume=self.total_volume,
            notional=self.total_notional,
            trade_count=self.count,
            buyer_maker_count=self.buyer_maker_count,
            seller_maker_count=self.seller_maker_count,
            buyer_volume=self.buy_volume,
            seller_volume=self.sell_volume,
            buy_pressure=buy_pressure,
            sell_pressure=sell_pressure,
            buyer_maker_ratio=buyer_maker_ratio,
            seller_maker_ratio=seller_maker_ratio,
        )


class VolumeAggregator(BaseAggregator[AggTradeRecord, AggTradeMetrics]):
    """Agrégateur spécialisé pour les métriques de volume."""
    
    def _create_window_data(self, symbol: str, window_key: str) -> VolumeWindowData:
        """Crée une nouvelle fenêtre de données pour les volumes."""
        return VolumeWindowData(symbol=symbol, window_start=window_key)
    
    def _should_process_record(self, record: AggTradeRecord) -> bool:
        """Détermine si un trade doit être traité pour les volumes."""
        return (
            record.symbol and 
            record.quantity > 0 and 
            record.timestamp > 0
        )
