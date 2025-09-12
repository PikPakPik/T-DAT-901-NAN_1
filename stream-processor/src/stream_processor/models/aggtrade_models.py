"""
AggTrade Models
================

Modèles spécifiques pour les données de trades agrégés (aggTrade topic).
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from .base_models import BaseRecord, BaseMetrics


@dataclass
class AggTradeRecord(BaseRecord):
    """Représente un trade agrégé individuel."""
    
    price: float
    quantity: float
    is_buyer_maker: bool = False
    trade_id: Optional[int] = None
    order_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AggTradeRecord':
        """Crée un AggTradeRecord depuis un dictionnaire."""
        return cls(
            symbol=data.get("s", ""),
            price=float(data.get("p", 0)),
            quantity=float(data.get("q", 0)),
            timestamp=int(data.get("T", 0)),
            is_buyer_maker=data.get("m", False),
            trade_id=data.get("t"),
            order_id=data.get("orderId"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "timestamp": self.timestamp,
            "is_buyer_maker": self.is_buyer_maker,
            "trade_id": self.trade_id,
            "order_id": self.order_id,
        }
    
    @property
    def notional_value(self) -> float:
        """Valeur notionnelle du trade."""
        return self.price * self.quantity
    
    @property
    def is_buy(self) -> bool:
        """True si c'est un achat (buyer n'est pas maker)."""
        return not self.is_buyer_maker
    
    @property
    def is_sell(self) -> bool:
        """True si c'est une vente (buyer est maker)."""
        return self.is_buyer_maker


@dataclass
class AggTradeMetrics(BaseMetrics):
    """Métriques finales pour les trades agrégés."""
    
    # Métriques de base
    volume: float = 0.0
    notional: float = 0.0
    trade_count: int = 0
    
    # Métriques de prix
    min_price: float = 0.0
    max_price: float = 0.0
    avg_price: float = 0.0
    median_price: float = 0.0
    price_std: float = 0.0
    price_range: float = 0.0
    vwap: float = 0.0
    
    # Métriques d'ordre
    buyer_maker_count: int = 0
    seller_maker_count: int = 0
    buyer_volume: float = 0.0
    seller_volume: float = 0.0
    buy_pressure: float = 0.0
    sell_pressure: float = 0.0
    buyer_maker_ratio: float = 0.0
    seller_maker_ratio: float = 0.0
    
    # Métriques de volatilité
    price_volatility: float = 0.0
    price_trend: float = 0.0
    
    # Métriques temporelles
    trade_duration_ms: int = 0
    trades_per_second: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour InfluxDB."""
        return {
            "measurement": "aggtrade_metrics",
            "tags": {
                "symbol": self.symbol,
            },
            "fields": {
                "window": self.window_start,
                "volume": self.volume,
                "notional": self.notional,
                "trade_count": self.trade_count,
                "min_price": self.min_price,
                "max_price": self.max_price,
                "avg_price": self.avg_price,
                "median_price": self.median_price,
                "price_std": self.price_std,
                "price_range": self.price_range,
                "vwap": self.vwap,
                "buyer_maker_count": self.buyer_maker_count,
                "seller_maker_count": self.seller_maker_count,
                "buyer_volume": self.buyer_volume,
                "seller_volume": self.seller_volume,
                "buy_pressure": self.buy_pressure,
                "sell_pressure": self.sell_pressure,
                "buyer_maker_ratio": self.buyer_maker_ratio,
                "seller_maker_ratio": self.seller_maker_ratio,
                "price_volatility": self.price_volatility,
                "price_trend": self.price_trend,
                "trade_duration_ms": self.trade_duration_ms,
                "trades_per_second": self.trades_per_second,
            }
        }
