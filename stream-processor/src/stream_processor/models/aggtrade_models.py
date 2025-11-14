"""
trade Models
================

Modèles spécifiques pour les données de trades agrégés (trade topic).
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from .base_models import BaseRecord, BaseMetrics


@dataclass
class TradeRecord(BaseRecord):
    """Représente un trade agrégé individuel."""
    
    price: float
    quantity: float
    is_buyer_maker: bool = False
    trade_id: Optional[int] = None
    order_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TradeRecord':
        """Crée un TradeRecord depuis un dictionnaire."""
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
class TradeMetrics(BaseMetrics):
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
    price_range: float = 0.0
    
    # Métriques d'ordre
    buyer_volume: float = 0.0
    seller_volume: float = 0.0
    buyer_maker_ratio: float = 0.0
    seller_maker_ratio: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour InfluxDB."""
        return {
            "measurement": "trade_metrics",
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
                "price_range": self.price_range,
                "buyer_volume": self.buyer_volume,
                "seller_volume": self.seller_volume,
                "buyer_maker_ratio": self.buyer_maker_ratio,
                "seller_maker_ratio": self.seller_maker_ratio,
            }
        }
