"""
Trade Data Models
=================

Modèles de données pour les trades et métriques agrégées.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List


@dataclass
class TradeRecord:
    """Représente un trade individuel."""
    
    symbol: str
    price: float
    quantity: float
    timestamp: int
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
class WindowMetrics:
    """Métriques pour une fenêtre temporelle."""
    
    symbol: str
    window_start: str
    window_end: str
    
    # Métriques de base
    volume: float = 0.0
    trade_count: int = 0
    
    # Métriques de prix
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    avg_price: float = 0.0
    
    # Métriques d'ordre
    buyer_volume: float = 0.0
    seller_volume: float = 0.0
    
    # Données brutes pour calculs
    prices: List[float] = field(default_factory=list)
    quantities: List[float] = field(default_factory=list)
    
    def add_trade(self, trade: TradeRecord):
        """Ajoute un trade aux métriques."""
        # Métriques de base
        self.volume += trade.quantity
        self.trade_count += 1
        
        # Métriques de prix - initialise avec le premier prix de la fenêtre
        if self.min_price is None:
            self.min_price = trade.price
            self.max_price = trade.price
        else:
            # Met à jour min/max uniquement pour cette fenêtre
            self.min_price = min(self.min_price, trade.price)
            self.max_price = max(self.max_price, trade.price)
        
        self.prices.append(trade.price)
        self.quantities.append(trade.quantity)
        
        # Métriques d'ordre
        if trade.is_buyer_maker:
            self.seller_volume += trade.quantity
        else:
            self.buyer_volume += trade.quantity
    
    def finalize(self) -> 'AggregatedMetrics':
        """Finalise les métriques et retourne les résultats agrégés."""
        if self.trade_count == 0:
            return AggregatedMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Calcul des métriques finales
        # avg_price = sum(price * quantity) / sum(quantity) = weighted average
        total_notional = sum(price * qty for price, qty in zip(self.prices, self.quantities)) if self.prices else 0.0
        self.avg_price = total_notional / self.volume if self.volume > 0 else 0.0
        
        return AggregatedMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            volume=self.volume,
            trade_count=self.trade_count,
            min_price=self.min_price if self.min_price is not None else 0.0,
            max_price=self.max_price if self.max_price is not None else 0.0,
            avg_price=self.avg_price,
            buyer_volume=self.buyer_volume,
            seller_volume=self.seller_volume,
        )


@dataclass
class AggregatedMetrics:
    """Métriques finales agrégées pour une fenêtre."""
    
    symbol: str
    window_start: str
    window_end: str
    
    # Métriques de base
    volume: float = 0.0
    trade_count: int = 0
    
    # Métriques de prix
    min_price: float = 0.0
    max_price: float = 0.0
    avg_price: float = 0.0
    
    # Métriques d'ordre
    buyer_volume: float = 0.0
    seller_volume: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour InfluxDB."""
        return {
            "symbol": self.symbol,
            "window": self.window_start,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "avg_price": self.avg_price,
            "buyer_volume": self.buyer_volume,
            "seller_volume": self.seller_volume,
        }
