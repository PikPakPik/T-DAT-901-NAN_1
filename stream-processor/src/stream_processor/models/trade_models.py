"""
Trade Data Models
=================

Modèles de données pour les trades et métriques agrégées.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime


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
    notional: float = 0.0
    trade_count: int = 0
    
    # Métriques de prix
    min_price: float = float('inf')
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
    first_trade_time: Optional[int] = None
    last_trade_time: Optional[int] = None
    trade_duration_ms: int = 0
    trades_per_second: float = 0.0
    
    # Données brutes pour calculs
    prices: List[float] = field(default_factory=list)
    quantities: List[float] = field(default_factory=list)
    price_changes: List[float] = field(default_factory=list)
    
    def add_trade(self, trade: TradeRecord):
        """Ajoute un trade aux métriques."""
        # Métriques de base
        self.volume += trade.quantity
        self.notional += trade.notional_value
        self.trade_count += 1
        
        # Métriques de prix
        self.min_price = min(self.min_price, trade.price)
        self.max_price = max(self.max_price, trade.price)
        self.prices.append(trade.price)
        self.quantities.append(trade.quantity)
        
        # Métriques d'ordre
        if trade.is_buyer_maker:
            self.buyer_maker_count += 1
            self.seller_volume += trade.quantity
        else:
            self.seller_maker_count += 1
            self.buyer_volume += trade.quantity
        
        # Métriques temporelles
        if self.first_trade_time is None:
            self.first_trade_time = trade.timestamp
        self.last_trade_time = trade.timestamp
        
        # Métriques de volatilité
        if len(self.prices) > 1:
            price_change = trade.price - self.prices[-2]
            self.price_changes.append(price_change)
    
    def finalize(self) -> 'AggregatedMetrics':
        """Finalise les métriques et retourne les résultats agrégés."""
        if self.trade_count == 0:
            return AggregatedMetrics(
                symbol=self.symbol,
                window_start=self.window_start,
                window_end=self.window_end
            )
        
        # Calcul des métriques finales
        self.avg_price = self.notional / self.volume if self.volume > 0 else 0.0
        self.vwap = self.notional / self.volume if self.volume > 0 else 0.0
        self.price_range = self.max_price - self.min_price
        
        # Calculs statistiques
        if self.prices:
            self.median_price = self._calculate_median(self.prices)
            self.price_std = self._calculate_std(self.prices)
        
        # Métriques d'ordre
        if self.volume > 0:
            self.buy_pressure = self.buyer_volume / self.volume
            self.sell_pressure = self.seller_volume / self.volume
        
        if self.trade_count > 0:
            self.buyer_maker_ratio = self.buyer_maker_count / self.trade_count
            self.seller_maker_ratio = self.seller_maker_count / self.trade_count
        
        # Métriques de volatilité
        if self.price_changes:
            self.price_volatility = self._calculate_std(self.price_changes)
            self.price_trend = sum(self.price_changes)
        
        # Métriques temporelles
        if self.first_trade_time and self.last_trade_time:
            self.trade_duration_ms = self.last_trade_time - self.first_trade_time
            self.trades_per_second = self.trade_count / (self.trade_duration_ms / 1000) if self.trade_duration_ms > 0 else 0.0
        
        return AggregatedMetrics(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            volume=self.volume,
            notional=self.notional,
            trade_count=self.trade_count,
            min_price=self.min_price if self.min_price != float('inf') else 0.0,
            max_price=self.max_price,
            avg_price=self.avg_price,
            median_price=self.median_price,
            price_std=self.price_std,
            price_range=self.price_range,
            vwap=self.vwap,
            buyer_maker_count=self.buyer_maker_count,
            seller_maker_count=self.seller_maker_count,
            buyer_volume=self.buyer_volume,
            seller_volume=self.seller_volume,
            buy_pressure=self.buy_pressure,
            sell_pressure=self.sell_pressure,
            buyer_maker_ratio=self.buyer_maker_ratio,
            seller_maker_ratio=self.seller_maker_ratio,
            price_volatility=self.price_volatility,
            price_trend=self.price_trend,
            trade_duration_ms=self.trade_duration_ms,
            trades_per_second=self.trades_per_second,
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


@dataclass
class AggregatedMetrics:
    """Métriques finales agrégées pour une fenêtre."""
    
    symbol: str
    window_start: str
    window_end: str
    
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
            "symbol": self.symbol,
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
