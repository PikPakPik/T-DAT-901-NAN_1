"""
Kline Models
=============

Modèles spécifiques pour les données de klines/candlesticks (kline topic).
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from .base_models import BaseRecord, BaseMetrics


@dataclass
class KlineRecord(BaseRecord):
    """Représente une kline/candlestick."""
    
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    open_time: int
    close_time: int
    interval: str = "1s"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'KlineRecord':
        """Crée un KlineRecord depuis un dictionnaire."""
        kline_data = data.get("k", {})
        return cls(
            symbol=kline_data.get("s", ""),
            timestamp=int(kline_data.get("T", 0)),
            open_price=float(kline_data.get("o", 0)),
            high_price=float(kline_data.get("h", 0)),
            low_price=float(kline_data.get("l", 0)),
            close_price=float(kline_data.get("c", 0)),
            volume=float(kline_data.get("v", 0)),
            quote_asset_volume=float(kline_data.get("q", 0)),
            number_of_trades=int(kline_data.get("n", 0)),
            taker_buy_base_asset_volume=float(kline_data.get("V", 0)),
            taker_buy_quote_asset_volume=float(kline_data.get("Q", 0)),
            open_time=int(kline_data.get("t", 0)),
            close_time=int(kline_data.get("T", 0)),
            interval=kline_data.get("i", "1s"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "volume": self.volume,
            "quote_asset_volume": self.quote_asset_volume,
            "number_of_trades": self.number_of_trades,
            "taker_buy_base_asset_volume": self.taker_buy_base_asset_volume,
            "taker_buy_quote_asset_volume": self.taker_buy_quote_asset_volume,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "interval": self.interval,
        }
    
    @property
    def price_change(self) -> float:
        """Changement de prix (close - open)."""
        return self.close_price - self.open_price
    
    @property
    def price_change_percent(self) -> float:
        """Changement de prix en pourcentage."""
        return (self.price_change / self.open_price * 100) if self.open_price > 0 else 0.0
    
    @property
    def is_green(self) -> bool:
        """True si la kline est verte (close > open)."""
        return self.close_price > self.open_price
    
    @property
    def is_red(self) -> bool:
        """True si la kline est rouge (close < open)."""
        return self.close_price < self.open_price


@dataclass
class KlineMetrics(BaseMetrics):
    """Métriques finales pour les klines."""
    
    # Métriques OHLC
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    
    # Métriques de volume
    volume: float = 0.0
    quote_asset_volume: float = 0.0
    number_of_trades: int = 0
    taker_buy_base_asset_volume: float = 0.0
    taker_buy_quote_asset_volume: float = 0.0
    
    # Métriques calculées
    price_change: float = 0.0
    price_change_percent: float = 0.0
    price_range: float = 0.0
    avg_price: float = 0.0
    vwap: float = 0.0
    
    # Métriques de trading
    buy_sell_ratio: float = 0.0
    taker_buy_ratio: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour InfluxDB."""
        return {
            "measurement": "kline_metrics",
            "tags": {
                "symbol": self.symbol,
            },
            "fields": {
                "window": self.window_start,
                "open_price": self.open_price,
                "high_price": self.high_price,
                "low_price": self.low_price,
                "close_price": self.close_price,
                "volume": self.volume,
                "quote_asset_volume": self.quote_asset_volume,
                "number_of_trades": self.number_of_trades,
                "taker_buy_base_asset_volume": self.taker_buy_base_asset_volume,
                "taker_buy_quote_asset_volume": self.taker_buy_quote_asset_volume,
                "price_change": self.price_change,
                "price_change_percent": self.price_change_percent,
                "price_range": self.price_range,
                "avg_price": self.avg_price,
                "vwap": self.vwap,
                "buy_sell_ratio": self.buy_sell_ratio,
                "taker_buy_ratio": self.taker_buy_ratio,
            }
        }
