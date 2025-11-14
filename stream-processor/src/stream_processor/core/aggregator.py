"""
Trade Aggregator Core
=====================

Agrégateur de trades avec fenêtrage temporel et calcul de métriques avancées.
"""

import time
from typing import Dict, Optional, List
from collections import defaultdict

from ..utils.logger import get_logger
from ..utils.helpers import iso_floor_to_second, get_current_timestamp
from ..models.trade_models import TradeRecord, WindowMetrics, AggregatedMetrics

logger = get_logger(__name__)


class TradeAggregator:
    """Agrégateur de trades avec métriques avancées et gestion mémoire optimisée."""
    
    def __init__(self, window_seconds: int = 1, max_memory_windows: int = 1000):
        """
        Initialise l'agrégateur de trades.
        
        Args:
            window_seconds: Taille des fenêtres temporelles en secondes (toujours 1s pour InfluxDB)
            max_memory_windows: Nombre maximum de fenêtres en mémoire
        """
        # Force window_seconds=1 car InfluxDB gère l'agrégation temporelle via ses requêtes
        self.window_seconds = 1
        self.max_memory_windows = max_memory_windows
        self.windows: Dict[str, WindowMetrics] = {}
        
        # Statistiques
        self.stats = {
            "processed_count": 0,
            "last_cleanup": get_current_timestamp(),
            "windows_created": 0,
            "windows_finalized": 0,
        }
        
        logger.info(f"TradeAggregator initialized with 1s windows (InfluxDB handles temporal aggregation), max {max_memory_windows} in memory")
    
    def process_trade(self, trade: TradeRecord) -> Optional[AggregatedMetrics]:
        """
        Traite un trade et retourne les métriques si la fenêtre est complète.
        
        Args:
            trade: Trade à traiter
            
        Returns:
            Métriques agrégées si la fenêtre est complète, None sinon
        """
        try:
            # Toujours utiliser des fenêtres de 1 seconde - InfluxDB gère l'agrégation temporelle
            window_key = iso_floor_to_second(trade.timestamp)
            trade_key = f"{trade.symbol}:{window_key}"
            
            # Initialise la fenêtre si nécessaire
            if trade_key not in self.windows:
                self._create_window(trade_key, trade.symbol, window_key)
            
            window = self.windows[trade_key]
            window.add_trade(trade)
            
            self.stats["processed_count"] += 1
            
            # Vérifie si la fenêtre est complète (nouvelle seconde)
            current_window = iso_floor_to_second(get_current_timestamp())
            if window_key < current_window:
                return self._finalize_window(trade_key)
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing trade: {e}")
            return None
    
    def _create_window(self, trade_key: str, symbol: str, window_key: str):
        """Crée une nouvelle fenêtre d'agrégation."""
        self.windows[trade_key] = WindowMetrics(
            symbol=symbol,
            window_start=window_key,
            window_end=window_key
        )
        self.stats["windows_created"] += 1
        logger.debug(f"Created window for {trade_key}")
    
    def _finalize_window(self, trade_key: str) -> Optional[AggregatedMetrics]:
        """Finalise une fenêtre et retourne les métriques."""
        if trade_key not in self.windows:
            return None
        
        window = self.windows[trade_key]
        if window.trade_count == 0:
            del self.windows[trade_key]
            return None
        
        # Finalise les métriques
        metrics = window.finalize()
        
        # Cleanup
        del self.windows[trade_key]
        self.stats["windows_finalized"] += 1
        
        logger.debug(f"Finalized window for {trade_key}: {metrics.trade_count} trades")
        return metrics
    
    def force_finalize_current_windows(self) -> List[AggregatedMetrics]:
        """Force la finalisation des fenêtres en cours."""
        current_time = get_current_timestamp()
        current_window = iso_floor_to_second(current_time)
        
        completed_metrics = []
        keys_to_remove = []
        
        for trade_key, window in self.windows.items():
            # Finalise les fenêtres qui sont dans la seconde précédente
            if window.window_start < current_window and window.trade_count > 0:
                metrics = window.finalize()
                if metrics:
                    completed_metrics.append(metrics)
                keys_to_remove.append(trade_key)
        
        # Nettoie les fenêtres finalisées
        for key in keys_to_remove:
            if key in self.windows:
                del self.windows[key]
                self.stats["windows_finalized"] += 1
        
        if completed_metrics:
            logger.info(f"⏰ Forced finalization of {len(completed_metrics)} windows")
        
        return completed_metrics
    
    def cleanup_old_windows(self):
        """Nettoie les anciennes fenêtres pour libérer la mémoire."""
        current_time = get_current_timestamp()
        current_window = iso_floor_to_second(current_time)
        
        keys_to_remove = []
        for trade_key, window in self.windows.items():
            # Supprime les fenêtres très anciennes (plus de 5 minutes)
            if window.window_start < current_window:
                keys_to_remove.append(trade_key)
        
        for key in keys_to_remove:
            if key in self.windows:
                del self.windows[key]
        
        if keys_to_remove:
            logger.info(f"🧹 Cleaned up {len(keys_to_remove)} old windows")
        
        # Force le cleanup si trop de fenêtres en mémoire
        if len(self.windows) > self.max_memory_windows:
            self._force_memory_cleanup()
    
    def _force_memory_cleanup(self):
        """Force le nettoyage mémoire si nécessaire."""
        # Garde seulement les fenêtres les plus récentes
        sorted_windows = sorted(
            self.windows.items(),
            key=lambda x: x[1].window_start,
            reverse=True
        )
        
        # Garde seulement la moitié des fenêtres
        keep_count = self.max_memory_windows // 2
        windows_to_remove = sorted_windows[keep_count:]
        
        for trade_key, _ in windows_to_remove:
            if trade_key in self.windows:
                del self.windows[trade_key]
        
        logger.warning(f"🧹 Force memory cleanup: removed {len(windows_to_remove)} windows")
    
    def get_active_windows_count(self) -> int:
        """Retourne le nombre de fenêtres actives."""
        return len(self.windows)
    
    def get_stats(self) -> Dict[str, any]:
        """Retourne les statistiques de l'agrégateur."""
        return {
            "processed_count": self.stats["processed_count"],
            "active_windows": len(self.windows),
            "windows_created": self.stats["windows_created"],
            "windows_finalized": self.stats["windows_finalized"],
            "memory_usage_windows": len(self.windows),
            "max_memory_windows": self.max_memory_windows,
        }
    
    def get_window_info(self, symbol: str) -> List[Dict[str, any]]:
        """Retourne les informations sur les fenêtres d'un symbole."""
        symbol_windows = []
        for trade_key, window in self.windows.items():
            if window.symbol == symbol:
                symbol_windows.append({
                    "window_start": window.window_start,
                    "trade_count": window.trade_count,
                    "volume": window.volume,
                })
        
        return symbol_windows
    
    def reset_stats(self):
        """Remet à zéro les statistiques."""
        self.stats = {
            "processed_count": 0,
            "last_cleanup": get_current_timestamp(),
            "windows_created": 0,
            "windows_finalized": 0,
        }
        logger.info("📊 Aggregator stats reset")
    
    def __repr__(self) -> str:
        """Représentation string de l'agrégateur."""
        return f"TradeAggregator(windows={len(self.windows)}, processed={self.stats['processed_count']})"
