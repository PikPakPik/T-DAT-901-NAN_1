"""
Base Aggregator
================

Classe de base pour tous les agrégateurs avec fenêtrage temporel.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, TypeVar, Generic
import time

from ..utils.logger import get_logger
from ..utils.helpers import iso_floor_to_second, get_current_timestamp
from ..models.base_models import BaseRecord, BaseMetrics

logger = get_logger(__name__)

T = TypeVar('T', bound=BaseRecord)
M = TypeVar('M', bound=BaseMetrics)


class BaseAggregator(Generic[T, M], ABC):
    """Classe de base pour tous les agrégateurs de données."""
    
    def __init__(self, window_seconds: int = 1, max_memory_windows: int = 1000):
        """
        Initialise l'agrégateur.
        
        Args:
            window_seconds: Taille des fenêtres temporelles en secondes
            max_memory_windows: Nombre maximum de fenêtres en mémoire
        """
        self.window_seconds = window_seconds
        self.max_memory_windows = max_memory_windows
        self.windows: Dict[str, 'WindowData'] = {}
        
        # Statistiques
        self.stats = {
            "processed_count": 0,
            "last_cleanup": get_current_timestamp(),
            "windows_created": 0,
            "windows_finalized": 0,
        }
        
        logger.info(f"{self.__class__.__name__} initialized with {window_seconds}s windows")
    
    @abstractmethod
    def _create_window_data(self, symbol: str, window_key: str) -> 'WindowData':
        """Crée une nouvelle fenêtre de données."""
        pass
    
    @abstractmethod
    def _should_process_record(self, record: T) -> bool:
        """Détermine si un enregistrement doit être traité."""
        pass
    
    def process_record(self, record: T) -> Optional[M]:
        """
        Traite un enregistrement et retourne les métriques si la fenêtre est complète.
        
        Args:
            record: Enregistrement à traiter
            
        Returns:
            Métriques agrégées si la fenêtre est complète, None sinon
        """
        if not self._should_process_record(record):
            return None
            
        try:
            window_key = iso_floor_to_second(record.timestamp)
            trade_key = f"{record.symbol}:{window_key}"
            
            # Initialise la fenêtre si nécessaire
            if trade_key not in self.windows:
                self._create_window(trade_key, record.symbol, window_key)
            
            window = self.windows[trade_key]
            window.add_record(record)
            
            self.stats["processed_count"] += 1
            
            # Vérifie si la fenêtre est complète (nouvelle seconde)
            current_window = iso_floor_to_second(get_current_timestamp())
            if window_key < current_window:
                return self._finalize_window(trade_key)
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing record in {self.__class__.__name__}: {e}")
            return None
    
    def _create_window(self, trade_key: str, symbol: str, window_key: str):
        """Crée une nouvelle fenêtre d'agrégation."""
        self.windows[trade_key] = self._create_window_data(symbol, window_key)
        self.stats["windows_created"] += 1
        logger.debug(f"Created window for {trade_key}")
    
    def _finalize_window(self, trade_key: str) -> Optional[M]:
        """Finalise une fenêtre et retourne les métriques."""
        if trade_key not in self.windows:
            return None
        
        window = self.windows[trade_key]
        if window.count == 0:
            del self.windows[trade_key]
            return None
        
        # Finalise les métriques
        metrics = window.finalize()
        
        # Cleanup
        del self.windows[trade_key]
        self.stats["windows_finalized"] += 1
        
        logger.debug(f"Finalized window for {trade_key}: {window.count} records")
        return metrics
    
    def force_finalize_current_windows(self) -> List[M]:
        """Force la finalisation des fenêtres en cours."""
        current_time = get_current_timestamp()
        current_window = iso_floor_to_second(current_time)
        
        completed_metrics = []
        keys_to_remove = []
        
        for trade_key, window in self.windows.items():
            # Finalise les fenêtres qui sont dans la seconde précédente
            if window.window_start < current_window and window.count > 0:
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
            logger.info(f"⏰ {self.__class__.__name__}: Forced finalization of {len(completed_metrics)} windows")
        
        return completed_metrics
    
    def cleanup_old_windows(self):
        """Nettoie les anciennes fenêtres pour libérer la mémoire."""
        current_time = get_current_timestamp()
        current_window = iso_floor_to_second(current_time)
        
        keys_to_remove = []
        for trade_key, window in self.windows.items():
            # Supprime les fenêtres anciennes
            if window.window_start < current_window:
                keys_to_remove.append(trade_key)
        
        for key in keys_to_remove:
            if key in self.windows:
                del self.windows[key]
        
        if keys_to_remove:
            logger.info(f"🧹 {self.__class__.__name__}: Cleaned up {len(keys_to_remove)} old windows")
        
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
        
        logger.warning(f"🧹 {self.__class__.__name__}: Force memory cleanup: removed {len(windows_to_remove)} windows")
    
    def get_active_windows_count(self) -> int:
        """Retourne le nombre de fenêtres actives."""
        return len(self.windows)
    
    def get_stats(self) -> Dict[str, any]:
        """Retourne les statistiques de l'agrégateur."""
        return {
            "aggregator_type": self.__class__.__name__,
            "processed_count": self.stats["processed_count"],
            "active_windows": len(self.windows),
            "windows_created": self.stats["windows_created"],
            "windows_finalized": self.stats["windows_finalized"],
            "memory_usage_windows": len(self.windows),
            "max_memory_windows": self.max_memory_windows,
        }


class WindowData(ABC):
    """Classe de base pour les données d'une fenêtre temporelle."""
    
    def __init__(self, symbol: str, window_start: str):
        self.symbol = symbol
        self.window_start = window_start
        self.window_end = window_start
        self.count = 0
    
    @abstractmethod
    def add_record(self, record: BaseRecord):
        """Ajoute un enregistrement à la fenêtre."""
        pass
    
    @abstractmethod
    def finalize(self) -> BaseMetrics:
        """Finalise les métriques de la fenêtre."""
        pass
