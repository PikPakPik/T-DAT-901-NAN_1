"""
Base Models
============

Modèles de base partagés entre tous les types de données.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime


@dataclass
class BaseRecord(ABC):
    """Classe de base pour tous les enregistrements de données."""
    
    symbol: str
    timestamp: int
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseRecord':
        """Crée un enregistrement depuis un dictionnaire."""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire."""
        pass
    
    @property
    def datetime(self) -> datetime:
        """Retourne le timestamp en datetime."""
        return datetime.fromtimestamp(self.timestamp / 1000)


@dataclass
class BaseMetrics(ABC):
    """Classe de base pour toutes les métriques agrégées."""
    
    symbol: str
    window_start: str
    window_end: str
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour stockage."""
        pass
