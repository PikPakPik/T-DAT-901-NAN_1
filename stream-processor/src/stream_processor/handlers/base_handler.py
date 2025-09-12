"""
Base Handler
=============

Classe de base pour tous les handlers de topics.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, TypeVar, Generic

from ..utils.logger import get_logger
from ..models.base_models import BaseRecord, BaseMetrics
from ..aggregators.base_aggregator import BaseAggregator

logger = get_logger(__name__)

T = TypeVar('T', bound=BaseRecord)
M = TypeVar('M', bound=BaseMetrics)


class BaseHandler(Generic[T, M], ABC):
    """Classe de base pour tous les handlers de topics."""
    
    def __init__(self, topic_name: str):
        """
        Initialise le handler.
        
        Args:
            topic_name: Nom du topic à traiter
        """
        self.topic_name = topic_name
        self.aggregators: List[BaseAggregator] = []
        
        # Statistiques
        self.stats = {
            "messages_processed": 0,
            "records_created": 0,
            "invalid_messages": 0,
            "metrics_generated": 0,
        }
        
        logger.info(f"{self.__class__.__name__} initialized for topic: {topic_name}")
    
    @abstractmethod
    def _is_valid_message(self, message: Dict[str, Any]) -> bool:
        """Vérifie si un message est valide pour ce handler."""
        pass
    
    @abstractmethod
    def _create_record_from_message(self, message: Dict[str, Any]) -> Optional[T]:
        """Crée un enregistrement depuis un message Kafka."""
        pass
    
    def add_aggregator(self, aggregator: BaseAggregator):
        """Ajoute un agrégateur au handler."""
        self.aggregators.append(aggregator)
        logger.info(f"Added {aggregator.__class__.__name__} to {self.__class__.__name__}")
    
    def can_handle(self, message: Dict[str, Any]) -> bool:
        """Vérifie si ce handler peut traiter le message."""
        return (
            message.get("topic") == self.topic_name and 
            self._is_valid_message(message)
        )
    
    def process_message(self, message: Dict[str, Any]) -> List[M]:
        """
        Traite un message et retourne les métriques générées.
        
        Args:
            message: Message Kafka à traiter
            
        Returns:
            Liste des métriques générées par tous les agrégateurs
        """
        if not self.can_handle(message):
            return []
        
        try:
            # Crée l'enregistrement depuis le message
            record = self._create_record_from_message(message)
            if not record:
                self.stats["invalid_messages"] += 1
                return []
            
            self.stats["records_created"] += 1
            
            # Traite avec tous les agrégateurs
            metrics = []
            for aggregator in self.aggregators:
                try:
                    result = aggregator.process_record(record)
                    if result:
                        metrics.append(result)
                        self.stats["metrics_generated"] += 1
                except Exception as e:
                    logger.error(f"Error in {aggregator.__class__.__name__}: {e}")
                    continue
            
            self.stats["messages_processed"] += 1
            return metrics
            
        except Exception as e:
            logger.error(f"Error processing message in {self.__class__.__name__}: {e}")
            self.stats["invalid_messages"] += 1
            return []
    
    def force_finalize_windows(self) -> List[M]:
        """Force la finalisation des fenêtres de tous les agrégateurs."""
        all_metrics = []
        for aggregator in self.aggregators:
            try:
                metrics = aggregator.force_finalize_current_windows()
                all_metrics.extend(metrics)
            except Exception as e:
                logger.error(f"Error forcing finalization in {aggregator.__class__.__name__}: {e}")
        
        if all_metrics:
            self.stats["metrics_generated"] += len(all_metrics)
        
        return all_metrics
    
    def cleanup_old_windows(self):
        """Nettoie les anciennes fenêtres de tous les agrégateurs."""
        for aggregator in self.aggregators:
            try:
                aggregator.cleanup_old_windows()
            except Exception as e:
                logger.error(f"Error cleaning up {aggregator.__class__.__name__}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques du handler."""
        aggregator_stats = []
        for aggregator in self.aggregators:
            try:
                aggregator_stats.append(aggregator.get_stats())
            except Exception as e:
                logger.debug(f"Could not get stats from {aggregator.__class__.__name__}: {e}")
        
        return {
            "handler_type": self.__class__.__name__,
            "topic": self.topic_name,
            "messages_processed": self.stats["messages_processed"],
            "records_created": self.stats["records_created"],
            "invalid_messages": self.stats["invalid_messages"],
            "metrics_generated": self.stats["metrics_generated"],
            "aggregators_count": len(self.aggregators),
            "aggregators_stats": aggregator_stats,
        }
    
    def get_active_windows_count(self) -> int:
        """Retourne le nombre total de fenêtres actives."""
        return sum(agg.get_active_windows_count() for agg in self.aggregators)
