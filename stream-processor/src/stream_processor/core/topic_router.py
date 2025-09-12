"""
Topic Router
=============

Routeur pour diriger les messages vers les handlers appropriés selon le topic.
"""

from typing import Dict, Any, List, Optional

from ..utils.logger import get_logger
from ..handlers.base_handler import BaseHandler
from ..handlers.aggtrade_handler import AggTradeHandler
from ..handlers.kline_handler import KlineHandler
from ..models.base_models import BaseMetrics

logger = get_logger(__name__)


class TopicRouter:
    """Routeur pour diriger les messages vers les handlers par topic."""
    
    def __init__(self, window_seconds: int = 1, max_memory_windows: int = 1000):
        """
        Initialise le routeur de topics.
        
        Args:
            window_seconds: Taille des fenêtres temporelles
            max_memory_windows: Nombre maximum de fenêtres en mémoire
        """
        self.window_seconds = window_seconds
        self.max_memory_windows = max_memory_windows
        self.handlers: Dict[str, BaseHandler] = {}
        
        # Statistiques
        self.stats = {
            "total_messages": 0,
            "routed_messages": 0,
            "unrouted_messages": 0,
            "invalid_messages": 0,
        }
        
        # Initialise les handlers par défaut
        self._init_default_handlers()
        
        logger.info(f"TopicRouter initialized with {len(self.handlers)} handlers")
    
    def _init_default_handlers(self):
        """Initialise les handlers par défaut pour les topics connus."""
        # Handler pour aggTrade
        aggtrade_handler = AggTradeHandler(
            window_seconds=self.window_seconds,
            max_memory_windows=self.max_memory_windows
        )
        self.register_handler("aggTrade", aggtrade_handler)
        
        # Handler pour kline
        kline_handler = KlineHandler(
            window_seconds=self.window_seconds,
            max_memory_windows=self.max_memory_windows
        )
        self.register_handler("kline", kline_handler)
        
        logger.info("Default handlers registered for topics: aggTrade, kline")
    
    def register_handler(self, topic: str, handler: BaseHandler):
        """
        Enregistre un handler pour un topic.
        
        Args:
            topic: Nom du topic
            handler: Handler à associer au topic
        """
        self.handlers[topic] = handler
        logger.info(f"Registered {handler.__class__.__name__} for topic '{topic}'")
    
    def get_handler(self, topic: str) -> Optional[BaseHandler]:
        """
        Retourne le handler pour un topic donné.
        
        Args:
            topic: Nom du topic
            
        Returns:
            Handler associé au topic ou None si pas trouvé
        """
        return self.handlers.get(topic)
    
    def route_message(self, message: Dict[str, Any]) -> List[BaseMetrics]:
        """
        Route un message vers le handler approprié.
        
        Args:
            message: Message Kafka à router
            
        Returns:
            Liste des métriques générées
        """
        self.stats["total_messages"] += 1
        
        try:
            # Extrait le topic du message
            topic = message.get("topic")
            if not topic:
                logger.warning("Message without topic received")
                self.stats["invalid_messages"] += 1
                return []
            
            # Trouve le handler approprié
            handler = self.get_handler(topic)
            if not handler:
                logger.debug(f"No handler found for topic: {topic}")
                self.stats["unrouted_messages"] += 1
                return []
            
            # Vérifie si le handler peut traiter le message
            if not handler.can_handle(message):
                logger.debug(f"Handler {handler.__class__.__name__} cannot handle message from topic {topic}")
                self.stats["unrouted_messages"] += 1
                return []
            
            # Traite le message
            metrics = handler.process_message(message)
            self.stats["routed_messages"] += 1
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error routing message: {e}")
            self.stats["invalid_messages"] += 1
            return []
    
    def force_finalize_all_windows(self) -> List[BaseMetrics]:
        """Force la finalisation des fenêtres de tous les handlers."""
        all_metrics = []
        
        for topic, handler in self.handlers.items():
            try:
                metrics = handler.force_finalize_windows()
                all_metrics.extend(metrics)
                logger.debug(f"Finalized {len(metrics)} windows for topic {topic}")
            except Exception as e:
                logger.error(f"Error finalizing windows for topic {topic}: {e}")
        
        if all_metrics:
            logger.info(f"⏰ Router: Forced finalization of {len(all_metrics)} total windows")
        
        return all_metrics
    
    def cleanup_all_old_windows(self):
        """Nettoie les anciennes fenêtres de tous les handlers."""
        for topic, handler in self.handlers.items():
            try:
                handler.cleanup_old_windows()
            except Exception as e:
                logger.error(f"Error cleaning up windows for topic {topic}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques détaillées du routeur."""
        handler_stats = {}
        for topic, handler in self.handlers.items():
            try:
                handler_stats[topic] = handler.get_stats()
            except Exception as e:
                logger.debug(f"Could not get stats for handler {topic}: {e}")
                handler_stats[topic] = {"error": str(e)}
        
        return {
            "router_stats": {
                "total_messages": self.stats["total_messages"],
                "routed_messages": self.stats["routed_messages"],
                "unrouted_messages": self.stats["unrouted_messages"],
                "invalid_messages": self.stats["invalid_messages"],
                "routing_success_rate": (
                    self.stats["routed_messages"] / max(self.stats["total_messages"], 1) * 100
                    if self.stats["total_messages"] > 0 else 0
                )
            },
            "handlers": handler_stats,
            "topics_supported": list(self.handlers.keys()),
            "total_active_windows": sum(
                handler.get_active_windows_count() 
                for handler in self.handlers.values()
            )
        }
    
    def get_topic_summary(self) -> Dict[str, Dict[str, Any]]:
        """Retourne un résumé par topic."""
        summary = {}
        
        for topic, handler in self.handlers.items():
            try:
                if hasattr(handler, 'get_summary'):
                    summary[topic] = handler.get_summary()
                else:
                    stats = handler.get_stats()
                    summary[topic] = {
                        "handler": handler.__class__.__name__,
                        "messages_processed": stats.get("messages_processed", 0),
                        "active_windows": handler.get_active_windows_count(),
                    }
            except Exception as e:
                logger.debug(f"Error getting summary for topic {topic}: {e}")
                summary[topic] = {"error": str(e)}
        
        return summary
