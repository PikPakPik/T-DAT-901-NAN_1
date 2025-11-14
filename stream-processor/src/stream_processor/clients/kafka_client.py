"""
Kafka Client
============

Client Kafka optimisé pour la consommation des données de trading.
"""

import json
from typing import List, Dict, Any, Iterator, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..config.settings import KafkaConfig
from ..utils.logger import get_logger
from ..models.trade_models import TradeRecord

logger = get_logger(__name__)


class KafkaClient:
    """Client Kafka optimisé pour le stream processing."""
    
    def __init__(self, config: KafkaConfig, topics: List[str]):
        """
        Initialise le client Kafka.
        
        Args:
            config: Configuration Kafka
            topics: Liste des topics à consommer
        """
        self.config = config
        self.topics = topics
        self.consumer: Optional[KafkaConsumer] = None
        self._create_consumer()
    
    def _create_consumer(self):
        """Crée le consumer Kafka avec la configuration optimisée."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                auto_commit_interval_ms=self.config.auto_commit_interval_ms,
                value_deserializer=self._deserialize_message,
                max_poll_records=self.config.max_poll_records,
                fetch_min_bytes=self.config.fetch_min_bytes,
                fetch_max_wait_ms=self.config.fetch_max_wait_ms,
                session_timeout_ms=self.config.session_timeout_ms,
                max_poll_interval_ms=self.config.max_poll_interval_ms,
                api_version=(0, 10, 1),  # Version API Kafka
                security_protocol='PLAINTEXT',
                retry_backoff_ms=100,
                request_timeout_ms=self.config.session_timeout_ms + 5000,  # Plus grand que session timeout
            )
            logger.info(f"✅ Kafka consumer created for topics: {self.topics}")
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka consumer: {e}")
            raise
    
    def _deserialize_message(self, value: bytes) -> Dict[str, Any]:
        """Désérialise un message Kafka."""
        try:
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to deserialize message: {e}")
            return {}
    
    def consume_messages(self) -> Iterator[Dict[str, Any]]:
        """
        Générateur pour consommer les messages Kafka.
        
        Yields:
            Dict contenant les informations du message
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not initialized")
        
        logger.info("🔄 Starting message consumption...")
        
        try:
            for message in self.consumer:
                try:
                    # Validation et enrichissement du message
                    if self._is_valid_message(message):
                        yield self._enrich_message(message)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("🛑 Message consumption stopped by user")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            raise
        except Exception as e:
            logger.error(f"Fatal error in message consumption: {e}")
            raise
    
    def _is_valid_message(self, message) -> bool:
        """Valide si un message est un trade valide."""
        if not message.value:
            return False
        
        if message.topic != "trade":
            return False
        
        value = message.value
        required_fields = ["p", "q", "T", "s"]
        
        return all(field in value for field in required_fields)
    
    def _enrich_message(self, message) -> Dict[str, Any]:
        """Enrichit le message avec des métadonnées."""
        return {
            "topic": message.topic,
            "value": message.value,
            "timestamp": message.timestamp,
            "offset": message.offset,
            "partition": message.partition,
            "key": message.key,
            "headers": dict(message.headers) if message.headers else {},
        }
    
    def create_trade_record(self, message: Dict[str, Any]) -> Optional[TradeRecord]:
        """Crée un TradeRecord depuis un message Kafka."""
        try:
            return TradeRecord.from_dict(message["value"])
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Failed to create TradeRecord: {e}")
            return None
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques du consumer."""
        if not self.consumer:
            return {}
        
        try:
            # Métriques basiques du consumer
            metrics = {
                "topics": list(self.consumer.subscription()),
                "group_id": self.config.group_id,
                "bootstrap_servers": self.config.bootstrap_servers,
                "auto_offset_reset": self.config.auto_offset_reset,
                "enable_auto_commit": self.config.enable_auto_commit,
            }
            
            # Métriques de performance (si disponibles)
            if hasattr(self.consumer, '_client'):
                client_metrics = self.consumer._client.metrics()
                if client_metrics:
                    metrics.update({
                        "consumer_lag": client_metrics.get("consumer_lag", {}),
                        "records_consumed": client_metrics.get("records_consumed_total", 0),
                    })
            
            return metrics
        except Exception as e:
            logger.debug(f"Could not get consumer metrics: {e}")
            return {}
    
    def close(self):
        """Ferme le consumer Kafka."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("✅ Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        """Représentation string du client."""
        return f"KafkaClient(topics={self.topics}, group_id={self.config.group_id})"
