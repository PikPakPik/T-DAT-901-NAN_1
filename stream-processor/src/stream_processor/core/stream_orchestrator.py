"""
Stream Orchestrator
====================

Orchestrateur principal du stream processing avec routing par topic.
Remplace l'ancien StreamProcessor avec une architecture modulaire.
"""

import time
from typing import List, Dict, Any

from ..config.settings import ConfigManager
from ..utils.logger import get_logger
from ..utils.helpers import get_current_timestamp, calculate_throughput
from ..clients.kafka_client import KafkaClient
from ..clients.influxdb_client import InfluxDBClient
from ..core.topic_router import TopicRouter
from ..models.base_models import BaseMetrics

logger = get_logger(__name__)


class StreamOrchestrator:
    """Orchestrateur principal avec architecture modulaire par topic."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialise l'orchestrateur.
        
        Args:
            config_manager: Gestionnaire de configuration
        """
        self.config = config_manager
        self.kafka_client: KafkaClient = None
        self.influx_client: InfluxDBClient = None
        self.topic_router: TopicRouter = None
        
        # État de l'orchestrateur
        self.metrics_buffer: List[BaseMetrics] = []
        self.last_force_write = get_current_timestamp()
        self.last_force_flush = get_current_timestamp()
        self.last_cleanup = get_current_timestamp()
        self.last_stats = get_current_timestamp()
        
        # Statistiques
        self.stats = {
            "start_time": get_current_timestamp(),
            "messages_processed": 0,
            "windows_completed": 0,
            "points_written": 0,
            "errors_count": 0,
        }
        
        # Initialisation des composants
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialise tous les composants de l'orchestrateur."""
        try:
            logger.info("🔧 Initializing orchestrator components...")
            
            # Validation de la configuration
            self.config.validate()
            
            # Initialisation du routeur de topics
            self.topic_router = TopicRouter(
                window_seconds=self.config.app.window_seconds,
                max_memory_windows=self.config.app.max_memory_windows
            )
            
            # Initialisation du client Kafka
            self.kafka_client = KafkaClient(
                config=self.config.kafka,
                topics=self.config.app.topics
            )
            
            # Initialisation du client InfluxDB
            self.influx_client = InfluxDBClient(
                config=self.config.influxdb
            )
            
            logger.info("✅ All orchestrator components initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize orchestrator components: {e}")
            raise
    
    def run(self):
        """Lance l'orchestration du stream."""
        logger.info("🚀 Starting stream orchestrator...")
        logger.info(f"📊 Processing configuration: {self._get_config_summary()}")
        
        try:
            with self.kafka_client as kafka:
                with self.influx_client as influx:
                    self._process_stream(kafka, influx)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Stream orchestrator stopped by user")
        except Exception as e:
            logger.error(f"❌ Fatal error in stream orchestrator: {e}")
            raise
        finally:
            self._cleanup()
    
    def _process_stream(self, kafka: KafkaClient, influx: InfluxDBClient):
        """Traite le stream de données avec le routeur de topics."""
        logger.info("🔄 Starting stream processing with topic routing...")
        
        for message in kafka.consume_messages():
            try:
                current_time = get_current_timestamp()
                
                # Route le message vers le handler approprié
                metrics = self.topic_router.route_message(message)
                
                # Ajoute les métriques au buffer
                if metrics:
                    self.metrics_buffer.extend(metrics)
                    self.stats["windows_completed"] += len(metrics)
                    
                    # Log des métriques générées
                    for metric in metrics:
                        logger.debug(f"📈 Generated metrics for {metric.symbol}")
                
                self.stats["messages_processed"] += 1
                
                # Gestion des timers et actions périodiques
                self._handle_periodic_actions(current_time, influx)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                self.stats["errors_count"] += 1
                continue
    
    def _handle_periodic_actions(self, current_time: int, influx: InfluxDBClient):
        """Gère les actions périodiques de l'orchestrateur."""
        # Force l'écriture des fenêtres toutes les secondes
        if current_time - self.last_force_write >= self.config.app.force_write_interval_seconds * 1000:
            self._force_finalize_windows()
            self.last_force_write = current_time
        
        # Écriture par batch
        if len(self.metrics_buffer) >= self.config.influxdb.batch_size:
            self._write_batch(influx)
        
        # Force l'écriture des points restants
        if current_time - self.last_force_flush >= self.config.app.force_flush_interval_seconds * 1000:
            if self.metrics_buffer:
                self._write_batch(influx, force=True)
            self.last_force_flush = current_time
        
        # Cleanup et statistiques périodiques
        if current_time - self.last_cleanup >= self.config.app.cleanup_interval_seconds * 1000:
            self._periodic_cleanup()
            self.last_cleanup = current_time
        
        # Affichage des statistiques
        if self.stats["messages_processed"] % self.config.app.stats_interval_messages == 0:
            self._display_stats()
    
    def _force_finalize_windows(self):
        """Force la finalisation des fenêtres via le routeur."""
        forced_metrics = self.topic_router.force_finalize_all_windows()
        if forced_metrics:
            self.metrics_buffer.extend(forced_metrics)
            self.stats["windows_completed"] += len(forced_metrics)
            logger.info(f"⏰ Forced {len(forced_metrics)} windows via router")
    
    def _write_batch(self, influx: InfluxDBClient, force: bool = False):
        """Écrit un batch de métriques dans InfluxDB."""
        if not self.metrics_buffer:
            return
        
        try:
            # Le client InfluxDB attend des objets métriques, pas des dictionnaires
            success = influx.write_metrics(self.metrics_buffer)
            if success:
                self.stats["points_written"] += len(self.metrics_buffer)
                logger.info(f"✅ {'Force ' if force else ''}wrote {len(self.metrics_buffer)} points to InfluxDB")
                self.metrics_buffer.clear()
            else:
                logger.error(f"Failed to write {len(self.metrics_buffer)} points to InfluxDB")
            
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
    
    def _periodic_cleanup(self):
        """Nettoyage périodique via le routeur."""
        self.topic_router.cleanup_all_old_windows()
        logger.debug("🧹 Periodic cleanup completed via router")
    
    def _display_stats(self):
        """Affiche les statistiques de performance détaillées."""
        current_time = get_current_timestamp()
        uptime_ms = current_time - self.stats["start_time"]
        uptime_seconds = uptime_ms / 1000.0
        
        throughput = calculate_throughput(self.stats["messages_processed"], uptime_seconds)
        
        # Statistiques du routeur
        router_stats = self.topic_router.get_stats()
        
        stats_info = {
            "uptime_seconds": f"{uptime_seconds:.1f}",
            "messages_processed": self.stats["messages_processed"],
            "windows_completed": self.stats["windows_completed"],
            "points_written": self.stats["points_written"],
            "errors_count": self.stats["errors_count"],
            "throughput_msg_per_sec": f"{throughput:.1f}",
            "total_active_windows": router_stats.get("total_active_windows", 0),
            "buffer_size": len(self.metrics_buffer),
            "routing_success_rate": f"{router_stats['router_stats']['routing_success_rate']:.1f}%",
        }
        
        logger.info(f"📊 Orchestrator Stats: {stats_info}")
        
        # Affiche le résumé par topic
        topic_summary = self.topic_router.get_topic_summary()
        for topic, summary in topic_summary.items():
            logger.info(f"📈 Topic {topic}: {summary}")
    
    def _get_config_summary(self) -> Dict[str, Any]:
        """Retourne un résumé de la configuration."""
        return {
            "window_seconds": self.config.app.window_seconds,
            "topics": self.config.app.topics,
            "influx_batch_size": self.config.influxdb.batch_size,
            "kafka_group_id": self.config.kafka.group_id,
            "write_mode": self.config.influxdb.write_mode.value,
        }
    
    def _cleanup(self):
        """Nettoyage final de l'orchestrateur."""
        logger.info("🧹 Starting orchestrator cleanup...")
        
        # Écriture des métriques restantes
        if self.metrics_buffer and self.influx_client:
            try:
                self._write_batch(self.influx_client, force=True)
                logger.info(f"✅ Wrote final {len(self.metrics_buffer)} points to InfluxDB")
            except Exception as e:
                logger.error(f"Error writing final points: {e}")
        
        # Affichage des statistiques finales
        self._display_final_stats()
        
        logger.info("✅ Stream orchestrator shutdown complete")
    
    def _display_final_stats(self):
        """Affiche les statistiques finales."""
        current_time = get_current_timestamp()
        uptime_ms = current_time - self.stats["start_time"]
        uptime_seconds = uptime_ms / 1000.0
        
        # Statistiques du routeur
        router_stats = self.topic_router.get_stats()
        
        final_stats = {
            "total_uptime_seconds": f"{uptime_seconds:.1f}",
            "total_messages_processed": self.stats["messages_processed"],
            "total_windows_completed": self.stats["windows_completed"],
            "total_points_written": self.stats["points_written"],
            "total_errors": self.stats["errors_count"],
            "average_throughput_msg_per_sec": f"{calculate_throughput(self.stats['messages_processed'], uptime_seconds):.1f}",
            "final_routing_success_rate": f"{router_stats['router_stats']['routing_success_rate']:.1f}%",
        }
        
        logger.info(f"📊 Final Orchestrator Stats: {final_stats}")
        
        # Affiche les statistiques finales par topic
        logger.info("📈 Final Topic Summary:")
        topic_summary = self.topic_router.get_topic_summary()
        for topic, summary in topic_summary.items():
            logger.info(f"  {topic}: {summary}")
