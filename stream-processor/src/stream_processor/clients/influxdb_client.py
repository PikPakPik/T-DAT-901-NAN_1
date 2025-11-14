"""
InfluxDB Client
===============

Client InfluxDB optimisé pour l'écriture des métriques calculées.
"""

from typing import List, Dict, Any, Optional
import time
from influxdb_client import InfluxDBClient as InfluxDB, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS

from ..config.settings import InfluxDBConfig, WriteMode
from ..utils.logger import get_logger
from ..models.base_models import BaseMetrics
from ..models.trade_models import AggregatedMetrics  # Compatibility

logger = get_logger(__name__)


class InfluxDBClient:
    """Client InfluxDB avec gestion avancée des erreurs et retry."""
    
    def __init__(self, config: InfluxDBConfig):
        """
        Initialise le client InfluxDB.
        
        Args:
            config: Configuration InfluxDB
        """
        self.config = config
        self.client: Optional[InfluxDB] = None
        self.write_api = None
        self._retry_count = 0
        self._create_client()
    
    def _create_client(self):
        """Crée le client InfluxDB avec la configuration optimisée."""
        try:
            # Création du client InfluxDB avec la syntaxe correcte
            self.client = InfluxDB(
                self.config.url,
                self.config.token
            )
            # Configuration de l'API d'écriture
            write_options = self._get_write_options()
            self.write_api = self.client.write_api(write_options=write_options)

            logger.info(f"✅ InfluxDB client created for bucket: {self.config.bucket}")
            
            # Test de connexion
            self._test_connection()
            
        except Exception as e:
            logger.error(f"❌ Failed to create InfluxDB client: {e}")
            raise
    
    def _get_write_options(self):
        """Retourne les options d'écriture selon la configuration."""
        if self.config.write_mode == WriteMode.ASYNCHRONOUS:
            return ASYNCHRONOUS
        else:
            return SYNCHRONOUS
    
    def _test_connection(self):
        """Test la connexion à InfluxDB."""
        try:
            # Test simple de connexion
            health = self.client.health()
            logger.info(f"InfluxDB health check: {health.status}")
        except Exception as e:
            logger.warning(f"InfluxDB health check failed: {e}")
    
    def write_metrics(self, metrics: List[BaseMetrics]) -> bool:
        """
        Écrit les métriques dans InfluxDB avec retry automatique.
        
        Args:
            metrics: Liste des métriques à écrire
            
        Returns:
            True si l'écriture a réussi
        """
        if not metrics:
            return True
        
        try:
            points = [self._metrics_to_point(metric) for metric in metrics]
            
            self.write_api.write(
                bucket=self.config.bucket,
                org=self.config.org,
                record=points
            )
            
            # Reset du compteur de retry en cas de succès
            self._retry_count = 0
            
            logger.info(f"✅ Wrote {len(metrics)} metrics to InfluxDB")
            return True
            
        except Exception as e:
            logger.error(f"⚠️ InfluxDB write error: {e}")
            return self._handle_write_error(e, metrics)
    
    def _handle_write_error(self, error: Exception, metrics: List[BaseMetrics]) -> bool:
        """Gère les erreurs d'écriture avec retry."""
        self._retry_count += 1
        
        if self._retry_count <= self.config.max_retries:
            logger.warning(f"Retrying write ({self._retry_count}/{self.config.max_retries})...")
            time.sleep(self.config.retry_interval_ms / 1000)
            
            try:
                return self.write_metrics(metrics)
            except Exception:
                pass
        
        logger.error(f"Failed to write after {self.config.max_retries} retries")
        return False
    
    def _metrics_to_point(self, metrics: BaseMetrics) -> Point:
        """Convertit les métriques en point InfluxDB de manière générique."""
        # Measurement pour les trades
        measurement = "trade_1s"
        
        point = Point(measurement).tag("symbol", metrics.symbol)
        
        # Liste des champs possibles à écrire
        possible_fields = [
            # Métriques de base
            'volume', 'trade_count',
            # Métriques de prix
            'min_price', 'max_price', 'avg_price',
            # Métriques d'ordre
            'buyer_volume', 'seller_volume',
        ]
        
        # Champs qui doivent toujours être écrits même s'ils sont à 0
        always_write_fields = ['price_change', 'price_change_percent', 'trade_count']
        
        for field_name in possible_fields:
            if hasattr(metrics, field_name):
                try:
                    value = getattr(metrics, field_name)
                    if value is None:
                        continue
                    
                    if isinstance(value, (int, float)):
                        # Écrit les champs entiers comme int
                        if field_name in ['trade_count']:
                            point = point.field(field_name, int(value))
                        # Écrit seulement si valeur != 0 ou champ dans always_write_fields
                        elif value != 0 or field_name in always_write_fields:
                            point = point.field(field_name, float(value))
                except Exception as e:
                    logger.debug(f"Could not add field {field_name}: {e}")
        
        # Timestamp
        point = point.time(metrics.window_start)
        return point
    
    def flush(self):
        """Force l'écriture des points en attente."""
        if self.write_api:
            try:
                self.write_api.flush()
                logger.debug("InfluxDB write API flushed")
            except Exception as e:
                logger.warning(f"Error flushing InfluxDB: {e}")
    
    def get_client_info(self) -> Dict[str, Any]:
        """Retourne les informations du client."""
        try:
            return {
                "url": self.config.url,
                "bucket": self.config.bucket,
                "org": self.config.org,
                "write_mode": self.config.write_mode.value,
                "batch_size": self.config.batch_size,
                "retry_count": self._retry_count,
                "health": self.client.health().status if self.client else "unknown",
            }
        except Exception as e:
            logger.debug(f"Could not get client info: {e}")
            return {}
    
    def close(self):
        """Ferme le client InfluxDB."""
        if self.client:
            try:
                # Flush final
                self.flush()
                self.client.close()
                logger.info("✅ InfluxDB client closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB client: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        """Représentation string du client."""
        return f"InfluxDBClient(bucket={self.config.bucket}, org={self.config.org})"
