"""
Configuration Settings
======================

Gestion centralisée de la configuration de l'application avec validation.
"""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class LogLevel(Enum):
    """Niveaux de logging disponibles."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class WriteMode(Enum):
    """Modes d'écriture InfluxDB."""
    SYNCHRONOUS = "SYNCHRONOUS"
    ASYNCHRONOUS = "ASYNCHRONOUS"


@dataclass
class KafkaConfig:
    """Configuration Kafka optimisée."""
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 2000
    fetch_min_bytes: int = 512
    fetch_max_wait_ms: int = 100
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000


@dataclass
class InfluxDBConfig:
    """Configuration InfluxDB avec options avancées."""
    url: str
    token: str
    org: str
    bucket: str
    batch_size: int = 100
    write_mode: WriteMode = WriteMode.ASYNCHRONOUS
    flush_interval_ms: int = 1000
    retry_interval_ms: int = 5000
    max_retries: int = 3


@dataclass
class AppConfig:
    """Configuration principale de l'application."""
    # Note: window_seconds est toujours forcé à 1 dans le code
    # InfluxDB gère l'agrégation temporelle via ses requêtes (GROUP BY time())
    window_seconds: int = 1
    topics: List[str] = field(default_factory=lambda: ["trade", "kline"])
    log_level: LogLevel = LogLevel.INFO
    max_memory_windows: int = 1000
    cleanup_interval_seconds: int = 60
    stats_interval_messages: int = 100
    force_write_interval_seconds: int = 1
    force_flush_interval_seconds: int = 5


class ConfigManager:
    """Gestionnaire de configuration avec validation et chargement d'environnement."""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialise le gestionnaire de configuration.
        
        Args:
            env_file: Chemin vers le fichier .env (optionnel)
        """
        self._config = self._load_config()
        if env_file and os.path.exists(env_file):
            self._load_env_file(env_file)
    
    def _load_env_file(self, env_file: str):
        """Charge les variables depuis un fichier .env."""
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        except Exception as e:
            print(f"Warning: Could not load env file {env_file}: {e}")
    
    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration depuis les variables d'environnement."""
        
        # Configuration Kafka
        kafka_config = KafkaConfig(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            group_id=os.environ.get("KAFKA_GROUP_ID", "stream-processor-kafka-streams-1s"),
            auto_offset_reset=os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
            enable_auto_commit=os.environ.get("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true",
            auto_commit_interval_ms=int(os.environ.get("KAFKA_AUTO_COMMIT_INTERVAL_MS", "5000")),
            max_poll_records=int(os.environ.get("KAFKA_MAX_POLL_RECORDS", "2000")),
            fetch_min_bytes=int(os.environ.get("KAFKA_FETCH_MIN_BYTES", "512")),
            fetch_max_wait_ms=int(os.environ.get("KAFKA_FETCH_MAX_WAIT_MS", "100")),
            session_timeout_ms=int(os.environ.get("KAFKA_SESSION_TIMEOUT_MS", "30000")),
            max_poll_interval_ms=int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
        )
        
        # Configuration InfluxDB
        influx_config = InfluxDBConfig(
            url=os.environ.get("INFLUX_URL", "http://influxdb:8086"),
            token=os.environ.get("INFLUX_TOKEN"),
            org=os.environ.get("INFLUX_ORG"),
            bucket=os.environ.get("INFLUX_BUCKET"),
            batch_size=int(os.environ.get("INFLUX_BATCH_SIZE", "100")),
            write_mode=WriteMode(os.environ.get("INFLUX_WRITE_MODE", "ASYNCHRONOUS")),
            flush_interval_ms=int(os.environ.get("INFLUX_FLUSH_INTERVAL_MS", "1000")),
            retry_interval_ms=int(os.environ.get("INFLUX_RETRY_INTERVAL_MS", "5000")),
            max_retries=int(os.environ.get("INFLUX_MAX_RETRIES", "3")),
        )
        
        # Configuration de l'application
        app_config = AppConfig(
            window_seconds=int(os.environ.get("WINDOW_SECONDS", "1")),
            topics=[
                os.environ.get("TOPIC_TRADE", "trade"),
                os.environ.get("TOPIC_KLINE", "kline"),
            ],
            log_level=LogLevel(os.environ.get("LOG_LEVEL", "INFO")),
            max_memory_windows=int(os.environ.get("MAX_MEMORY_WINDOWS", "1000")),
            cleanup_interval_seconds=int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "60")),
            stats_interval_messages=int(os.environ.get("STATS_INTERVAL_MESSAGES", "100")),
            force_write_interval_seconds=int(os.environ.get("FORCE_WRITE_INTERVAL_SECONDS", "1")),
            force_flush_interval_seconds=int(os.environ.get("FORCE_FLUSH_INTERVAL_SECONDS", "5")),
        )
        
        return {
            "kafka": kafka_config,
            "influxdb": influx_config,
            "app": app_config,
        }
    
    @property
    def kafka(self) -> KafkaConfig:
        """Retourne la configuration Kafka."""
        return self._config["kafka"]
    
    @property
    def influxdb(self) -> InfluxDBConfig:
        """Retourne la configuration InfluxDB."""
        return self._config["influxdb"]
    
    @property
    def app(self) -> AppConfig:
        """Retourne la configuration de l'application."""
        return self._config["app"]
    
    def validate(self) -> bool:
        """Valide la configuration et retourne True si valide."""
        errors = []
        
        # Validation Kafka
        if not self.kafka.bootstrap_servers:
            errors.append("KAFKA_BOOTSTRAP_SERVERS is required")
        if not self.kafka.group_id:
            errors.append("KAFKA_GROUP_ID is required")
        
        # Validation InfluxDB
        if not self.influxdb.token:
            errors.append("INFLUX_TOKEN is required")
        if not self.influxdb.org:
            errors.append("INFLUX_ORG is required")
        if not self.influxdb.bucket:
            errors.append("INFLUX_BUCKET is required")
        if not self.influxdb.url:
            errors.append("INFLUX_URL is required")
        
        # Validation Application
        if self.app.window_seconds <= 0:
            errors.append("WINDOW_SECONDS must be positive")
        if not self.app.topics:
            errors.append("At least one topic must be configured")
        if self.influxdb.batch_size <= 0:
            errors.append("INFLUX_BATCH_SIZE must be positive")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Retourne la configuration sous forme de dictionnaire."""
        return {
            "kafka": {
                "bootstrap_servers": self.kafka.bootstrap_servers,
                "group_id": self.kafka.group_id,
                "auto_offset_reset": self.kafka.auto_offset_reset,
                "enable_auto_commit": self.kafka.enable_auto_commit,
                "auto_commit_interval_ms": self.kafka.auto_commit_interval_ms,
                "max_poll_records": self.kafka.max_poll_records,
                "fetch_min_bytes": self.kafka.fetch_min_bytes,
                "fetch_max_wait_ms": self.kafka.fetch_max_wait_ms,
            },
            "influxdb": {
                "url": self.influxdb.url,
                "bucket": self.influxdb.bucket,
                "org": self.influxdb.org,
                "batch_size": self.influxdb.batch_size,
                "write_mode": self.influxdb.write_mode.value,
            },
            "app": {
                "window_seconds": self.app.window_seconds,
                "topics": self.app.topics,
                "log_level": self.app.log_level.value,
                "max_memory_windows": self.app.max_memory_windows,
                "cleanup_interval_seconds": self.app.cleanup_interval_seconds,
                "stats_interval_messages": self.app.stats_interval_messages,
            },
        }
