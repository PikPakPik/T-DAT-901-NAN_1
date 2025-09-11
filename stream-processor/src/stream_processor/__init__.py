"""
Stream Processor Package
========================

Application Python professionnelle pour le traitement en temps réel des données de trading crypto.

Version: 2.0.0
Author: Stream Processor Team
License: MIT
"""

__version__ = "2.0.0"
__author__ = "Stream Processor Team"
__email__ = "team@stream-processor.dev"
__license__ = "MIT"

# Import des composants principaux
from .core.processor import StreamProcessor
from .config.settings import AppConfig, KafkaConfig, InfluxDBConfig
from .utils.logger import get_logger

__all__ = [
    "StreamProcessor",
    "AppConfig", 
    "KafkaConfig",
    "InfluxDBConfig",
    "get_logger",
]
