"""
Logging Configuration
====================

Configuration centralisée et avancée du logging pour l'application.
"""

import logging
import sys
import os
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """Formateur de logs avec couleurs pour la console."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Vert
        'WARNING': '\033[33m',  # Jaune
        'ERROR': '\033[31m',    # Rouge
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


class LoggerManager:
    """Gestionnaire de logging avancé avec support multi-handlers."""
    
    def __init__(self, name: str = "stream-processor", level: str = "INFO"):
        self.name = name
        self.level = level
        self._logger = None
        self._handlers = {}
        self._setup_logger()
    
    def _setup_logger(self):
        """Configure le logger avec handlers multiples."""
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(getattr(logging, self.level.upper()))
        
        # Évite les doublons de handlers
        if self._logger.handlers:
            return
        
        # Handler pour la console avec couleurs
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.level.upper()))
        
        # Formatter avec couleurs
        colored_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(colored_formatter)
        
        # Handler pour fichier (optionnel)
        if os.environ.get("LOG_TO_FILE", "false").lower() == "true":
            self._setup_file_handler()
        
        # Ajout des handlers
        self._logger.addHandler(console_handler)
        self._handlers['console'] = console_handler
        
        # Évite la propagation vers le root logger
        self._logger.propagate = False
    
    def _setup_file_handler(self):
        """Configure le handler de fichier."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"{self.name}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, self.level.upper()))
        
        # Formatter simple pour fichier
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        self._logger.addHandler(file_handler)
        self._handlers['file'] = file_handler
    
    @property
    def logger(self) -> logging.Logger:
        """Retourne le logger configuré."""
        return self._logger
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Retourne un logger avec un nom spécifique."""
        if name:
            child_logger = self._logger.getChild(name)
            return child_logger
        return self._logger
    
    def set_level(self, level: str):
        """Change le niveau de logging."""
        self.level = level.upper()
        self._logger.setLevel(getattr(logging, self.level))
        
        for handler in self._handlers.values():
            handler.setLevel(getattr(logging, self.level))
    
    def add_handler(self, handler: logging.Handler, name: str):
        """Ajoute un handler personnalisé."""
        self._logger.addHandler(handler)
        self._handlers[name] = handler
    
    def remove_handler(self, name: str):
        """Supprime un handler par nom."""
        if name in self._handlers:
            self._logger.removeHandler(self._handlers[name])
            del self._handlers[name]
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques du logger."""
        return {
            "name": self.name,
            "level": self.level,
            "handlers": list(self._handlers.keys()),
            "effective_level": logging.getLevelName(self._logger.getEffectiveLevel()),
        }


# Instance globale du logger manager
_logger_manager = None


def get_logger_manager() -> LoggerManager:
    """Retourne l'instance globale du logger manager."""
    global _logger_manager
    if _logger_manager is None:
        _logger_manager = LoggerManager()
    return _logger_manager


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Retourne un logger configuré."""
    return get_logger_manager().get_logger(name)


def setup_logging(level: str = "INFO", log_to_file: bool = False):
    """
    Configure le logging global de l'application.
    
    Args:
        level: Niveau de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Active l'écriture dans un fichier
    """
    global _logger_manager
    
    if log_to_file:
        os.environ["LOG_TO_FILE"] = "true"
    
    _logger_manager = LoggerManager(level=level)
    _logger_manager.set_level(level)
    
    # Configuration des loggers des dépendances
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("influxdb_client").setLevel(logging.WARNING)
