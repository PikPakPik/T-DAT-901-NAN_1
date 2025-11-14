"""
Helper Functions
================

Fonctions utilitaires pour le stream processor.
"""

import time
from datetime import datetime, timezone
from typing import Union


def iso_floor_to_second(ts_ms: Union[int, float]) -> str:
    """
    Convertit un timestamp en clé de fenêtre (seconde).
    
    Args:
        ts_ms: Timestamp en millisecondes
        
    Returns:
        String ISO de la fenêtre de temps (seconde)
    """
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    floored = dt.replace(microsecond=0)
    return floored.isoformat()


def iso_floor_to_window(ts_ms: Union[int, float], window_seconds: int = 1) -> str:
    """
    Convertit un timestamp en clé de fenêtre avec une taille de fenêtre personnalisée.
    
    Args:
        ts_ms: Timestamp en millisecondes
        window_seconds: Taille de la fenêtre en secondes
        
    Returns:
        String ISO de la fenêtre de temps
    """
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    # Arrondit à la fenêtre la plus proche
    seconds_since_epoch = int(dt.timestamp())
    window_start_seconds = (seconds_since_epoch // window_seconds) * window_seconds
    floored = datetime.fromtimestamp(window_start_seconds, tz=timezone.utc)
    return floored.isoformat()


def format_bytes(bytes_value: int) -> str:
    """
    Formate une taille en bytes en format lisible.
    
    Args:
        bytes_value: Taille en bytes
        
    Returns:
        String formatée (ex: "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_number(number: Union[int, float], precision: int = 2) -> str:
    """
    Formate un nombre avec des séparateurs de milliers.
    
    Args:
        number: Nombre à formater
        precision: Nombre de décimales
        
    Returns:
        String formatée (ex: "1,234.56")
    """
    if isinstance(number, int):
        return f"{number:,}"
    else:
        return f"{number:,.{precision}f}"


def get_current_timestamp() -> int:
    """
    Retourne le timestamp actuel en millisecondes.
    
    Returns:
        Timestamp en millisecondes
    """
    return int(time.time() * 1000)


def calculate_throughput(items: int, duration_seconds: float) -> float:
    """
    Calcule le throughput (items par seconde).
    
    Args:
        items: Nombre d'items traités
        duration_seconds: Durée en secondes
        
    Returns:
        Throughput en items/seconde
    """
    if duration_seconds <= 0:
        return 0.0
    return items / duration_seconds


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Division sécurisée avec valeur par défaut en cas de division par zéro.
    
    Args:
        numerator: Numérateur
        denominator: Dénominateur
        default: Valeur par défaut si division par zéro
        
    Returns:
        Résultat de la division ou valeur par défaut
    """
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ZeroDivisionError):
        return default
