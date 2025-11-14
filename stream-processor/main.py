#!/usr/bin/env python3
"""
Stream Processor - Version Modulaire Refactorisée
===================================================

Version refactorisée avec architecture modulaire par topic :
- Handlers spécialisés par topic (trade, kline)
- Agrégateurs séparés par type de métriques (trade, price, volume)
- Routage automatique des messages par topic
- Architecture plus maintenable et extensible
"""

import os
import sys
from pathlib import Path

# Ajouter le répertoire src au path Python
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Import de la nouvelle architecture modulaire
from stream_processor.core.stream_orchestrator import StreamOrchestrator
from stream_processor.config.settings import ConfigManager

def main():
    """Point d'entrée principal simplifié."""
    try:
        # Configuration simple
        config = ConfigManager()
        
        # Affichage simple
        print("🚀 Stream Processor - Version Modulaire")
        print(f"📊 Window Size: {config.app.window_seconds}s")
        print(f"📈 Topics: {', '.join(config.app.topics)}")
        print(f"🗄️ InfluxDB Bucket: {config.influxdb.bucket}")
        print("🏗️ Architecture: Topic-based routing with specialized handlers")
        
        # Création et lancement de l'orchestrateur
        orchestrator = StreamOrchestrator(config)
        orchestrator.run()
        
    except KeyboardInterrupt:
        print("🛑 Application arrêtée par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
