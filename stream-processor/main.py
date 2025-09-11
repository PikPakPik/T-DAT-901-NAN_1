#!/usr/bin/env python3
"""
Stream Processor - Version Simple qui fonctionne
===============================================

Version simplifiée qui utilise l'ancienne architecture qui fonctionnait.
"""

import os
import sys
from pathlib import Path

# Ajouter le répertoire src au path Python
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Import de l'ancienne version qui fonctionnait
from stream_processor.core.processor import StreamProcessor
from stream_processor.config.settings import ConfigManager

def main():
    """Point d'entrée principal simplifié."""
    try:
        # Configuration simple
        config = ConfigManager()
        
        # Affichage simple
        print("🚀 Stream Processor - Version Simple")
        print(f"📊 Window Size: {config.app.window_seconds}s")
        print(f"📈 Topics: {', '.join(config.app.topics)}")
        print(f"🗄️ InfluxDB Bucket: {config.influxdb.bucket}")
        
        # Création et lancement du processeur
        processor = StreamProcessor(config)
        processor.run()
        
    except KeyboardInterrupt:
        print("🛑 Application arrêtée par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
