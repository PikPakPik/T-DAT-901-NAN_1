#!/usr/bin/env python3
"""
Stream Processor Main Entry Point
=================================

Point d'entrée principal pour l'application de stream processing.
"""

import sys
import os
from pathlib import Path

# Ajouter le répertoire src au path Python
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from stream_processor.config.settings import ConfigManager
from stream_processor.utils.logger import setup_logging, get_logger
from stream_processor.core.processor import StreamProcessor


def main():
    """Point d'entrée principal de l'application."""
    try:
        # Configuration du logging
        config_manager = ConfigManager()
        setup_logging(
            level=config_manager.app.log_level.value,
            log_to_file=os.environ.get("LOG_TO_FILE", "false").lower() == "true"
        )
        logger = get_logger(__name__)
        
        # Affichage du header
        logger.info("=" * 80)
        logger.info("🚀 Stream Processor Application v2.0.0")
        logger.info("=" * 80)
        
        # Validation de la configuration
        config_manager.validate()
        logger.info("✅ Configuration validated successfully")
        
        # Affichage de la configuration
        logger.info("📋 Configuration Summary:")
        config_dict = config_manager.to_dict()
        
        logger.info(f"   • Window Size: {config_manager.app.window_seconds}s")
        logger.info(f"   • Topics: {', '.join(config_manager.app.topics)}")
        logger.info(f"   • Kafka Group: {config_manager.kafka.group_id}")
        logger.info(f"   • InfluxDB Bucket: {config_manager.influxdb.bucket}")
        logger.info(f"   • Batch Size: {config_manager.influxdb.batch_size}")
        logger.info(f"   • Write Mode: {config_manager.influxdb.write_mode.value}")
        logger.info(f"   • Log Level: {config_manager.app.log_level.value}")
        
        # Création et lancement du processeur
        processor = StreamProcessor(config_manager)
        processor.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
