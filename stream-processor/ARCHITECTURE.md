# Stream Processor - Architecture Modulaire

> 📚 **Documentation complémentaire** : Voir [AGGREGATION.md](./AGGREGATION.md) pour une explication détaillée de l'agrégation (quoi, pourquoi, comment).

## Structure des dossiers

```
stream_processor/
├── handlers/              # 🎯 Handlers spécialisés par topic
│   ├── base_handler.py    # Classe de base pour tous les handlers
│   └── aggtrade_handler.py # Handler pour les trades
│
├── aggregators/           # 📊 Agrégateurs séparés par type de métriques
│   ├── base_aggregator.py # Classe de base pour tous les agrégateurs
│   ├── trade_aggregator.py # Agrégateur complet de trades
│   ├── price_aggregator.py # Agrégateur spécialisé prix
│   └── volume_aggregator.py # Agrégateur spécialisé volume
│
├── models/                # 📋 Modèles spécialisés par type de données
│   ├── base_models.py     # Modèles de base abstraits
│   ├── aggtrade_models.py # Modèles pour les trades
│   └── trade_models.py    # Anciens modèles (compatibilité)
│
└── core/                  # 🏗️ Architecture orchestrée
    ├── stream_orchestrator.py # Orchestrateur principal
    ├── topic_router.py    # Routage des messages par topic
    ├── processor.py       # Ancien processor (compatibilité)
    └── aggregator.py      # Ancien agrégateur (compatibilité)
```


## Flux de données

```
Message Kafka
     ↓
Stream Orchestrator
     ↓
Topic Router
     ↓
Handler spécialisé (trade)
     ↓
Agrégateurs multiples (trade/price/volume)
     ↓
Métriques typées
     ↓
Buffer InfluxDB
     ↓
Écriture par batch
```