# Stream Processor - Architecture Modulaire


### Structure des dossiers

```
stream_processor/
├── handlers/              # 🎯 Handlers spécialisés par topic
│   ├── base_handler.py    # Classe de base pour tous les handlers
│   ├── aggtrade_handler.py # Handler pour les trades agrégés
│   └── kline_handler.py   # Handler pour les klines/candlesticks
│
├── aggregators/           # 📊 Agrégateurs séparés par type de métriques
│   ├── base_aggregator.py # Classe de base pour tous les agrégateurs
│   ├── trade_aggregator.py # Agrégateur complet de trades
│   ├── price_aggregator.py # Agrégateur spécialisé prix
│   └── volume_aggregator.py # Agrégateur spécialisé volume
│
├── models/                # 📋 Modèles spécialisés par type de données
│   ├── base_models.py     # Modèles de base abstraits
│   ├── aggtrade_models.py # Modèles pour aggTrade
│   ├── kline_models.py    # Modèles pour klines
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
Handler spécialisé (aggTrade/kline)
     ↓
Agrégateurs multiples (trade/price/volume)
     ↓
Métriques typées
     ↓
Buffer InfluxDB
     ↓
Écriture par batch
```