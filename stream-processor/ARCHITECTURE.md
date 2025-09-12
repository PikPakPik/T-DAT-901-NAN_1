# Stream Processor - Architecture Modulaire

## Vue d'ensemble

L'application stream-processor a été refactorisée pour séparer clairement les responsabilités par topic et par type d'agrégation, éliminant le problème du "fourre-tout" de l'ancienne version.

## Nouvelle Architecture

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

## Composants clés

### 1. Stream Orchestrator (`core/stream_orchestrator.py`)
- **Rôle** : Orchestrateur principal qui remplace l'ancien `StreamProcessor`
- **Responsabilités** :
  - Coordination générale du flux de données
  - Gestion des timers et actions périodiques
  - Interface avec Kafka et InfluxDB
  - Écriture par batch optimisée

### 2. Topic Router (`core/topic_router.py`)
- **Rôle** : Routage intelligent des messages par topic
- **Responsabilités** :
  - Enregistrement des handlers par topic
  - Routage automatique des messages
  - Agrégation des statistiques par topic
  - Gestion centralisée des fenêtres temporelles

### 3. Handlers (`handlers/`)
- **Rôle** : Traitement spécialisé par type de données
- **AggTradeHandler** :
  - Traite les messages `aggTrade`
  - Utilise 3 agrégateurs spécialisés (trade, price, volume)
  - Validation spécifique aux trades
- **KlineHandler** :
  - Traite les messages `kline`
  - Structure prête pour les agrégateurs de candlesticks

### 4. Agrégateurs (`aggregators/`)
- **TradeAggregator** : Métriques complètes de trading
- **PriceAggregator** : Focus sur les métriques de prix (OHLC, VWAP, volatilité)
- **VolumeAggregator** : Focus sur les volumes et ratios buy/sell

### 5. Modèles (`models/`)
- **Spécialisés par topic** : `AggTradeRecord`, `KlineRecord`
- **Métriques typées** : `AggTradeMetrics`, `KlineMetrics`
- **Architecture héritée** : Classes de base abstraites

## Avantages de la nouvelle architecture

### ✅ Séparation claire des responsabilités
- Chaque topic a son handler dédié
- Chaque type d'agrégation est isolé
- Plus de mélange de logiques métier

### ✅ Extensibilité
- Ajout facile de nouveaux topics (créer un handler)
- Ajout facile de nouveaux types d'agrégation
- Architecture plugin-ready

### ✅ Maintenabilité
- Code plus lisible et organisé
- Tests unitaires plus faciles
- Debugging simplifié

### ✅ Performance
- Traitement spécialisé par type de données
- Pas de logique conditionnelle complexe
- Optimisations ciblées possibles

### ✅ Compatibilité
- Anciens modules préservés
- Migration progressive possible
- Pas de breaking changes

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

## Points d'entrée

### Nouveau (recommandé)
```python
from stream_processor.core import StreamOrchestrator
orchestrator = StreamOrchestrator(config)
orchestrator.run()
```

### Ancien (compatibilité)
```python
from stream_processor.core import StreamProcessor
processor = StreamProcessor(config)
processor.run()
```

## Configuration

Aucun changement de configuration nécessaire. L'application utilise la même configuration mais avec une architecture interne complètement refactorisée.

## Migration

L'application peut être utilisée immédiatement avec la nouvelle architecture. L'ancien code reste disponible pour compatibilité mais est désormais considéré comme obsolète.
