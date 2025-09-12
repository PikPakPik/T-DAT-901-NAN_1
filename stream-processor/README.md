# Stream Processor

Un système de traitement de flux de données en temps réel pour l'agrégation de données de marché crypto-monnaie.

## 🚀 Vue d'ensemble

Le Stream Processor est un système modulaire qui consomme des messages Kafka contenant des données de trading (trades agrégés et klines), les agrège en métriques temporelles et les stocke dans InfluxDB pour la visualisation.

### Architecture modulaire v2.0

Le système a été refactorisé en **architecture modulaire par topic** pour une meilleure séparation des responsabilités et une extensibilité améliorée.

## 📋 Table des matières

- [Installation et configuration](#installation-et-configuration)
- [Architecture](#architecture)
- [Structure du projet](#structure-du-projet)
- [Guide de développement](#guide-de-développement)
- [Configuration](#configuration)
- [Surveillance et monitoring](#surveillance-et-monitoring)
- [Tests et debugging](#tests-et-debugging)
- [FAQ et troubleshooting](#faq-et-troubleshooting)

## 🛠 Installation et configuration

### Prérequis

- Python 3.8+
- Kafka accessible
- InfluxDB 2.0+ accessible

### Installation des dépendances

```bash
cd stream-processor
pip install -r requirements.txt
```

### Variables d'environnement

Créez un fichier `.env` ou configurez les variables suivantes :

```bash
# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_GROUP_ID=stream-processor-kafka-streams-1s
TOPIC_AGGTRADE=aggTrade
TOPIC_KLINE=kline

# Configuration InfluxDB
INFLUX_URL=http://influxdb:8086
INFLUX_TOKEN=your_token_here
INFLUX_ORG=your_org
INFLUX_BUCKET=your_bucket

# Configuration Application
WINDOW_SECONDS=1
LOG_LEVEL=INFO
INFLUX_BATCH_SIZE=100
```

### Lancement rapide

```bash
# Version nouvelle architecture (recommandée)
python main.py

# Version compatibilité
python src/stream_processor/main.py
```

## 🏗 Architecture

### Vue d'ensemble du flux

```
Messages Kafka (aggTrade/kline)
         ↓
  Stream Orchestrator
         ↓
    Topic Router
         ↓
  Handlers spécialisés
  (AggTrade/Kline)
         ↓
   Agrégateurs multiples
   (Trade/Price/Volume)
         ↓
   Métriques typées
         ↓
   Buffer InfluxDB
         ↓
  Écriture par batch
```

### Composants principaux

#### 1. Stream Orchestrator (`core/stream_orchestrator.py`)
- **Rôle** : Orchestrateur principal qui coordonne tout le flux
- **Responsabilités** :
  - Gestion des connexions Kafka et InfluxDB
  - Coordination des timers et actions périodiques
  - Gestion du buffer de métriques
  - Écriture par batch optimisée

#### 2. Topic Router (`core/topic_router.py`)
- **Rôle** : Routage intelligent des messages par topic
- **Responsabilités** :
  - Enregistrement des handlers par topic
  - Routage automatique des messages
  - Agrégation des statistiques

#### 3. Handlers (`handlers/`)
- **AggTradeHandler** : Traite les messages `aggTrade`
- **KlineHandler** : Traite les messages `kline`
- **BaseHandler** : Classe abstraite commune

#### 4. Agrégateurs (`aggregators/`)
- **TradeAggregator** : Métriques complètes de trading
- **PriceAggregator** : Métriques de prix (OHLC, VWAP, volatilité)
- **VolumeAggregator** : Métriques de volume et ratios
- **BaseAggregator** : Classe abstraite avec fenêtrage temporel

#### 5. Modèles (`models/`)
- **Records** : Structures de données d'entrée
- **Metrics** : Structures de données de sortie
- **Base Models** : Classes abstraites communes

## 📁 Structure du projet

```
stream-processor/
├── main.py                    # 🚀 Point d'entrée nouvelle architecture
├── requirements.txt           # 📦 Dépendances Python
├── Dockerfile                # 🐳 Configuration Docker
├── ARCHITECTURE.md           # 📋 Documentation architecture détaillée
├── README.md                 # 📖 Ce fichier
│
└── src/stream_processor/
    ├── main.py               # 🔄 Point d'entrée compatibilité
    │
    ├── config/               # ⚙️ Configuration
    │   ├── __init__.py
    │   └── settings.py       # Configuration centralisée avec validation
    │
    ├── core/                 # 🏗️ Cœur du système
    │   ├── __init__.py
    │   ├── stream_orchestrator.py  # Orchestrateur principal (nouveau)
    │   ├── topic_router.py         # Routage par topic (nouveau)
    │   ├── processor.py            # Ancien processeur (compatibilité)
    │   └── aggregator.py           # Ancien agrégateur (compatibilité)
    │
    ├── handlers/             # 🎯 Handlers spécialisés par topic
    │   ├── __init__.py
    │   ├── base_handler.py   # Classe de base
    │   ├── aggtrade_handler.py
    │   └── kline_handler.py
    │
    ├── aggregators/          # 📊 Agrégateurs par type de métriques
    │   ├── __init__.py
    │   ├── base_aggregator.py
    │   ├── trade_aggregator.py
    │   ├── price_aggregator.py
    │   └── volume_aggregator.py
    │
    ├── models/               # 📋 Modèles de données
    │   ├── __init__.py
    │   ├── base_models.py    # Modèles de base
    │   ├── aggtrade_models.py
    │   ├── kline_models.py
    │   └── trade_models.py   # Compatibilité
    │
    ├── clients/              # 🔌 Clients externes
    │   ├── __init__.py
    │   ├── kafka_client.py
    │   └── influxdb_client.py
    │
    └── utils/                # 🛠 Utilitaires
        ├── __init__.py
        ├── logger.py
        └── helpers.py
```

## 👨‍💻 Guide de développement

### Ajouter un nouveau type de message

1. **Créer le modèle** dans `models/`
```python
# models/my_message_models.py
@dataclass
class MyMessageRecord(BaseRecord):
    my_field: float
    
@dataclass
class MyMessageMetrics(BaseMetrics):
    my_metric: float
```

2. **Créer le handler** dans `handlers/`
```python
# handlers/my_message_handler.py
class MyMessageHandler(BaseHandler[MyMessageRecord, MyMessageMetrics]):
    def _is_valid_message(self, message: Dict[str, Any]) -> bool:
        return "my_field" in message.get("data", {})
    
    def _create_record_from_message(self, message: Dict[str, Any]) -> Optional[MyMessageRecord]:
        # Conversion du message en record
        pass
```

3. **Créer l'agrégateur** dans `aggregators/`
```python
# aggregators/my_message_aggregator.py
class MyMessageAggregator(BaseAggregator[MyMessageRecord, MyMessageMetrics]):
    def _create_window_data(self, symbol: str, window_key: str) -> 'MyMessageWindowData':
        # Logique d'agrégation
        pass
```

### Exemple de développement complet

Consultez `handlers/aggtrade_handler.py` pour un exemple complet d'implémentation.

### Points d'attention

- **Fenêtrage temporel** : Toutes les données sont agrégées par fenêtres de 1 seconde
- **Mémoire** : Cleanup automatique des anciennes fenêtres
- **Performance** : Écriture par batch pour optimiser les I/O
- **Extensibilité** : Architecture plugin-ready

## ⚙️ Configuration

### Configuration complète

```python
# Exemple de configuration avancée
config = ConfigManager()

# Paramètres Kafka
config.kafka.max_poll_records = 2000        # Messages par poll
config.kafka.fetch_max_wait_ms = 100        # Latence vs throughput

# Paramètres InfluxDB
config.influxdb.batch_size = 100            # Points par batch
config.influxdb.write_mode = "ASYNCHRONOUS" # Performance

# Paramètres Application
config.app.window_seconds = 1               # Taille des fenêtres
config.app.max_memory_windows = 1000        # Limite mémoire
```

### Variables d'environnement détaillées

| Variable | Défaut | Description |
|----------|---------|-------------|
| `WINDOW_SECONDS` | `1` | Taille des fenêtres d'agrégation |
| `INFLUX_BATCH_SIZE` | `100` | Nombre de points par batch InfluxDB |
| `MAX_MEMORY_WINDOWS` | `1000` | Limite de fenêtres en mémoire |
| `LOG_LEVEL` | `INFO` | Niveau de logging |
| `STATS_INTERVAL_MESSAGES` | `100` | Intervalle d'affichage des stats |

## 📊 Surveillance et monitoring

### Métriques disponibles

L'application expose plusieurs métriques utiles pour le monitoring :

```python
# Métriques de l'orchestrateur
{
    "messages_processed": 15234,
    "windows_completed": 892,
    "points_written": 8532,
    "throughput_msg_per_sec": 1250.5,
    "routing_success_rate": 99.8
}

# Métriques par topic
{
    "aggTrade": {
        "messages_processed": 12000,
        "active_windows": 45,
        "handlers": ["AggTradeHandler"],
        "aggregators": 3
    }
}
```

### Logs structurés

Les logs sont organisés par niveau et incluent des émojis pour une lecture facile :

```
🚀 Stream Processor - Version Modulaire
📊 Window Size: 1s
📈 Topics: aggTrade, kline
✅ All orchestrator components initialized successfully
🔄 Starting stream processing with topic routing...
```

### Dashboard Grafana

Les métriques stockées dans InfluxDB peuvent être visualisées avec les tableaux de bord Grafana fournis.

## 🧪 Tests et debugging

### Debug mode

```bash
LOG_LEVEL=DEBUG python main.py
```

### Logs utiles pour le debugging

- `🎯` : Routage des messages
- `📊` : Métriques générées  
- `⏰` : Finalisation forcée des fenêtres
- `🧹` : Cleanup mémoire
- `❌` : Erreurs de traitement

### Validation des données

Le système inclut une validation robuste à tous les niveaux :
- Validation des messages Kafka
- Validation de la configuration
- Validation des modèles de données

### Tests unitaires

```bash
# Lancer les tests unitaires
python -m pytest tests/unit/

# Lancer les tests d'intégration  
python -m pytest tests/integration/
```

## ❓ FAQ et troubleshooting

### Problèmes courants

**Q: L'application ne traite aucun message**
- Vérifiez la connexion Kafka
- Vérifiez les noms des topics
- Vérifiez le group ID Kafka

**Q: Erreurs d'écriture InfluxDB**
- Vérifiez les credentials InfluxDB
- Vérifiez les permissions sur le bucket
- Réduisez la taille des batches

**Q: Consommation mémoire élevée**
- Réduisez `MAX_MEMORY_WINDOWS`
- Augmentez la fréquence de cleanup
- Vérifiez les fenêtres bloquées

### Optimisation des performances

1. **Throughput élevé** :
   - Augmentez `INFLUX_BATCH_SIZE`
   - Utilisez le mode `ASYNCHRONOUS`
   - Augmentez `KAFKA_MAX_POLL_RECORDS`

2. **Latence faible** :
   - Réduisez `KAFKA_FETCH_MAX_WAIT_MS`
   - Réduisez `INFLUX_BATCH_SIZE`
   - Utilisez le mode `SYNCHRONOUS`

### Migration depuis l'ancienne version

L'architecture v2.0 est rétrocompatible. Vous pouvez :
- Utiliser le nouveau point d'entrée : `python main.py`
- Garder l'ancien : `python src/stream_processor/main.py`
- Migrer progressivement vos personnalisations

---

## 🤝 Contribution

Pour contribuer au projet :

1. Fork le repository
2. Créez une branche feature
3. Implémentez vos changements
4. Ajoutez des tests
5. Soumettez une pull request

### Standards de code

- Utilisez les type hints Python
- Documentez les nouvelles fonctionnalités
- Suivez le pattern d'architecture modulaire
- Ajoutez des logs appropriés avec émojis

---

## 📝 Licence

Ce projet est sous licence interne Epitech.

---

**Dernière mise à jour** : Version modulaire 2.0 - Architecture par topic avec handlers spécialisés
