# Crypto Stream Processing - Architecture Globale

> 📚 **Documentation détaillée** : Consultez les fichiers dans `docs/` pour des explications approfondies :
> - [`docs/KAFKA.md`](docs/KAFKA.md) : Pourquoi Kafka, group_id, paramètres Kafka, fonctionnement
> - [`docs/KAFKA_PARTITIONS.md`](docs/KAFKA_PARTITIONS.md) : Comment configurer les partitions pour le scaling
> - [`docs/STREAM_PROCESSOR.md`](docs/STREAM_PROCESSOR.md) : Comment fonctionne l'agrégation, boucle principale
> - [`docs/DUCKDB.md`](docs/DUCKDB.md) : Pourquoi DuckDB, comment il agrège les données
> - [`docs/CODE.md`](docs/CODE.md) : Documentation détaillée de toutes les fonctions et du code

## 📋 Vue d'ensemble

Ce projet est un système de traitement de données cryptographiques en temps réel qui collecte, traite, agrège et visualise les trades de différentes paires de cryptomonnaies. Le système est conçu pour être scalable, performant et facile à maintenir.

## 🏗️ Architecture Globale

```
┌─────────────────────┐
│   Binance WebSocket │  (ou ws-mocked.py)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  scrapper-websocket │  (Producer Kafka)
│   - main.py         │
│   - ws-mocked.py    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│       Kafka         │  (Message Broker)
│   - Topic: trade    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  stream-processor   │  (Consumer + Aggregator)
│   - DuckDB          │
│   - Aggrégation     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│     InfluxDB        │  (Time-Series DB)
│   - crypto_aggregates│
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      Grafana        │  (Visualisation)
│   - Dashboards      │
└─────────────────────┘
```

## 🔧 Composants

### 1. **Scrapper WebSocket** (`scrapper-websocket/`)

**Rôle** : Collecte les données de trades depuis Binance via WebSocket et les envoie dans Kafka.

**Fichiers** :
- `main.py` : Connexion WebSocket à Binance réelle
- `ws-mocked.py` : Générateur de données mockées pour les tests (spam Kafka)

**Fonctionnalités** :
- Connexion asynchrone aux streams Binance (`btcusdt@trade`, `ethusdt@trade`, etc.)
- Formatage des données au format Kafka
- Gestion de la reconnexion automatique
- Compression des messages (gzip)

**Configuration** :
- `BINANCE_WEBSOCKET_URL` : URL du WebSocket Binance
- `BINANCE_AGGREGATE_TRADE_STEAMS` : Liste des streams à écouter
- `KAFKA_BOOTSTRAP_SERVERS` : Serveurs Kafka

### 2. **Kafka** (Message Broker)

**Rôle** : Intermédiaire entre la collecte et le traitement des données.

**Caractéristiques** :
- Topic : `trade`
- Compression : gzip
- Réplication : configurable via `compose.yaml`
- Interface de gestion : Kafka UI (port 9090)

**Avantages** :
- Découplage entre producteurs et consommateurs
- Buffering des messages
- Scalabilité horizontale (plusieurs partitions)
- Durabilité des données

### 3. **Stream Processor** (`stream-processor/`)

**Rôle** : Consomme les trades depuis Kafka, les agrège par fenêtre temporelle et les envoie dans InfluxDB.

**Technologies** :
- **DuckDB** : Base de données analytique en mémoire pour les agrégations SQL
- **Python** : Langage principal
- **Kafka Consumer** : Lecture des messages
- **InfluxDB Client** : Écriture asynchrone

**Métriques calculées** :
- Prix min/max/moyen
- Volume d'achat/vente/total
- Volatilité (écart-type relatif en %)
- Nombre de trades

**Fonctionnement** :
1. Poll Kafka toutes les 100ms (configurable)
2. Accumule les trades dans un buffer
3. Agrège toutes les 1 seconde (ou quand buffer plein = 100 trades)
4. Charge les données dans DuckDB
5. Exécute une requête SQL d'agrégation par fenêtre et symbole
6. Écrit les résultats dans InfluxDB (asynchrone)

**Configuration** :
- `WINDOW_SECONDS` : Durée de la fenêtre d'agrégation (défaut: 60s)
- `KAFKA_BATCH_SIZE` : Taille du buffer (défaut: 100)
- `KAFKA_POLL_TIMEOUT_MS` : Timeout de poll (défaut: 100ms)
- `KAFKA_MAX_POLL_RECORDS` : Max messages par poll (défaut: 1000)
- `AGGR_INTERVAL_SECONDS` : Intervalle d'agrégation (défaut: 1.0s)

**Scaling** :
- Supporte plusieurs instances via `docker-compose up --scale stream-processor=N`
- Kafka répartit automatiquement les partitions entre les consumers
- Chaque instance partage le même `KAFKA_GROUP_ID`

### 4. **InfluxDB** (Time-Series Database)

**Rôle** : Stockage des données agrégées avec timestamps.

**Structure** :
- **Measurement** : `crypto_aggregates`
- **Tags** : `symbol` (BTCUSDT, ETHUSDT, etc.)
- **Fields** :
  - `price_max`, `price_min`, `price_avg`
  - `volume_buy`, `volume_sell`, `volume_total`
  - `volatility`
  - `trade_count`
- **Timestamp** : Début de la fenêtre temporelle

**Avantages** :
- Optimisé pour les séries temporelles
- Requêtes rapides sur les timestamps
- Compression automatique
- Retention policies configurables

### 5. **Grafana** (Visualisation)

**Rôle** : Tableaux de bord interactifs pour visualiser les métriques.

**Dashboards** :
- `crypto-v1.json` : Dashboard simple avec prix et volumes
- `crypto-v2.json` : Dashboard avancé avec volatilité et métriques détaillées

**Fonctionnalités** :
- Variable `symbol` : Dropdown dynamique pour sélectionner la crypto
- Requêtes Flux pour InfluxDB
- Agrégation sur fenêtres de 1 minute
- Rafraîchissement automatique (10s)

**Configuration** :
- Port : 3000 (configurable via `GRAFANA_PORT`)
- Datasource InfluxDB : Configuré automatiquement via provisioning
- Dashboards : Chargés automatiquement depuis `grafana/provisioning/dashboards/`

### 6. **Kafka UI**

**Rôle** : Interface web pour monitorer Kafka.

**Fonctionnalités** :
- Visualisation des topics et partitions
- Monitoring des consumers et lag
- Consultation des messages
- Gestion des consumers groups

**Accès** : http://localhost:9090

## 📊 Flux de données

### 1. Collecte
```
Binance WebSocket → scrapper-websocket → Kafka (topic: trade)
```

### 2. Traitement
```
Kafka → stream-processor → DuckDB (agrégation) → InfluxDB
```

### 3. Visualisation
```
InfluxDB → Grafana (dashboards)
```

## 🚀 Démarrage rapide

### Prérequis
- Docker et Docker Compose
- Variables d'environnement (voir `.env.example` si disponible)

### Lancer le projet

```bash
# Lancer tous les services
docker compose up -d

# Lancer avec 3 instances de stream-processor (scaling)
docker compose up -d --scale stream-processor=3

# Voir les logs
docker compose logs -f stream-processor

# Script de démarrage (avec scaling)
./dat.sh
```

### Accès aux services

- **Grafana** : http://localhost:3000 (admin/admin par défaut)
- **InfluxDB** : http://localhost:8086
- **Kafka UI** : http://localhost:9090
- **Kafka** : localhost:9092

## 🔄 Modes de fonctionnement

### Mode Production
Utilise `scrapper-websocket/main.py` qui se connecte aux streams Binance réels.

```bash
docker compose up scrapper-websocket
```

### Mode Test/Mock
Utilise `scrapper-websocket/ws-mocked.py` qui génère des données mockées.

```bash
docker compose up scrapper-ws-mock
```

**Configuration** :
- `MOCK_EVENTS_PER_SECOND` : Nombre d'événements par seconde (défaut: 100)

## 📈 Scaling et Performance

### Configuration des partitions Kafka

**⚠️ Important :** Pour le scaling horizontal, le topic `trade` doit avoir **au moins autant de partitions que de consumers**.

**Tentative automatique :** Les scrapper tentent de créer/augmenter le topic `trade` avec **3 partitions** au démarrage, mais cela peut échouer silencieusement pour les topics existants.

**Vérifier le nombre de partitions actuelles :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade | grep PartitionCount
```

**Augmenter manuellement à 3 partitions (recommandé si auto échoue) :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic trade --partitions 3
```

**Vérifier le résultat :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade
```

Vous devriez voir 3 partitions : Partition 0, Partition 1, Partition 2

**Configuration via variables d'environnement (pour création de nouveaux topics) :**
- `TOPIC_TRADE` : Nom du topic (défaut: `trade`)
- `KAFKA_PARTITIONS` : Nombre de partitions (défaut: `3`)

> 📚 **Documentation complète :** Voir [`docs/KAFKA_PARTITIONS.md`](docs/KAFKA_PARTITIONS.md) pour tous les détails.

### Scaling horizontal du stream-processor

**Avant de scaler, vérifiez que vous avez au moins 3 partitions !**

```bash
# Démarrer 3 instances (nécessite 3 partitions minimum)
docker compose up --scale stream-processor=3
```

**Avantages :**
- Traitement parallèle des partitions Kafka
- Réduction du consumer lag
- Augmentation du throughput

**Requirements :**
- Le topic Kafka doit avoir suffisamment de partitions (≥ nombre d'instances)
- Toutes les instances doivent partager le même `KAFKA_GROUP_ID`

**Exemple avec 3 partitions et 3 processors :**
```
Partition 0 → Processor 1
Partition 1 → Processor 2
Partition 2 → Processor 3
```

Chaque processor traite sa partition en parallèle, multipliant le throughput par 3.

### Optimisation du consumer lag

Le système est optimisé pour minimiser le lag :
- Poll fréquent (100ms)
- Batch small (100 messages)
- Agrégation rapide (1 seconde)
- Écriture asynchrone vers InfluxDB
- Commit fréquent des offsets (1 seconde)

## 🔍 Monitoring

### Kafka Consumer Lag

Surveiller via Kafka UI :
- Consumer Groups → `duckdb-stream-processor`
- Lag par partition

### Métriques Grafana

- Prix min/max/moyen
- Volumes buy/sell
- Volatilité
- Nombre de trades
- Consumer lag (si configuré)

### Logs

```bash
# Stream processor
docker compose logs -f stream-processor

# Scrapper
docker compose logs -f scrapper-websocket

# Tous les services
docker compose logs -f
```

## 🛠️ Configuration avancée

### Variables d'environnement principales

**Stream Processor** :
```env
WINDOW_SECONDS=60              # Fenêtre d'agrégation (secondes)
KAFKA_BATCH_SIZE=100           # Taille du buffer
KAFKA_POLL_TIMEOUT_MS=100      # Poll timeout
AGGR_INTERVAL_SECONDS=1.0      # Intervalle d'agrégation
```

**Scrapper** :
```env
BINANCE_AGGREGATE_TRADE_STEAMS=btcusdt@trade,ethusdt@trade
MOCK_EVENTS_PER_SECOND=100     # Pour ws-mocked.py
```

### Paires de cryptos supportées

30 paires configurées dans `ws-mocked.py` :
- BTCUSDT, ETHUSDT, BNBUSDT, ADAUSDT, DOGEUSDT
- XRPUSDT, DOTUSDT, UNIUSDT, LTCUSDT, LINKUSDT
- MATICUSDT, SOLUSDT, AVAXUSDT, ATOMUSDT, ETCUSDT
- Et 15 autres...

La liste est récupérée dynamiquement dans Grafana depuis InfluxDB.

## 📁 Structure du projet

```
.
├── compose.yaml                 # Configuration Docker Compose
├── dat.sh                      # Script de démarrage rapide
├── scrapper-websocket/         # Collecteur WebSocket
│   ├── main.py                 # Connexion Binance réelle
│   ├── ws-mocked.py            # Générateur de données mockées
│   └── requirements.txt
├── stream-processor/           # Processeur de stream
│   ├── main.py                 # Logique principale
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md               # Documentation détaillée
├── grafana/
│   └── provisioning/
│       ├── dashboards/
│       │   └── crypto/
│       │       ├── crypto-v1.json
│       │       └── crypto-v2.json
│       └── datasources/
│           └── influxdb.yaml
└── README.md                   # Ce fichier
```

## 🎯 Choix techniques

### Pourquoi DuckDB ?
- **Performance** : Très rapide pour les agrégations SQL en mémoire
- **Simplicité** : Pas de cluster à gérer, démarrage instantané
- **Léger** : Beaucoup plus léger que Spark
- **SQL natif** : Facile à maintenir et déboguer

### Pourquoi Kafka ?
- **Découplage** : Séparation claire entre collecte et traitement
- **Scalabilité** : Supporte plusieurs consumers et partitions
- **Durabilité** : Les messages sont persistés
- **Performance** : Haut débit et faible latence

### Pourquoi InfluxDB ?
- **Time-series** : Optimisé pour les données temporelles
- **Performance** : Requêtes rapides sur les timestamps
- **Grafana** : Intégration native
- **Retention** : Gestion automatique de la rétention des données

## 🔒 Sécurité

- Les tokens InfluxDB sont gérés via variables d'environnement
- Kafka n'est exposé que sur le réseau Docker interne
- Grafana nécessite une authentification (configurable)

## 📝 Notes importantes

1. **Consumer Group ID** : Tous les stream-processors doivent utiliser le même `KAFKA_GROUP_ID` pour partager le travail
2. **Partitions Kafka** : Le topic doit avoir suffisamment de partitions pour le scaling
3. **Rétention InfluxDB** : Configurer des retention policies selon vos besoins
4. **Données mockées** : `ws-mocked.py` génère 30 paires de cryptos différentes

## 🤝 Contribution

Pour contribuer au projet :
1. Respecter la structure existante
2. Tester avec les données mockées d'abord
3. Vérifier les performances (consumer lag)
4. Mettre à jour la documentation

## 📚 Ressources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [InfluxDB Documentation](https://docs.influxdata.com/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Binance WebSocket API](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)

