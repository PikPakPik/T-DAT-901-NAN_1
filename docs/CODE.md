# Documentation du code

## 📁 Structure du code

### `stream-processor/main.py`

Service principal qui consomme Kafka, agrège avec DuckDB et écrit dans InfluxDB.

#### Configuration

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_TRADE = os.getenv('TOPIC_TRADE', 'trade')
INFLUX_URL = os.getenv('INFLUX_URL', 'http://influxdb:8086')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'cryptoviz')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', 'home')
WINDOW_SECONDS = int(os.getenv('WINDOW_SECONDS', '5'))
KAFKA_BATCH_SIZE = int(os.getenv('KAFKA_BATCH_SIZE', '100'))
KAFKA_POLL_TIMEOUT_MS = int(os.getenv('KAFKA_POLL_TIMEOUT_MS', '100'))
KAFKA_MAX_POLL_RECORDS = int(os.getenv('KAFKA_MAX_POLL_RECORDS', '1000'))
AGGR_INTERVAL_SECONDS = float(os.getenv('AGGR_INTERVAL_SECONDS', '1.0'))
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'duckdb-stream-processor')
INSTANCE_ID = os.getenv('HOSTNAME', os.getenv('INSTANCE_ID', 'stream-processor-1'))
```

**Variables d'environnement :**
- `KAFKA_BOOTSTRAP_SERVERS` : Serveurs Kafka (format: host:port)
- `TOPIC_TRADE` : Topic Kafka à consommer
- `INFLUX_URL` : URL InfluxDB (http://host:port)
- `INFLUX_TOKEN` : Token d'authentification InfluxDB (requis)
- `INFLUX_ORG` : Organisation InfluxDB
- `INFLUX_BUCKET` : Bucket InfluxDB où écrire
- `WINDOW_SECONDS` : Durée des fenêtres d'agrégation (secondes)
- `KAFKA_BATCH_SIZE` : Taille max du buffer avant agrégation
- `KAFKA_POLL_TIMEOUT_MS` : Timeout pour poll Kafka (ms)
- `KAFKA_MAX_POLL_RECORDS` : Max messages par poll
- `AGGR_INTERVAL_SECONDS` : Intervalle max entre agrégations
- `KAFKA_GROUP_ID` : ID du consumer group Kafka
- `INSTANCE_ID` : Identifiant unique de l'instance (pour logs)

#### `create_kafka_consumer() -> KafkaConsumer`

Crée et configure le consumer Kafka.

**Configuration :**
- `auto_offset_reset='latest'` : Commence à lire les nouveaux messages
- `enable_auto_commit=True` : Commit automatique des offsets
- `auto_commit_interval_ms=1000` : Commit toutes les secondes
- `group_id=KAFKA_GROUP_ID` : Groupe pour partager le travail
- `consumer_timeout_ms=KAFKA_POLL_TIMEOUT_MS` : Timeout pour poll
- `max_poll_records=KAFKA_MAX_POLL_RECORDS` : Max 1000 messages/poll
- `max_poll_interval_ms=300000` : 5 minutes max entre polls

**Returns:** Consumer Kafka configuré

**Voir :** `docs/KAFKA.md` pour les détails sur le group_id et les paramètres

#### `create_influx_client() -> InfluxDBClient`

Crée le client InfluxDB.

**Vérifie :** Que `INFLUX_TOKEN` est défini (sinon raise ValueError)

**Returns:** Client InfluxDB configuré

**Raises:** ValueError si token manquant

#### `create_duckdb_connection() -> duckdb.DuckDBPyConnection`

Crée une connexion DuckDB en mémoire avec le schéma de la table trades.

**Schéma :**
- `event_time BIGINT` : Timestamp de l'événement
- `symbol VARCHAR` : Symbole crypto (BTCUSDT, etc.)
- `trade_id BIGINT` : ID unique du trade
- `price DOUBLE` : Prix
- `quantity DOUBLE` : Quantité
- `trade_time BIGINT` : Timestamp du trade (ms)
- `is_buyer_maker BOOLEAN` : Buyer est market maker ?
- `volume DOUBLE` : volume = price * quantity
- `volume_buy DOUBLE` : volume si buy
- `volume_sell DOUBLE` : volume si sell

**Returns:** Connexion DuckDB avec table créée

**Voir :** `docs/DUCKDB.md` pour plus de détails

#### `process_trades(conn: duckdb.DuckDBPyConnection, trades_data: list) -> list`

Charge les trades dans DuckDB et calcule les agrégations par fenêtre.

**Process :**
1. Insère tous les trades avec `executemany()`
2. Exécute la requête SQL d'agrégation
3. Vide la table avec `DELETE FROM trades`
4. Retourne les résultats

**Fenêtres :** Calculées avec `FLOOR(trade_time / (WINDOW_SECONDS * 1000)) * (WINDOW_SECONDS * 1000)`

**Agrégations :**
- `MAX(price)`, `MIN(price)`, `AVG(price)`
- `SUM(volume_buy)`, `SUM(volume_sell)`, `SUM(volume)`
- `STDDEV(price)` pour volatilité
- `COUNT(*)` pour nombre de trades

**Returns:** Liste de tuples `(window_start, symbol, price_max, price_min, price_avg, volume_buy, volume_sell, volume_total, price_stddev, trade_count)`

**Voir :** `docs/STREAM_PROCESSOR.md` pour le détail de l'agrégation

#### `write_to_influxdb(aggregated_data: list, write_api) -> None`

Convertit les données agrégées en Points InfluxDB et les écrit.

**Process :**
1. Calcule la volatilité : `(price_stddev / price_avg) * 100`
2. Convertit `window_start` (timestamp ms) en `datetime`
3. Crée un Point InfluxDB pour chaque ligne
4. Écrit tous les points en batch (asynchrone)

**Structure Point :**
- Measurement: `crypto_aggregates`
- Tag: `symbol` (BTCUSDT, ETHUSDT, ...)
- Fields: `price_max`, `price_min`, `price_avg`, `volume_buy`, `volume_sell`, `volume_total`, `volatility`, `trade_count`
- Time: `window_dt` (début de la fenêtre)

**Écriture asynchrone :** Ne bloque pas le traitement Kafka

#### `main() -> None`

Point d'entrée principal du stream processor.

**Flow :**
1. Affiche la configuration
2. Initialise les connexions (Kafka, InfluxDB, DuckDB)
3. Boucle infinie :
   - Poll Kafka (100ms timeout)
   - Parse et valide les messages
   - Accumule dans `trades_buffer`
   - Vérifie si agrégation nécessaire (buffer plein OU 1s écoulé OU pas de nouveaux messages)
   - Si oui : charge dans DuckDB, agrège, écrit dans InfluxDB, vide le buffer
   - Pause 1ms si vraiment rien à faire
4. Gestion des erreurs et nettoyage à l'arrêt

**Critères d'agrégation :**
```python
should_aggregate = (
    len(trades_buffer) >= KAFKA_BATCH_SIZE or
    (current_time - last_aggregation_time) >= AGGR_INTERVAL_SECONDS or
    (len(trades_buffer) > 0 and len(message_batch) == 0)
)
```

**Gestion d'erreurs :**
- Messages invalides : ignorés, log warning
- Erreurs d'agrégation : log error, traceback, vide le buffer
- Erreurs dans la boucle : log error, attend 1s avant de réessayer

### `scrapper-websocket/main.py`

Collecte les trades depuis Binance WebSocket et les envoie dans Kafka.

#### `listen_stream(url: str, stream_type: str, handler) -> None`

Écoute un stream WebSocket Binance et appelle le handler pour chaque message.

**Process :**
1. Connecte au WebSocket
2. Boucle infinie :
   - Reçoit un message JSON
   - Parse le JSON
   - Appelle le handler avec les données
   - En cas d'erreur : log et break (reconnexion gérée par le script principal)

**Format attendu :** JSON avec structure Binance WebSocket

**Args:**
- `url`: URL du stream WebSocket
- `stream_type`: Type de stream (pour logs)
- `handler`: Fonction async appelée avec chaque message

#### `agg_trade_handler(producer: KafkaProducer)`

Crée un handler pour les événements de trade agrégés Binance.

**Process :**
1. Extrait les champs nécessaires : `["e", "E", "s", "t", "p", "q", "T", "m"]`
2. Filtre depuis `data['data']` (format Binance avec wrapper "data")
3. Envoie dans Kafka topic "trade"
4. Flush immédiat pour garantir l'envoi

**Format sortie Kafka :**
```json
{
  "e": "trade",
  "E": 1763117480982,
  "s": "BTCUSDT",
  "t": 5488678497,
  "p": "97094.00",
  "q": "0.00006",
  "T": 1763117480981,
  "m": true
}
```

**Returns:** Fonction async handler

**Reference:** https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams

#### `create_kafka_producer() -> KafkaProducer`

Crée un producteur Kafka avec gestion de la reconnexion automatique.

**Process :**
1. Tente de se connecter à Kafka
2. Valide la connexion en fetchant les métadonnées du topic "trade"
3. Si échec : attend 3s et réessaye indéfiniment
4. Configuration :
   - `value_serializer`: JSON encode UTF-8
   - `compression_type='gzip'`: Compression des messages

**Returns:** Producteur Kafka configuré

**Gestion d'erreurs :** Retry infini jusqu'à connexion réussie

#### `main() -> None`

Point d'entrée principal : connecte aux streams Binance et envoie dans Kafka.

**Configuration :**
- `BINANCE_WEBSOCKET_URL`: URL de base des streams Binance
- `BINANCE_AGGREGATE_TRADE_STEAMS`: Liste des streams (ex: "btcusdt@trade,ethusdt@trade")

**Process :**
1. Parse les variables d'environnement
2. Formate l'URL WebSocket complète
3. Crée le producteur Kafka
4. Lance l'écoute asynchrone des streams
5. Gère les interruptions (KeyboardInterrupt)

### `scrapper-websocket/ws-mocked.py`

Générateur de données mockées pour les tests.

#### Configuration

```python
CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", ..., "ARBUSDT"]  # 30 paires
PRICE_RANGES = {"BTCUSDT": (40000, 50000), ...}        # Ranges réalistes
price_cache = {symbol: random.uniform(...)}             # Prix actuels par symbole
```

**30 paires cryptos** : Liste complète dans `CRYPTO_PAIRS`

**Ranges de prix** : Prix initialisés dans des ranges réalistes selon le symbole

**Cache de prix** : Évolue avec des variations réalistes (±0.1%)

#### `create_kafka_producer() -> KafkaProducer`

Identique à `main.py`, crée un producteur Kafka avec retry.

#### `generate_aggtrade_data(symbol: str) -> dict`

Génère un événement de trade agrégé mocké au format Kafka.

**Process :**
1. Récupère le prix actuel du cache
2. Applique une variation aléatoire ±0.1% (réaliste)
3. Met à jour le cache avec le nouveau prix
4. Génère un trade avec prix, quantité aléatoire, timestamps

**Format retour :**
```json
{
  "e": "trade",
  "E": 1234567890,
  "s": "BTCUSDT",
  "t": 5488678497,
  "p": "50000.12345678",
  "q": "0.12345678",
  "T": 1234567890,
  "m": false
}
```

**Returns:** Dictionnaire au format Kafka attendu par le processor

#### `main() -> None`

Point d'entrée : génère des trades mockés et les envoie dans Kafka.

**Configuration :**
- `MOCK_EVENTS_PER_SECOND`: Nombre d'événements par seconde (défaut: 100)

**Process :**
1. Calcule le délai entre événements : `1.0 / events_per_second`
2. Crée le producteur Kafka
3. Boucle infinie :
   - Sélectionne une crypto aléatoirement
   - Génère un trade mocké
   - Envoie dans Kafka topic "trade"
   - Affiche stats toutes les 10 secondes
   - Attend le délai pour respecter le taux
4. Gère les interruptions et affiche les stats finales

**Statistiques :**
- Compte total d'événements
- Taux réel (événements/seconde)
- Affichage toutes les 10 secondes

## 🔄 Flux de données complet

### Production (main.py)

```
Binance WebSocket
    ↓ (JSON messages)
listen_stream()
    ↓ (parsed JSON)
agg_trade_handler()
    ↓ (filtered dict)
Kafka Producer
    ↓ (gzip compressed)
Topic "trade" (Kafka)
```

### Mock (ws-mocked.py)

```
generate_aggtrade_data()
    ↓ (random trade dict)
Kafka Producer
    ↓ (gzip compressed)
Topic "trade" (Kafka)
```

### Consommation et agrégation

```
Topic "trade" (Kafka)
    ↓ (poll every 100ms)
Kafka Consumer
    ↓ (parse & validate)
trades_buffer
    ↓ (every 1s or 100 trades)
DuckDB INSERT
    ↓ (SQL aggregation)
DuckDB SELECT (GROUP BY)
    ↓ (aggregated results)
write_to_influxdb()
    ↓ (Point conversion)
InfluxDB (async write)
    ↓ (time-series data)
Grafana Dashboards
```

## 🎯 Points clés du code

### Gestion des erreurs

**Messages invalides :**
- Validation des champs obligatoires
- Conversion price/quantity en float (catch ValueError)
- Ignorés silencieusement (log si nécessaire)

**Erreurs d'agrégation :**
- Try/except autour de `process_trades()` et `write_to_influxdb()`
- Traceback affiché pour debugging
- Buffer vidé pour éviter les duplications

**Erreurs critiques :**
- Connexions : Retry automatique (Kafka producer)
- Fatal errors : Traceback complet + exit code 1

### Performance

**Optimisations :**
- Poll court (100ms) pour réactivité
- Batch processing dans DuckDB (executemany)
- Écriture asynchrone InfluxDB (non-bloquant)
- Pause minimale (1ms) seulement si inactif

**Bottlenecks potentiels :**
- Kafka poll timeout (optimisé à 100ms)
- DuckDB agrégation (optimisé avec SQL)
- InfluxDB write (asynchrone donc non-bloquant)

### Scaling

**Horizontal scaling :**
- Tous les processors utilisent le même `KAFKA_GROUP_ID`
- Kafka répartit automatiquement les partitions
- Chaque instance traite une partie des données

**Comment scaler :**
```bash
docker compose up --scale stream-processor=3
```

**Requirements :**
- Topic Kafka avec au moins N partitions (N = nombre d'instances)
- Même `KAFKA_GROUP_ID` pour toutes les instances

## 📝 Format des données

### Format Kafka (topic "trade")

```json
{
  "e": "trade",           // Event type (always "trade")
  "E": 1763117480982,     // Event time (timestamp ms)
  "s": "BTCUSDT",         // Symbol
  "t": 5488678497,        // Trade ID
  "p": "97094.00000000",  // Price (string, 8 decimals)
  "q": "0.00006000",      // Quantity (string, 8 decimals)
  "T": 1763117480981,     // Trade time (timestamp ms)
  "m": true               // Is buyer market maker? (boolean)
}
```

### Format DuckDB (table trades)

```python
(
    event_time: int,      # BIGINT - Event time (ms)
    symbol: str,          # VARCHAR - Symbol
    trade_id: int,        # BIGINT - Trade ID
    price: float,         # DOUBLE - Price
    quantity: float,      # DOUBLE - Quantity
    trade_time: int,      # BIGINT - Trade time (ms)
    is_buyer_maker: bool, # BOOLEAN
    volume: float,        # DOUBLE - price * quantity
    volume_buy: float,    # DOUBLE - volume if not is_buyer_maker
    volume_sell: float    # DOUBLE - volume if is_buyer_maker
)
```

### Format InfluxDB (measurement crypto_aggregates)

```
Measurement: crypto_aggregates

Tags:
  symbol: "BTCUSDT" | "ETHUSDT" | ...

Fields:
  price_max: float
  price_min: float
  price_avg: float
  volume_buy: float
  volume_sell: float
  volume_total: float
  volatility: float (percentage)
  trade_count: int

Time: window_start (datetime, precision: seconds)
```

## 🐛 Debugging

### Logs importants

**Démarrage :**
- `🚀 Starting DuckDB Stream Processor...`
- Configuration affichée
- `✅ All connections established`

**Traitement :**
- `⚠️ Error processing message:` - Message invalide ignoré
- `❌ Error in aggregation:` - Erreur lors de l'agrégation
- `✅ Written N points to InfluxDB` - Écriture réussie

**Arrêt :**
- `🛑 Stopping stream processor...`
- `✅ Stream processor stopped`

### Points de vérification

1. **Consumer lag** : Vérifier dans Kafka UI (http://localhost:9090)
2. **Messages dans InfluxDB** : Vérifier avec Grafana ou InfluxDB UI
3. **Logs d'erreur** : Chercher `⚠️` ou `❌` dans les logs
4. **Buffer size** : Vérifier que les agrégations se déclenchent régulièrement

### Tests locaux

**Tester avec données mockées :**
```bash
docker compose up scrapper-ws-mock stream-processor
```

**Vérifier les données dans InfluxDB :**
- Via Grafana : http://localhost:3000
- Via InfluxDB CLI ou UI

