# Documentation du code

## 📁 Structure du code

### `stream-processor/main.py`

Service principal qui consomme Kafka, agrège avec DuckDB et écrit dans InfluxDB.

**Voir [STREAM_PROCESSOR.md](./STREAM_PROCESSOR.md) pour le fonctionnement détaillé.**

**Fonctions principales :**
- `create_kafka_consumer()` : Consumer Kafka configuré
- `create_influx_client()` : Client InfluxDB
- `create_duckdb_connection()` : Connexion DuckDB en mémoire
- `process_trades()` : Agrège les trades par fenêtre temporelle
- `write_to_influxdb()` : Écrit les résultats dans InfluxDB
- `main()` : Boucle principale (poll → buffer → agrégation → écriture)

**Variables d'environnement principales :**
- `KAFKA_BOOTSTRAP_SERVERS`, `TOPIC_TRADE`, `KAFKA_GROUP_ID`
- `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`
- `WINDOW_SECONDS`, `KAFKA_BATCH_SIZE`, `AGGR_INTERVAL_SECONDS`

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

**Voir [STREAM_PROCESSOR.md](./STREAM_PROCESSOR.md) pour le flux de données complet et les détails de fonctionnement.**

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

