# Stream Processor - Fonctionnement détaillé

## 🎯 Objectif

Le stream processor consomme les trades depuis Kafka, les agrège par fenêtre temporelle et symbol, puis écrit les résultats dans InfluxDB pour visualisation dans Grafana.

## 🏗️ Architecture du processor

```
┌─────────────┐
│   Kafka     │ (Topic: trade)
└──────┬──────┘
       │ poll() toutes les 100ms
       ▼
┌─────────────────┐
│ Trades Buffer   │ (max 100 messages)
└──────┬──────────┘
       │ Toutes les 1s ou buffer plein
       ▼
┌─────────────────┐
│     DuckDB      │ (Agrégation SQL)
└──────┬──────────┘
       │ Résultats agrégés
       ▼
┌─────────────────┐
│    InfluxDB     │ (crypto_aggregates)
└─────────────────┘
```

## 📥 Consommation Kafka

### Fonctionnement du poll

```python
message_batch = consumer.poll(timeout_ms=100)
```

**Ce qui se passe :**
1. Le consumer attend jusqu'à 100ms pour recevoir des messages
2. Si des messages arrivent, ils sont retournés immédiatement
3. Si rien n'arrive après 100ms, retourne un batch vide
4. Maximum 1000 messages par poll (`max_poll_records`)

**Pourquoi ce timeout court ?**
- Permet de traiter le buffer même sans nouveaux messages
- Permet l'agrégation périodique (toutes les 1 seconde)
- Évite d'accumuler du lag

### Validation et parsing

Chaque message Kafka contient :
```json
{
  "e": "trade",
  "E": 1763117480982,  // Event time
  "s": "BTCUSDT",       // Symbol
  "t": 5488678497,      // Trade ID
  "p": "97094.00",      // Price (string)
  "q": "0.00006",       // Quantity (string)
  "T": 1763117480981,   // Trade time
  "m": true             // Is buyer maker?
}
```

**Validation :**
- Vérifie la présence des champs obligatoires : `s`, `p`, `q`, `T`, `m`
- Convertit les strings en float pour prix et quantité
- Calcule le volume : `price * quantity`

**Classification buy/sell :**
- Si `is_buyer_maker = false` → C'est un buy (buyer n'est pas maker)
- Si `is_buyer_maker = true` → C'est un sell (buyer est maker)

## 📊 Buffer et agrégation

### Trades Buffer

Les trades validés sont accumulés dans un buffer en mémoire :

```python
trades_buffer = [
    (event_time, symbol, trade_id, price, quantity, trade_time, is_buyer_maker, volume, volume_buy, volume_sell),
    ...
]
```

**Critères de déclenchement de l'agrégation :**

1. **Buffer plein** : `len(trades_buffer) >= KAFKA_BATCH_SIZE` (100 messages)
2. **Temps écoulé** : `current_time - last_aggregation_time >= AGGR_INTERVAL_SECONDS` (1 seconde)
3. **Pas de nouveaux messages** : `len(message_batch) == 0` et `len(trades_buffer) > 0`

**Pourquoi ces critères ?**
- Buffer plein → Traite immédiatement pour éviter l'accumulation
- Temps écoulé → Garantit l'agrégation même avec peu de messages (latence acceptable)
- Pas de nouveaux messages → Traite immédiatement le reste avant d'attendre

## 🗄️ DuckDB - Agrégation

### Pourquoi DuckDB ?

**Problème :** On doit agréger des milliers de trades par seconde.

**Solutions possibles :**

#### ❌ Pandas / NumPy
- Trop lent pour de gros volumes
- Consomme beaucoup de mémoire
- Pas optimisé pour les agrégations SQL

#### ❌ SQLite
- Conçu pour transactions ACID, pas pour analytics
- Lent pour les agrégations sur gros volumes
- Pas optimisé pour les requêtes analytiques

#### ❌ PostgreSQL / MySQL
- Trop lourd (nécessite serveur séparé)
- Lent pour les agrégations en mémoire
- Overhead réseau inutile

#### ✅ DuckDB
- **Optimisé pour analytics** : Conçu pour les requêtes analytiques rapides
- **En mémoire** : Pas besoin de serveur, tout en RAM (ultra-rapide)
- **SQL natif** : Requêtes SQL standards, facile à maintenir
- **Léger** : Bibliothèque Python simple, pas de Java/Cluster
- **Performance** : Agrège des milliers de lignes en millisecondes

### Schéma de la table

```sql
CREATE TABLE trades (
    event_time BIGINT,        -- Timestamp de l'événement
    symbol VARCHAR,            -- Symbole (BTCUSDT, ETHUSDT, ...)
    trade_id BIGINT,           -- ID unique du trade
    price DOUBLE,              -- Prix du trade
    quantity DOUBLE,           -- Quantité du trade
    trade_time BIGINT,         -- Timestamp du trade (en ms)
    is_buyer_maker BOOLEAN,    -- Buyer est market maker ?
    volume DOUBLE,             -- volume = price * quantity
    volume_buy DOUBLE,         -- volume si buy (is_buyer_maker = false)
    volume_sell DOUBLE         -- volume si sell (is_buyer_maker = true)
)
```

### Requête d'agrégation

```sql
SELECT 
    CAST(FLOOR(trade_time / (WINDOW_SECONDS * 1000)) * (WINDOW_SECONDS * 1000) AS BIGINT) AS window_start,
    symbol,
    MAX(price) AS price_max,
    MIN(price) AS price_min,
    AVG(price) AS price_avg,
    SUM(volume_buy) AS volume_buy,
    SUM(volume_sell) AS volume_sell,
    SUM(volume) AS volume_total,
    STDDEV(price) AS price_stddev,
    COUNT(*) AS trade_count
FROM trades
GROUP BY window_start, symbol
ORDER BY window_start, symbol
```

**Explication ligne par ligne :**

1. **window_start** : 
   ```sql
   FLOOR(trade_time / (60 * 1000)) * (60 * 1000)
   ```
   - Divise le timestamp (ms) par la taille de fenêtre (60s = 60000ms)
   - Arrondit à l'inférieur avec `FLOOR`
   - Multiplie par la taille de fenêtre pour obtenir le début de la fenêtre
   - Exemple : Trade à 123456789 → Fenêtre [123456000, 123456599]

2. **price_max / price_min** : Prix maximum et minimum dans la fenêtre

3. **price_avg** : Prix moyen (arithmétique simple)

4. **volume_buy / volume_sell** : Somme des volumes selon le type de trade

5. **volume_total** : Somme de tous les volumes

6. **price_stddev** : Écart-type des prix (pour calculer la volatilité)

7. **trade_count** : Nombre de trades dans la fenêtre

**Résultat :** Une ligne par fenêtre temporelle et par symbole.

### Cycle de vie

1. **Insertion** : `executemany()` insère tous les trades du buffer
2. **Agrégation** : Requête SQL calcule les métriques par fenêtre
3. **Vidage** : `DELETE FROM trades` pour préparer le prochain batch
4. **Répétition** : Le cycle recommence avec le nouveau batch

**Pourquoi vider la table ?**
- DuckDB est en mémoire, on peut réutiliser la même table
- Plus efficace que créer/détruire la table à chaque fois
- Garde le schéma entre les batches

## 📈 Calcul de la volatilité

La volatilité est calculée comme le **coefficient de variation** (écart-type relatif) :

```python
volatility = (price_stddev / price_avg) * 100
```

**Explication :**
- **Écart-type** : Mesure la dispersion des prix autour de la moyenne
- **Divisé par la moyenne** : Normalise pour être indépendant du niveau de prix
- **Multiplié par 100** : Exprime en pourcentage

**Exemple :**
- Prix moyen : 50000
- Écart-type : 500
- Volatilité : (500 / 50000) * 100 = 1%

**Signification :** Plus la volatilité est élevée, plus les prix varient dans la fenêtre.

## 💾 Écriture dans InfluxDB

### Conversion en Points

Chaque ligne agrégée devient un Point InfluxDB :

```python
Point("crypto_aggregates")
    .tag("symbol", "BTCUSDT")           # Tag pour filtrage rapide
    .field("price_max", 51000.0)        # Champ numérique
    .field("price_min", 49000.0)
    .field("price_avg", 50000.0)
    .field("volume_buy", 1234.56)
    .field("volume_sell", 567.89)
    .field("volume_total", 1802.45)
    .field("volatility", 1.5)           # En pourcentage
    .field("trade_count", 150)
    .time(window_dt, write_precision="s")  # Timestamp
```

**Structure InfluxDB :**
- **Measurement** : `crypto_aggregates` (équivalent à une table)
- **Tags** : `symbol` (indexé pour filtrage rapide)
- **Fields** : Toutes les métriques calculées
- **Timestamp** : Début de la fenêtre temporelle

### Écriture asynchrone

```python
write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
```

**Pourquoi asynchrone ?**
- Ne bloque pas le traitement Kafka
- Les écritures sont mises en queue
- Permet de continuer à consommer pendant l'écriture
- Améliore le throughput global

**Risque :** Si InfluxDB est lent, les écritures peuvent s'accumuler en mémoire. Dans notre cas, c'est acceptable car InfluxDB est généralement rapide.

## 🔄 Boucle principale - Flow complet

```python
while True:
    # 1. Poll Kafka (100ms timeout)
    message_batch = consumer.poll(timeout_ms=100)
    
    # 2. Parse et valide les messages
    for message in messages:
        # Extract data, validate, calculate volume
        trades_buffer.append((...))
    
    # 3. Vérifie si on doit agréger
    if should_aggregate:
        # 4. Charge dans DuckDB
        conn.executemany("INSERT INTO trades ...", trades_buffer)
        
        # 5. Exécute la requête d'agrégation
        aggregated_data = conn.execute("SELECT ... GROUP BY ...").fetchall()
        
        # 6. Vide la table
        conn.execute("DELETE FROM trades")
        
        # 7. Convertit en Points InfluxDB
        points = [Point(...) for row in aggregated_data]
        
        # 8. Écrit dans InfluxDB (asynchrone)
        write_api.write(bucket=..., record=points)
        
        # 9. Vide le buffer
        trades_buffer.clear()
```

## ⚙️ Paramètres de configuration

### `WINDOW_SECONDS`

**Valeur :** 60 secondes (par défaut)

**Rôle :** Durée de la fenêtre d'agrégation.

**Impact :**
- Plus petit = plus de granularité mais plus de points dans InfluxDB
- Plus grand = moins de points mais moins de granularité

**Dans le projet :** 60s pour équilibrer granularité et volume de données.

### `KAFKA_BATCH_SIZE`

**Valeur :** 100 messages

**Rôle :** Taille maximale du buffer avant agrégation forcée.

**Impact :**
- Plus petit = agrégation plus fréquente mais plus d'overhead
- Plus grand = moins d'agrégations mais plus de latence

**Choix :** 100 pour traiter rapidement sans trop d'overhead.

### `AGGR_INTERVAL_SECONDS`

**Valeur :** 1.0 seconde

**Rôle :** Temps maximum entre deux agrégations.

**Impact :**
- Garantit une latence maximale acceptable
- Même avec peu de messages, on agrège régulièrement

**Choix :** 1 seconde pour latence acceptable tout en gardant de la granularité.

### `KAFKA_POLL_TIMEOUT_MS`

**Valeur :** 100 ms

**Rôle :** Timeout pour le poll Kafka.

**Impact :**
- Permet de traiter le buffer même sans nouveaux messages
- Permet l'agrégation périodique

**Choix :** 100ms pour réactivité et agrégation périodique.

## 🚀 Optimisations

### Pour minimiser le lag

1. **Poll fréquent** : 100ms pour ne pas accumuler
2. **Batch petit** : 100 messages pour traiter rapidement
3. **Agrégation fréquente** : 1 seconde max pour latence acceptable
4. **Écriture asynchrone** : Ne bloque pas la consommation
5. **Pause minimale** : 1ms seulement si vraiment rien à faire

### Pour la performance

1. **DuckDB en mémoire** : Ultra-rapide pour les agrégations
2. **SQL optimisé** : DuckDB optimise automatiquement les requêtes
3. **Batch processing** : Traite plusieurs trades en une seule requête
4. **Compression Kafka** : gzip réduit la bande passante

## 📊 Exemple concret

**Input Kafka (5 trades) :**
```
Trade 1: BTCUSDT @ 50000, qty 0.1, time 1000
Trade 2: BTCUSDT @ 50100, qty 0.2, time 1050
Trade 3: ETHUSDT @ 3000, qty 1.0, time 1100
Trade 4: BTCUSDT @ 49900, qty 0.15, time 1200
Trade 5: BTCUSDT @ 50200, qty 0.1, time 1500
```

**Buffer accumulé :**
```python
trades_buffer = [
    (1000, 'BTCUSDT', 1, 50000, 0.1, 1000, False, 5000, 5000, 0),
    (1050, 'BTCUSDT', 2, 50100, 0.2, 1050, True, 10020, 0, 10020),
    (1100, 'ETHUSDT', 3, 3000, 1.0, 1100, False, 3000, 3000, 0),
    (1200, 'BTCUSDT', 4, 49900, 0.15, 1200, False, 7485, 7485, 0),
    (1500, 'BTCUSDT', 5, 50200, 0.1, 1500, True, 5020, 0, 5020)
]
```

**Fenêtres (WINDOW_SECONDS=60, trade_time en ms) :**
- Fenêtre 0-59s : Trades 1, 2, 3, 4 (trade_time 1000-1200)
- Fenêtre 60-119s : Trade 5 (trade_time 1500)

**Résultat DuckDB (après agrégation) :**
```
Fenêtre 0-59s, BTCUSDT:
- price_max: 50100
- price_min: 49900
- price_avg: 50000
- volume_buy: 12485
- volume_sell: 10020
- volume_total: 22505
- volatility: 0.8%
- trade_count: 3

Fenêtre 0-59s, ETHUSDT:
- price_max: 3000
- price_min: 3000
- price_avg: 3000
- volume_buy: 3000
- volume_sell: 0
- volume_total: 3000
- volatility: 0%
- trade_count: 1

Fenêtre 60-119s, BTCUSDT:
- price_max: 50200
- price_min: 50200
- price_avg: 50200
- volume_buy: 0
- volume_sell: 5020
- volume_total: 5020
- volatility: 0%
- trade_count: 1
```

**Points InfluxDB écrits :** 3 points (une fenêtre BTCUSDT, une ETHUSDT, une BTCUSDT)

