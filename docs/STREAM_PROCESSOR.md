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

DuckDB est utilisé pour agréger les trades par fenêtre temporelle et symbole. Voir [DUCKDB.md](./DUCKDB.md) pour les détails complets.

**En bref :**
- Base de données analytique en mémoire (ultra-rapide)
- Agrège les trades avec SQL : `GROUP BY window_start, symbol`
- Calcule : min/max/moyenne prix, volumes buy/sell, écart-type, count
- Cycle : Insert → Aggregate → Delete → Repeat

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

## ⚙️ Paramètres principaux

- `WINDOW_SECONDS=60` : Durée fenêtre d'agrégation
- `KAFKA_BATCH_SIZE=100` : Taille max buffer avant agrégation
- `AGGR_INTERVAL_SECONDS=1.0` : Intervalle max entre agrégations
- `KAFKA_POLL_TIMEOUT_MS=100` : Timeout poll Kafka

**Optimisations :** Poll fréquent (100ms), batch petit (100), agrégation fréquente (1s), écriture asynchrone InfluxDB, DuckDB en mémoire.

