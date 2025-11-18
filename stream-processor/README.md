# Stream Processor avec DuckDB

Ce service traite les données de trades cryptographiques depuis Kafka et les agrège avant de les envoyer dans InfluxDB.

## Architecture

### Choix de DuckDB

Le service utilise **DuckDB**, une base de données analytique en mémoire, pour l'agrégation des données.

**Avantages de DuckDB :**

1. **Performance exceptionnelle** : DuckDB est optimisé pour l'analyse de données en mémoire, très rapide pour les agrégations SQL.

2. **Simplicité** : Pas besoin de cluster, configuration minimale, démarrage rapide.

3. **SQL natif** : Utilise SQL standard pour les agrégations, facile à maintenir et à comprendre.

4. **Léger** : Beaucoup plus léger que Spark, pas besoin de Java ni de services supplémentaires.

5. **Micro-batching efficace** : Lit les données depuis Kafka par batches, les charge dans DuckDB, agrège avec SQL, puis envoie dans InfluxDB.

6. **Compatibilité Python** : API Python native, intégration simple.

**Approche micro-batch :**
- Lit les messages depuis Kafka par batches (configurable)
- Charge les données dans DuckDB en mémoire
- Exécute des requêtes SQL d'agrégation
- Envoie les résultats dans InfluxDB
- Répète le processus toutes les N secondes (fenêtre temporelle)

## Fonctionnalités

Le service agrège les trades par fenêtre temporelle (configurable, défaut: 5 secondes) et calcule :

- **Prix maximum** : Le prix le plus élevé dans la fenêtre
- **Prix minimum** : Le prix le plus bas dans la fenêtre
- **Prix moyen** : Moyenne des prix dans la fenêtre
- **Volume d'achat** : Somme des volumes où le buyer n'est pas market maker
- **Volume de vente** : Somme des volumes où le buyer est market maker
- **Volume total** : Somme de tous les volumes
- **Volatilité** : Écart-type relatif (coefficient de variation) en pourcentage
- **Nombre de trades** : Nombre de trades dans la fenêtre

## Structure des données InfluxDB

### Measurement : `crypto_aggregates`

**Tags :**
- `symbol` : Symbole de la crypto (ex: BTCUSDT, ETHUSDT)

**Fields :**
- `price_max` : Prix maximum (float)
- `price_min` : Prix minimum (float)
- `price_avg` : Prix moyen (float)
- `volume_buy` : Volume d'achat (float)
- `volume_sell` : Volume de vente (float)
- `volume_total` : Volume total (float)
- `volatility` : Volatilité en pourcentage (float)
- `trade_count` : Nombre de trades (integer)

**Timestamp :** Début de la fenêtre temporelle

## Configuration

Variables d'environnement :

- `KAFKA_BOOTSTRAP_SERVERS` : Serveurs Kafka (défaut: kafka:29092)
- `TOPIC_TRADE` : Topic Kafka à consommer (défaut: trade)
- `INFLUX_URL` : URL InfluxDB (défaut: http://influxdb:8086)
- `INFLUX_TOKEN` : Token d'authentification InfluxDB (requis)
- `INFLUX_ORG` : Organisation InfluxDB (défaut: cryptoviz)
- `INFLUX_BUCKET` : Bucket InfluxDB (défaut: home)
- `WINDOW_SECONDS` : Durée de la fenêtre d'agrégation en secondes (défaut: 5)
- `KAFKA_BATCH_SIZE` : Nombre de messages à accumuler avant agrégation (défaut: 1000)
- `KAFKA_POLL_TIMEOUT_MS` : Timeout pour la lecture Kafka en millisecondes (défaut: 1000)
- `KAFKA_GROUP_ID` : ID du groupe de consumers Kafka (défaut: tdat901nan1)
- `INSTANCE_ID` : Identifiant unique de l'instance (auto-généré depuis HOSTNAME)

## Format des données Kafka

Les données attendues dans le topic Kafka doivent être au format JSON :

```json
{
    "e": "trade",
    "E": 1763117480982,
    "s": "BTCUSDT",
    "t": 5488678497,
    "p": "97094.00000000",
    "q": "0.00006000",
    "T": 1763117480981,
    "m": true
}
```

Où :
- `e` : Type d'événement ("trade")
- `E` : Temps de l'événement (timestamp)
- `s` : Symbole (BTCUSDT, ETHUSDT, etc.)
- `t` : ID du trade
- `p` : Prix (string)
- `q` : Quantité (string)
- `T` : Temps du trade (timestamp)
- `m` : Is buyer market maker? (boolean)

## Déploiement

Le service est déployé via Docker Compose. Le Dockerfile installe :
- Python 3.11
- Les dépendances Python (DuckDB, kafka-python, InfluxDB client)

**Avantages par rapport à Spark :**
- ✅ Build Docker beaucoup plus rapide (pas de Java/Spark à télécharger)
- ✅ Image Docker plus légère
- ✅ Démarrage plus rapide
- ✅ Moins de ressources système nécessaires
- ✅ Configuration simplifiée (pas de cluster à gérer)

## Fonctionnement

Le service fonctionne en mode micro-batch :
1. Lit les messages depuis Kafka par batches
2. Accumule les trades dans un buffer
3. Toutes les N secondes (ou quand le buffer est plein), charge les données dans DuckDB
4. Exécute une requête SQL d'agrégation par fenêtre temporelle et symbole
5. Envoie les résultats agrégés dans InfluxDB
6. Vide le buffer et répète le processus

## Scaling - Plusieurs Consumers Actifs

Le service supporte le scaling horizontal avec plusieurs consumers actifs :

**Comment ça fonctionne :**
- Tous les consumers partagent le même `KAFKA_GROUP_ID`
- Kafka répartit automatiquement les partitions du topic entre les consumers
- Chaque consumer traite une partie des données en parallèle
- Les données agrégées sont écrites dans InfluxDB par tous les consumers

**Pour démarrer plusieurs consumers :**
```bash
# Démarrer avec 3 consumers
docker-compose up --scale stream-processor=3

# Démarrer avec 5 consumers
docker-compose up --scale stream-processor=5
```

**Avantages :**
- ✅ Augmentation du throughput de traitement
- ✅ Répartition automatique de la charge par Kafka
- ✅ Redondance : si un consumer tombe, les autres continuent
- ✅ Scalabilité horizontale selon la charge

**Note importante :** 
- Assurez-vous que le topic Kafka a suffisamment de partitions (au moins autant que de consumers souhaités)
- Tous les consumers doivent utiliser le même `KAFKA_GROUP_ID` pour partager le travail

