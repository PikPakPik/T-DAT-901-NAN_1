# DuckDB - Pourquoi et comment

## 🤔 Pourquoi DuckDB dans ce projet ?

### Besoin : Agrégation rapide en mémoire

Nous devons agréger des milliers de trades par seconde :
- Regrouper par fenêtre temporelle (60 secondes)
- Regrouper par symbole (BTCUSDT, ETHUSDT, ...)
- Calculer des statistiques : min, max, moyenne, écart-type, sommes, comptes

### Comparaison avec d'autres solutions

#### ❌ Pandas

**Problèmes :**
- Consomme beaucoup de mémoire (copie les données)
- Lent pour les gros DataFrames
- Pas optimisé pour les agrégations SQL
- Syntaxe verbeuse pour les groupby complexes

**Quand c'est OK :** Petits datasets, transformations complexes, exploration de données

#### ❌ SQLite

**Problèmes :**
- Conçu pour transactions ACID, pas analytics
- Pas optimisé pour les requêtes analytiques
- Plus lent que DuckDB pour les agrégations
- Pas de types optimisés pour analytics (pas de vecteurisation)

**Quand c'est OK :** Stockage simple, transactions, applications légères

#### ❌ PostgreSQL/MySQL

**Problèmes :**
- Nécessite un serveur séparé (overhead réseau)
- Trop lourd pour un use case simple
- Overhead de connexion/authentification
- Pas optimisé pour analytics (optimisé pour OLTP)

**Quand c'est OK :** Applications complexes, transactions, accès concurrent

#### ❌ Spark

**Problèmes :**
- Très lourd (nécessite JVM, cluster)
- Configuration complexe
- Overhead énorme pour un use case simple
- Temps de démarrage long

**Quand c'est OK :** Big data (TB+), cluster distribué, pipelines complexes

#### ✅ DuckDB

**Avantages :**

1. **Performance exceptionnelle** :
   - Optimisé pour les requêtes analytiques
   - Vecteurisation automatique (SIMD)
   - Agrège des milliers de lignes en millisecondes

2. **Simplicité** :
   - Bibliothèque Python simple (`pip install duckdb`)
   - Pas de serveur à gérer
   - Pas de configuration complexe

3. **SQL natif** :
   - Syntaxe SQL standard
   - Facile à comprendre et maintenir
   - Requêtes optimisées automatiquement

4. **En mémoire** :
   - Tout en RAM (ultra-rapide)
   - Pas de I/O disque
   - Parfait pour notre use case (batch temporaires)

5. **Léger** :
   - Pas de Java/Cluster
   - Pas de dépendances lourdes
   - Démarrage instantané

6. **Type system adapté** :
   - Types optimisés pour analytics (DOUBLE, BIGINT)
   - Pas de overhead de types complexes

## 🏗️ Comment fonctionne DuckDB ?

### Architecture

DuckDB est une base de données analytique **in-process** (dans le processus Python).

```
┌─────────────────────────┐
│   Python Process        │
│  ┌───────────────────┐  │
│  │  stream-processor │  │
│  │       └─┬─────────┘  │
│  │         │            │
│  │  ┌──────▼──────┐    │
│  │  │   DuckDB    │    │  ← Bibliothèque C++ dans le processus
│  │  │  (in-memory)│    │
│  │  └─────────────┘    │
│  └──────────────────────┘
└─────────────────────────┘
```

**Pas de serveur** : DuckDB tourne directement dans le processus Python, pas de communication réseau.

### Mode de fonctionnement dans notre projet

#### 1. Connexion en mémoire

```python
conn = duckdb.connect(':memory:')
```

**`:memory:`** signifie que tout est en RAM, aucune persistance sur disque.

**Pourquoi :**
- Nos données sont temporaires (juste pour l'agrégation)
- Pas besoin de persistance (déjà dans Kafka et InfluxDB)
- Ultra-rapide (pas de I/O disque)

#### 2. Schéma de table

```sql
CREATE TABLE trades (
    event_time BIGINT,
    symbol VARCHAR,
    trade_id BIGINT,
    price DOUBLE,
    quantity DOUBLE,
    trade_time BIGINT,
    is_buyer_maker BOOLEAN,
    volume DOUBLE,
    volume_buy DOUBLE,
    volume_sell DOUBLE
)
```

**Types choisis :**
- `BIGINT` : Pour les timestamps (ms) et IDs (grands nombres)
- `DOUBLE` : Pour les prix et volumes (précision décimale)
- `VARCHAR` : Pour les symboles (chaînes de caractères)
- `BOOLEAN` : Pour is_buyer_maker (true/false)

#### 3. Insertion par batch

```python
conn.executemany("""
    INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", trades_data)
```

**`executemany()`** : Insère toutes les lignes en une seule opération optimisée.

**Performance :**
- Beaucoup plus rapide que des INSERT individuels
- DuckDB optimise automatiquement le batch
- En moyenne : 10,000+ lignes/seconde

#### 4. Requête d'agrégation

```sql
SELECT 
    window_start,
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
```

**Ce qui se passe :**
1. DuckDB lit toutes les lignes de la table
2. Groupe par `(window_start, symbol)`
3. Calcule les agrégations pour chaque groupe
4. Retourne les résultats

**Optimisations automatiques :**
- Vecteurisation : Traite plusieurs lignes en parallèle (SIMD)
- Cache-aware : Optimisé pour le cache CPU
- Query planning : Choisit le meilleur plan d'exécution

**Performance :**
- Pour 1000 lignes : ~1-5ms
- Pour 10,000 lignes : ~5-20ms
- Pour 100,000 lignes : ~50-200ms

#### 5. Vidage pour le prochain batch

```sql
DELETE FROM trades
```

**Pourquoi :**
- On réutilise la même table pour chaque batch
- Plus efficace que DROP/CREATE
- Le schéma reste en place

### Cycle de vie d'un batch

```
1. Buffer accumule 100 trades
   └─> trades_buffer = [trade1, trade2, ..., trade100]

2. Insertion dans DuckDB
   └─> INSERT INTO trades VALUES (?, ...) × 100

3. Agrégation SQL
   └─> SELECT ... GROUP BY window_start, symbol
   └─> Résultat : 5 lignes (ex: 3 fenêtres BTCUSDT, 2 ETHUSDT)

4. Vidage de la table
   └─> DELETE FROM trades

5. Résultats écrits dans InfluxDB
   └─> 5 Points InfluxDB créés

6. Nouveau batch
   └─> Retour à l'étape 1
```

## 📊 Fenêtres temporelles

### Calcul de la fenêtre

```sql
FLOOR(trade_time / (WINDOW_SECONDS * 1000)) * (WINDOW_SECONDS * 1000)
```

**Explication étape par étape :**

1. `trade_time / (60 * 1000)` :
   - Divise le timestamp (ms) par 60000ms (60 secondes)
   - Exemple : 123456789 / 60000 = 2057.61315

2. `FLOOR(...)` :
   - Arrondit à l'inférieur
   - Exemple : FLOOR(2057.61315) = 2057

3. `... * (60 * 1000)` :
   - Multiplie par 60000 pour obtenir le timestamp de début
   - Exemple : 2057 * 60000 = 123420000

**Résultat :**
- Trade à 123456789 → Fenêtre [123420000, 123479999]
- Trade à 123460000 → Même fenêtre [123420000, 123479999]

### Exemple concret

**WINDOW_SECONDS = 60**

```
Trade 1: time = 1000  → Fenêtre [0, 59999]
Trade 2: time = 50000 → Fenêtre [0, 59999]
Trade 3: time = 61000 → Fenêtre [60000, 119999]
Trade 4: time = 65000 → Fenêtre [60000, 119999]
```

**Agrégation :**
- Trades 1 et 2 → 1 ligne agrégée (fenêtre 0-59s)
- Trades 3 et 4 → 1 ligne agrégée (fenêtre 60-119s)

## 🚀 Performance et optimisations

### Pourquoi c'est rapide ?

1. **En mémoire** :
   - Pas de I/O disque
   - Latence RAM : ~100ns vs disque : ~10ms
   - 100,000x plus rapide

2. **Vecteurisation (SIMD)** :
   - Processe plusieurs valeurs en parallèle
   - Exemple : Calcule MAX sur 8 valeurs simultanément

3. **Colonne-store optimisé** :
   - Stocke les données par colonne (pas par ligne)
   - Efficace pour les agrégations (lit seulement les colonnes nécessaires)

4. **Query optimizer** :
   - Choisit automatiquement le meilleur plan
   - Optimise les JOINs, GROUP BY, etc.

### Benchmarks approximatifs

**Sur un batch de 1000 trades :**
- Insertion : ~1-2ms
- Agrégation : ~2-5ms
- **Total : ~5-10ms**

**Sur un batch de 10000 trades :**
- Insertion : ~10-20ms
- Agrégation : ~10-30ms
- **Total : ~30-50ms**

**Bottleneck :** Généralement pas DuckDB, mais plutôt Kafka (consommation) ou InfluxDB (écriture).

## 🔧 Utilisation dans le code

### Initialisation

```python
conn = duckdb.connect(':memory:')
conn.execute("CREATE TABLE trades (...)")
```

**Une seule fois** : Au démarrage du processor.

### Insertion

```python
conn.executemany("""
    INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", trades_data)
```

**À chaque batch** : Quand le buffer est plein ou l'intervalle est écoulé.

### Agrégation

```python
result = conn.execute(query).fetchall()
```

**Retourne** : Liste de tuples avec les résultats agrégés.

### Vidage

```python
conn.execute("DELETE FROM trades")
```

**Après chaque agrégation** : Pour préparer le prochain batch.

## 📈 Métriques calculées

### Prix

- **MAX(price)** : Prix le plus élevé dans la fenêtre
- **MIN(price)** : Prix le plus bas dans la fenêtre
- **AVG(price)** : Prix moyen arithmétique

### Volumes

- **SUM(volume_buy)** : Somme des volumes des trades "buy"
- **SUM(volume_sell)** : Somme des volumes des trades "sell"
- **SUM(volume)** : Somme de tous les volumes

### Statistiques

- **STDDEV(price)** : Écart-type des prix (pour volatilité)
- **COUNT(*)** : Nombre de trades dans la fenêtre

### Calculs dérivés

- **Volatilité** : `(STDDEV(price) / AVG(price)) * 100` (fait dans Python, pas SQL)

## ⚠️ Limitations et considérations

### Mémoire

**Limite :** Tout est en RAM, attention à la consommation mémoire.

**Dans notre projet :**
- Batch max : 100 trades
- Chaque trade : ~100 bytes
- Total par batch : ~10 KB
- Acceptable même avec 1000x plus

### Concurrence

**Limite :** DuckDB n'est pas thread-safe par défaut.

**Dans notre projet :**
- Un seul thread utilise DuckDB
- Pas de problème de concurrence
- Chaque instance de processor a sa propre connexion

### Persistance

**Limite :** `:memory:` ne persiste pas.

**Dans notre projet :**
- Pas un problème : données déjà dans Kafka (durable)
- Résultats écrits dans InfluxDB (persistant)
- DuckDB est juste un outil de calcul temporaire

## 🎯 Alternatives considérées

### Polars

**Avantage :** DataFrame API moderne, très rapide

**Pourquoi pas choisi :** 
- SQL plus standard et lisible
- Moins de changement de code nécessaire
- DuckDB est déjà excellent pour notre use case

### ClickHouse (local)

**Avantage :** Ultra-rapide pour analytics

**Pourquoi pas choisi :**
- Trop lourd (serveur séparé)
- Configuration complexe
- Overhead réseau inutile

### Apache Arrow

**Avantage :** Format mémoire optimisé

**Pourquoi pas choisi :**
- Pas de SQL natif
- Nécessite plus de code
- DuckDB utilise déjà Arrow en interne

## ✅ Conclusion

DuckDB est le choix optimal pour notre use case :
- **Rapide** : Agrège des milliers de lignes en millisecondes
- **Simple** : SQL standard, pas de serveur
- **Léger** : Pas de dépendances lourdes
- **Adapté** : Conçu pour les analytics en mémoire

Perfect fit pour un stream processor qui agrège des données temporaires avant de les envoyer dans InfluxDB.

