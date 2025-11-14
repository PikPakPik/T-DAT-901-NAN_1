# Documentation de l'Agrégation

## Vue d'ensemble

Le stream processor agrège les données de trading en temps réel pour créer des métriques temporelles qui sont stockées dans InfluxDB. Cette documentation explique **quoi** est agrégé, **pourquoi** c'est agrégé, et **comment** c'est calculé.

---

## Pourquoi l'agrégation ?

### Problèmes sans agrégation

1. **Volume de données** : Les marchés crypto génèrent des milliers de trades par seconde. Stocker chaque trade individuellement serait :
   - Coûteux en stockage
   - Lent pour les requêtes
   - Inutile pour l'analyse temporelle

2. **Analyse temporelle** : Pour visualiser l'évolution du marché, on a besoin de métriques agrégées par période (par seconde, par minute, etc.)

3. **Performance** : Les requêtes sur des données agrégées sont beaucoup plus rapides que sur des données brutes

### Solutions avec agrégation

1. **Réduction du volume** : Agréger les trades par fenêtre temporelle réduit drastiquement le nombre de points stockés
2. **Métriques significatives** : Les métriques agrégées (volume, prix moyen, min/max) sont plus utiles pour l'analyse
3. **Requêtes rapides** : InfluxDB peut ensuite agréger ces fenêtres d'1 seconde en périodes plus longues (1 minute, 1 heure, etc.) très rapidement

---

## Architecture d'agrégation

### Fenêtres temporelles

Le système utilise des **fenêtres temporelles de 1 seconde** comme unité de base :

- Chaque fenêtre est isolée : les métriques sont calculées **uniquement** pour les trades dans cette fenêtre
- Les fenêtres sont identifiées par leur timestamp arrondi à la seconde (ex: `2024-01-01T12:00:00`)
- Chaque symbole (ex: `BTCUSDT`) a ses propres fenêtres
- Une fenêtre est finalisée quand la seconde suivante commence

**Exemple** :
```
Trades reçus entre 12:00:00.000 et 12:00:00.999 → Fenêtre "2024-01-01T12:00:00"
Trades reçus entre 12:00:01.000 et 12:00:01.999 → Fenêtre "2024-01-01T12:00:01"
```

### Agrégateurs spécialisés

Le système utilise plusieurs agrégateurs spécialisés qui calculent différentes métriques :

1. **TradeAggregator** : Métriques complètes (volume, prix, ordres)
2. **PriceAggregator** : Métriques de prix uniquement (OHLC)
3. **VolumeAggregator** : Métriques de volume uniquement

Ces agrégateurs peuvent fonctionner en parallèle pour optimiser les performances.

---

## Métriques agrégées

### 1. Métriques de base

#### Volume (`volume`)
- **Quoi** : Somme des quantités de tous les trades dans la fenêtre
- **Pourquoi** : Indique l'activité du marché sur la période
- **Calcul** : `volume = Σ(quantity)` pour tous les trades de la fenêtre
- **Unité** : Base asset (ex: BTC pour BTCUSDT)

#### Nombre de trades (`trade_count`)
- **Quoi** : Nombre total de trades dans la fenêtre
- **Pourquoi** : Indique la fréquence de trading
- **Calcul** : Compteur incrémenté à chaque trade
- **Unité** : Nombre entier

### 2. Métriques de prix

#### Prix minimum (`min_price`)
- **Quoi** : Le prix le plus bas observé dans la fenêtre
- **Pourquoi** : Montre la fourchette de prix sur la période
- **Calcul** : `min_price = min(price)` pour tous les trades de la fenêtre
- **Important** : Calculé **uniquement** pour la fenêtre, pas sur toute la connexion
- **Unité** : Quote asset (ex: USDT pour BTCUSDT)

#### Prix maximum (`max_price`)
- **Quoi** : Le prix le plus haut observé dans la fenêtre
- **Pourquoi** : Montre la fourchette de prix sur la période
- **Calcul** : `max_price = max(price)` pour tous les trades de la fenêtre
- **Important** : Calculé **uniquement** pour la fenêtre, pas sur toute la connexion
- **Unité** : Quote asset (ex: USDT pour BTCUSDT)

#### Prix moyen (`avg_price`)
- **Quoi** : Prix moyen pondéré par le volume
- **Pourquoi** : Indique le prix représentatif de la période (plus précis qu'une simple moyenne)
- **Calcul** : 
  ```
  total_notional = Σ(price × quantity)
  avg_price = total_notional / volume
  ```
- **Unité** : Quote asset (ex: USDT pour BTCUSDT)
- **Note** : C'est une moyenne pondérée, pas une simple moyenne arithmétique. Les trades avec plus de volume ont plus de poids.

### 3. Métriques d'ordre

#### Volume acheteur (`buyer_volume`)
- **Quoi** : Volume total des trades d'achat (buyer n'est pas maker)
- **Pourquoi** : Indique la pression d'achat sur le marché
- **Calcul** : `buyer_volume = Σ(quantity)` pour les trades où `is_buyer_maker = false`
- **Unité** : Base asset (ex: BTC pour BTCUSDT)

#### Volume vendeur (`seller_volume`)
- **Quoi** : Volume total des trades de vente (buyer est maker)
- **Pourquoi** : Indique la pression de vente sur le marché
- **Calcul** : `seller_volume = Σ(quantity)` pour les trades où `is_buyer_maker = true`
- **Unité** : Base asset (ex: BTC pour BTCUSDT)

---

## Agrégateurs détaillés

### TradeAggregator

**Rôle** : Agrège les métriques complètes pour les trades.

**Métriques calculées** :
- Volume total
- Nombre de trades
- Prix min/max/moyen (pondéré)
- Volume acheteur/vendeur

**Utilisation** : Principal agrégateur utilisé pour les données de trading.

**Exemple de calcul** :
```
Fenêtre : 2024-01-01T12:00:00
Trades :
  - Trade 1: price=50000, quantity=0.1, is_buyer_maker=false
  - Trade 2: price=50010, quantity=0.2, is_buyer_maker=true
  - Trade 3: price=50005, quantity=0.15, is_buyer_maker=false

Résultats :
  - volume = 0.1 + 0.2 + 0.15 = 0.45
  - trade_count = 3
  - min_price = 50000
  - max_price = 50010
  - avg_price = (50000×0.1 + 50010×0.2 + 50005×0.15) / 0.45 = 50006.67
  - buyer_volume = 0.1 + 0.15 = 0.25
  - seller_volume = 0.2
```

### PriceAggregator

**Rôle** : Agrège uniquement les métriques de prix (pattern OHLC).

**Métriques calculées** :
- Prix OHLC (Open, High, Low, Close)
- Prix moyen (arithmétique)
- Nombre de trades

**Utilisation** : Pour l'analyse technique et les graphiques en chandeliers.

**Exemple de calcul** :
```
Fenêtre : 2024-01-01T12:00:00
Trades (dans l'ordre) :
  - Trade 1: price=50000
  - Trade 2: price=50010
  - Trade 3: price=50005

Résultats :
  - open_price = 50000 (premier prix)
  - high_price = 50010 (prix le plus haut)
  - low_price = 50000 (prix le plus bas)
  - close_price = 50005 (dernier prix)
  - avg_price = (50000 + 50010 + 50005) / 3 = 50005
```

**Note** : Le `avg_price` ici est une moyenne arithmétique simple, contrairement au `TradeAggregator` qui utilise une moyenne pondérée.

### VolumeAggregator

**Rôle** : Agrège uniquement les métriques de volume.

**Métriques calculées** :
- Volume total
- Volume acheteur
- Volume vendeur
- Nombre de trades

**Utilisation** : Pour l'analyse de volume pur.

**Exemple de calcul** :
```
Fenêtre : 2024-01-01T12:00:00
Trades :
  - Trade 1: quantity=0.1, is_buy=true
  - Trade 2: quantity=0.2, is_buy=false
  - Trade 3: quantity=0.15, is_buy=true

Résultats :
  - volume = 0.1 + 0.2 + 0.15 = 0.45
  - buyer_volume = 0.1 + 0.15 = 0.25
  - seller_volume = 0.2
  - trade_count = 3
```

---

## Flux de traitement

### 1. Réception des données

Les trades sont reçus depuis Kafka en temps réel.

### 2. Routage par topic

Le `TopicRouter` dirige chaque message vers le handler approprié :
- Topic `trade` → `TradeHandler`

### 3. Agrégation par fenêtre

Chaque handler utilise ses agrégateurs pour :
1. Extraire le timestamp du trade
2. Calculer la clé de fenêtre (timestamp arrondi à la seconde)
3. Ajouter le trade à la fenêtre correspondante
4. Mettre à jour les métriques de la fenêtre

### 4. Finalisation de la fenêtre

Quand une nouvelle seconde commence, la fenêtre précédente est finalisée :
1. Les métriques finales sont calculées
2. Un objet `TradeMetrics` est créé
3. La fenêtre est supprimée de la mémoire

### 5. Écriture dans InfluxDB

Les métriques finalisées sont :
1. Ajoutées à un buffer
2. Écrites par batch dans InfluxDB
3. Stockées avec le timestamp de la fenêtre comme index temporel

---

## Stockage dans InfluxDB

### Structure des données

**Measurement** : `trade_1s` (pour les trades)

**Tags** :
- `symbol` : Le symbole (ex: `BTCUSDT`)

**Fields** :
- `volume` : Volume total
- `trade_count` : Nombre de trades
- `min_price` : Prix minimum
- `max_price` : Prix maximum
- `avg_price` : Prix moyen pondéré
- `buyer_volume` : Volume acheteur
- `seller_volume` : Volume vendeur
- `window` : Timestamp de la fenêtre (ISO format)

**Timestamp** : Timestamp de la fenêtre (précision à la seconde)

### Agrégation temporelle dans InfluxDB

Une fois stockées dans InfluxDB, les fenêtres d'1 seconde peuvent être agrégées en périodes plus longues :

- **1 minute** : Agrégation de 60 fenêtres d'1 seconde
- **1 heure** : Agrégation de 3600 fenêtres d'1 seconde
- **1 jour** : Agrégation de 86400 fenêtres d'1 seconde

InfluxDB peut faire cette agrégation à la volée lors des requêtes, sans avoir à recalculer depuis les données brutes.

---

## Optimisations

### Isolation des fenêtres

Chaque fenêtre est **isolée** : les métriques min/max sont calculées uniquement pour la fenêtre, pas sur toute la durée de la connexion. Cela garantit que :
- Les métriques sont cohérentes avec la période
- Pas de pollution entre fenêtres
- Les données sont correctes même en cas de reconnexion

### Calcul du prix moyen

Le prix moyen utilise une **moyenne pondérée** par le volume, pas une simple moyenne arithmétique :
- Un trade de 1 BTC à 50000$ compte plus qu'un trade de 0.001 BTC à 51000$
- Cela reflète mieux le prix réel auquel les volumes ont été échangés

### Gestion mémoire

- Les fenêtres sont supprimées après finalisation
- Limite du nombre de fenêtres en mémoire (configurable)
- Nettoyage automatique des anciennes fenêtres

---

## Exemples d'utilisation

### Exemple 1 : Volume sur 1 minute

Pour obtenir le volume total sur 1 minute, InfluxDB agrège 60 fenêtres d'1 seconde :

```flux
from(bucket: "crypto")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "trade_1s")
  |> filter(fn: (r) => r.symbol == "BTCUSDT")
  |> filter(fn: (r) => r._field == "volume")
  |> aggregateWindow(every: 1m, fn: sum)
```

### Exemple 2 : Prix moyen sur 1 heure

Pour obtenir le prix moyen sur 1 heure (moyenne pondérée des prix moyens de chaque seconde) :

```flux
from(bucket: "crypto")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "trade_1s")
  |> filter(fn: (r) => r.symbol == "BTCUSDT")
  |> filter(fn: (r) => r._field == "avg_price")
  |> aggregateWindow(every: 1h, fn: mean)
```

### Exemple 3 : Ratio acheteur/vendeur

Pour analyser la pression d'achat vs vente :

```flux
from(bucket: "crypto")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "trade_1s")
  |> filter(fn: (r) => r.symbol == "BTCUSDT")
  |> filter(fn: (r) => r._field == "buyer_volume" or r._field == "seller_volume")
  |> aggregateWindow(every: 1m, fn: sum)
```

---

## Métriques non agrégées

Certaines métriques ont été **supprimées** car elles n'étaient pas utilisées ou ajoutaient de la complexité sans valeur :

- ❌ **median_price** : Prix médian (supprimé)
- ❌ **price_range** : Fourchette de prix (supprimé, peut être calculé comme `max_price - min_price`)
- ❌ **notional** : Valeur notionnelle totale (supprimé, peut être calculé comme `avg_price × volume`)
- ❌ **buyer_maker_ratio** : Ratio acheteur maker (supprimé)
- ❌ **seller_maker_ratio** : Ratio vendeur maker (supprimé)

Ces métriques peuvent être calculées à la volée dans InfluxDB si nécessaire, sans avoir à les stocker.

---

## Conclusion

L'agrégation permet de :
1. **Réduire** le volume de données stockées
2. **Améliorer** les performances des requêtes
3. **Fournir** des métriques significatives pour l'analyse
4. **Permettre** l'agrégation temporelle flexible dans InfluxDB

Les fenêtres d'1 seconde sont l'unité de base, et InfluxDB peut ensuite agréger ces fenêtres en périodes plus longues selon les besoins de visualisation et d'analyse.

