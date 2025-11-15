# Configuration des partitions Kafka

## 📊 Situation actuelle

Par défaut, le topic `trade` est créé automatiquement avec **3 partitions** par les scrapper (`scrapper-websocket/main.py` et `scrapper-websocket/ws-mocked.py`).

Si vous avez un topic existant avec 1 partition, il sera automatiquement augmenté à 3 partitions lors du prochain démarrage d'un scrapper.

Pour permettre le scaling horizontal avec 3 instances de stream-processor, vous devez avoir **au moins 3 partitions**.

## 🔍 Vérifier le nombre de partitions actuelles

### Via Kafka UI

1. Ouvrez http://localhost:9090
2. Allez dans **Topics** → `trade`
3. Regardez la colonne **Partitions**

### Via ligne de commande

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade
```

Cela affichera le nombre de partitions actuelles.

## 🚀 Création automatique (tentative)

**Les scrapper tentent de créer automatiquement le topic avec 3 partitions.**

Quand vous démarrez `scrapper-websocket` ou `scrapper-ws-mock`, ils :
1. Vérifient si le topic existe
2. Le créent avec 3 partitions s'il n'existe pas
3. Tentent d'augmenter le nombre de partitions à 3 s'il a moins de partitions

**⚠️ Note importante :** L'augmentation automatique des partitions peut parfois échouer silencieusement. Si vous avez un topic existant avec 1 partition, **vérifiez manuellement** après le démarrage du scrapper et augmentez-le si nécessaire (voir section "Augmenter manuellement" ci-dessous).

**Configuration via variables d'environnement :**
- `TOPIC_TRADE` : Nom du topic (défaut: `trade`)
- `KAFKA_PARTITIONS` : Nombre de partitions (défaut: `3`)

**Exemple :**
```bash
KAFKA_PARTITIONS=5 docker compose up scrapper-websocket
```

**Vérification après démarrage :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade | grep PartitionCount
```

Si le résultat montre toujours 1 partition, augmentez-la manuellement (voir ci-dessous).

## 🔧 Augmenter manuellement le nombre de partitions (recommandé si auto échoue)

Si l'augmentation automatique échoue ou si vous avez un topic existant, utilisez une de ces méthodes :

### Méthode 1 : Ligne de commande (rapide)

Un script est fourni pour automatiser la création/modification du topic :

```bash
./scripts/create-topic.sh
```

**Ce que fait le script :**
- Vérifie si le topic existe
- Si existe et a < 3 partitions → Augmente à 3
- Si n'existe pas → Crée avec 3 partitions
- Affiche les informations du topic

**Variables d'environnement (optionnel) :**
```bash
KAFKA_PARTITIONS=3 ./scripts/create-topic.sh
```

**Vérifier le nombre actuel :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade | grep PartitionCount
```

**Augmenter à 3 partitions :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic trade --partitions 3
```

**Vérifier le résultat :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade
```

Vous devriez voir :
```
Topic: trade	PartitionCount: 3	ReplicationFactor: 1
	Topic: trade	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: trade	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: trade	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

### Méthode 2 : Via Kafka UI

1. Ouvrez http://localhost:9090
2. Allez dans **Topics** → `trade`
3. Cliquez sur **Settings**
4. Changez le nombre de **Partitions** à 3
5. Cliquez sur **Update**

### Méthode 3 : Script automatique

Si vous avez encore le script `create-topic.sh` :
```bash
./scripts/create-topic.sh
```

## ⚠️ Important : Augmenter vs Réduire

### ✅ Augmenter (possible)

**Kafka permet d'augmenter** le nombre de partitions d'un topic existant sans perte de données.

**Ce qui se passe :**
- Les nouvelles partitions sont créées
- Les messages existants restent dans leur partition d'origine
- Les nouveaux messages sont répartis sur toutes les partitions (y compris les nouvelles)

**Exemple :**
```
Avant : trade (1 partition)
  Partition 0: [msg1, msg2, msg3, ...]

Après augmentation à 3 :
  Partition 0: [msg1, msg2, msg3, ...]  (anciens messages)
  Partition 1: []                       (nouvelle, vide au début)
  Partition 2: []                       (nouvelle, vide au début)
  
Nouveaux messages : répartis sur 0, 1, 2
```

### ❌ Réduire (impossible)

**Kafka ne permet pas de réduire** le nombre de partitions d'un topic existant.

**Si vous voulez réduire :**
- Il faut supprimer le topic (⚠️ **perte de toutes les données**)
- Puis recréer avec moins de partitions

**Commandes (⚠️ perte de données) :**
```bash
# Supprimer le topic
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic trade

# Recréer avec 2 partitions
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic trade --partitions 2 --replication-factor 1
```

## 📈 Impact du nombre de partitions

### Avec 1 partition

```
Topic "trade" (1 partition)
└── Partition 0 → Consumer A (tout le travail)
```

**Limites :**
- Un seul consumer peut traiter à la fois
- Pas de parallélisation possible
- Si vous lancez 3 processors, un seul travaille, les autres sont inactifs

### Avec 3 partitions

```
Topic "trade" (3 partitions)
├── Partition 0 → Consumer A
├── Partition 1 → Consumer B
└── Partition 2 → Consumer C
```

**Avantages :**
- 3 consumers peuvent travailler en parallèle
- Throughput multiplié par 3
- Lag réduit proportionnellement

**Répartition automatique :**
- Si vous avez 3 processors → Chacun traite 1 partition
- Si vous avez 2 processors → Un traite 1 partition, l'autre traite 2 partitions
- Si vous avez 1 processor → Il traite les 3 partitions

## 🎯 Stratégie de partitionnement

### Répartition des messages

**Comment Kafka répartit les messages ?**

Kafka utilise une fonction de hash sur la **clé du message** (key) pour décider dans quelle partition écrire.

**Dans notre cas :**
- Si vous ne spécifiez pas de clé → Round-robin entre les partitions
- Si vous spécifiez une clé (ex: `symbol`) → Tous les messages avec la même clé vont dans la même partition

### Exemple avec clé = symbol

```python
# Producer avec clé
producer.send("trade", key="BTCUSDT", value=trade_data)  # Toujours dans la même partition
producer.send("trade", key="ETHUSDT", value=trade_data)  # Toujours dans une autre partition
```

**Avantages :**
- Ordre garanti par symbole (tous les BTCUSDT dans l'ordre dans la même partition)
- Agrégation plus efficace (tous les trades d'un symbole dans une partition)

**Dans notre projet :**
- Actuellement pas de clé spécifiée → Répartition round-robin
- C'est OK car on agrège par fenêtre temporelle (pas besoin d'ordre par symbole)

## 🚀 Scaling après augmentation des partitions

Une fois que vous avez 3 partitions :

### Lancer 3 instances

```bash
docker compose up --scale stream-processor=3
```

### Vérifier la répartition

**Via Kafka UI :**
1. http://localhost:9090
2. **Consumer Groups** → `duckdb-stream-processor`
3. Vérifiez que les 3 partitions sont assignées

**Via logs :**
Chaque instance affiche quelle partition elle traite dans ses logs.

### Monitoring du lag

**Via Kafka UI :**
- **Consumer Groups** → `duckdb-stream-processor`
- Vérifiez le lag par partition
- Le lag devrait être divisé par 3 avec 3 instances

## 📊 Nombre optimal de partitions

### Règle générale

**Nombre de partitions ≤ Nombre de consumers souhaités**

**Dans notre projet :**
- 3 partitions = supporte jusqu'à 3 consumers
- Si vous voulez scaler à 5 consumers → 5 partitions

### Considérations

**Trop de partitions :**
- Overhead de gestion
- Plus de fichiers ouverts
- Peut ralentir légèrement

**Trop peu de partitions :**
- Limite le parallélisme
- Impossibilité de scaler au-delà

**Recommandation :**
- 3 partitions pour commencer (permet 1-3 consumers)
- Augmentez si besoin (facile à faire)

## 🔄 Modifier après création

### Si le topic existe déjà

Si vous avez déjà des données dans le topic avec 1 partition :

1. **Option 1 : Augmenter (sans perte)**
   ```bash
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic trade --partitions 3
   ```
   - Les données existantes restent dans partition 0
   - Nouvelles partitions créées (vides)
   - Nouveaux messages répartis sur 0, 1, 2

2. **Option 2 : Recréer (avec perte)**
   ```bash
   # ⚠️ Supprime toutes les données
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic trade
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic trade --partitions 3 --replication-factor 1
   ```

**Recommandation :** Utilisez Option 1 (augmenter) sauf si vous êtes en dev/test et que la perte de données est acceptable.

## ✅ Vérification finale

Après modification, vérifiez :

1. **Nombre de partitions :**
   ```bash
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade | grep PartitionCount
   ```

2. **Répartition avec consumers :**
   - Lancez 3 instances : `docker compose up --scale stream-processor=3`
   - Vérifiez dans Kafka UI que les 3 partitions sont assignées
   - Vérifiez que le lag diminue proportionnellement

## 💡 Bonnes pratiques

1. **Créer le topic avec le bon nombre de partitions dès le début** si possible
2. **Augmenter plutôt que réduire** (Kafka ne permet pas la réduction)
3. **Nombre de partitions ≥ nombre de consumers** pour parallélisation maximale
4. **Ne pas sur-dimensionner** (3-5 partitions suffisent pour commencer)
5. **Monitorer le lag** pour décider si plus de partitions sont nécessaires

