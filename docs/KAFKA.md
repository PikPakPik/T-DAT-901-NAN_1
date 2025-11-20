# Kafka dans ce projet

## 🔧 Architecture Kafka

**Kafka** est le broker de messages qui stocke et distribue les messages.

**Notre projet utilise KRaft** (Kafka Raft). Voir [KRAFT.md](./KRAFT.md) pour les détails.

**Configuration dans notre projet :**
```yaml
kafka:
  environment:
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
    KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
```

## 📋 Pourquoi Kafka ?

**Besoin :** Traiter des milliers de trades/seconde avec découplage, durabilité et scaling horizontal.

**Kafka apporte :**
- ✅ **Durabilité** : Messages persistés (pas de perte si processor crash)
- ✅ **Scaling horizontal** : Partitions → plusieurs consumers en parallèle
- ✅ **Découplage** : Scrapper et processor indépendants (buffering automatique)
- ✅ **Performance** : Haut débit, compression gzip
- ✅ **Consumer groups** : Répartition automatique de la charge

## 🔧 Comment fonctionne Kafka ?

### Concepts de base

#### Topic

Un **topic** est un flux de messages organisé (comme une table dans une DB).

```
Topic: "trade"
├── Partition 0: [msg1, msg4, msg7, ...]
├── Partition 1: [msg2, msg5, msg8, ...]
└── Partition 2: [msg3, msg6, msg9, ...]
```

**Dans notre projet :** Un seul topic `trade` qui contient tous les événements de trades.

#### Partition

Une **partition** est une séquence ordonnée de messages dans un topic.

**Avantages :**
- Parallélisation : Chaque partition peut être consommée par un consumer différent
- Ordre garanti : Les messages dans une partition sont dans l'ordre
- Scalabilité : Plus de partitions = plus de parallélisme

**Dans notre projet :** Le topic peut avoir plusieurs partitions pour permettre le scaling horizontal.

#### Producer

Le **producteur** envoie des messages dans un topic.

```
Scrapper → Producer → Topic "trade"
```

**Dans notre projet :** 
- `scrapper-websocket/main.py` : Producer réel qui lit Binance
- `scrapper-websocket/ws-mocked.py` : Producer mocké pour les tests

#### Consumer

Le **consommateur** lit les messages d'un topic.

```
Topic "trade" → Consumer → Stream Processor
```

**Dans notre projet :** `stream-processor/main.py` qui consomme et agrège.

#### Consumer Group

Un **consumer group** est un ensemble de consumers qui travaillent ensemble.

```
Consumer Group: "tdat901nan1"
├── Consumer 1 → Partition 0
├── Consumer 2 → Partition 1
└── Consumer 3 → Partition 2
```

**Comment ça marche :**
- Tous les consumers du même groupe partagent le travail
- Kafka assigne automatiquement les partitions aux consumers
- Si un consumer crash, ses partitions sont réassignées aux autres
- Si un nouveau consumer arrive, Kafka répartit les partitions

**Dans notre projet :** 
- Tous les stream-processors utilisent le même `KAFKA_GROUP_ID`
- Kafka répartit automatiquement les partitions entre les instances

#### Offset

L'**offset** est la position de lecture d'un consumer dans une partition.

```
Partition 0: [msg0, msg1, msg2, msg3, msg4, ...]
                  ↑
            Consumer à offset 2
            (a lu msg0, msg1)
```

**Gestion des offsets :**
- Kafka stocke les offsets dans un topic spécial (`__consumer_offsets`)
- Auto-commit : Kafka commit automatiquement les offsets toutes les N secondes
- Manuel : Le consumer peut commit manuellement après traitement

**Dans notre projet :** Auto-commit toutes les secondes (`auto_commit_interval_ms=1000`).

### Flux de données dans notre projet

```
┌─────────────────┐
│   Binance WS    │
└────────┬────────┘
         │ trades en temps réel
         ▼
┌─────────────────┐
│ Scrapper (Prod) │ ──┐
└─────────────────┘   │
                      │
┌─────────────────┐   │
│ ws-mocked (Prod)│ ──┼─→ Topic "trade" (Kafka)
└─────────────────┘   │
                      │
                      │ Buffer persistant
                      ▼
              ┌───────────────┐
              │   Partitions  │
              │  [0, 1, 2...] │
              └───────┬───────┘
                      │
         ┌────────────┼────────────┐
         │            │            │
    ┌────▼────┐  ┌───▼────┐  ┌───▼────┐
    │Consumer1│  │Consumer2│  │Consumer3│
    │ Group:  │  │ Group:  │  │ Group:  │
    │duckdb-  │  │duckdb-  │  │duckdb-  │
    │stream-  │  │stream-  │  │stream-  │
    │processor│  │processor│  │processor│
    └────┬────┘  └───┬────┘  └───┬────┘
         │            │            │
         └────────────┼────────────┘
                      │
                      ▼
            ┌─────────────────┐
            │ Stream Processor│
            │  (Agrégation)   │
            └─────────────────┘
```

## 🔑 Paramètres Kafka dans le processor

### `group_id` (KAFKA_GROUP_ID)

**Rôle :** Identifie le groupe de consumers qui partagent le travail.

**Valeur dans le projet :** `tdat901nan1`

**Pourquoi c'est important :**
- Tous les stream-processors avec le même `group_id` se répartissent les partitions
- Chaque partition est consommée par un seul consumer du groupe à la fois
- Si vous avez 3 partitions et 3 processors, chaque processor traite 1 partition
- Si vous avez 3 partitions et 1 processor, le processor traite les 3 partitions

**Exemple concret :**
```
Topic "trade" avec 3 partitions :
- Partition 0 → Consumer A (group: tdat901nan1)
- Partition 1 → Consumer B (group: tdat901nan1)  
- Partition 2 → Consumer C (group: tdat901nan1)

Si Consumer B crash :
- Partition 1 → Réassignée à Consumer A ou C
- Les autres continuent normalement
```

### `auto_offset_reset`

**Valeur :** `'latest'`

**Rôle :** Que faire quand un consumer démarre et n'a pas d'offset sauvegardé.

**Options :**
- `'latest'` : Commence à lire les nouveaux messages seulement
- `'earliest'` : Commence depuis le début du topic

**Choix dans notre projet :** `'latest'` car on s'intéresse au temps réel, pas à l'historique.

### `enable_auto_commit`

**Valeur :** `True`

**Rôle :** Kafka commit automatiquement les offsets à intervalles réguliers.

**Avantages :**
- Simplicité : Pas besoin de gérer les commits manuellement
- Performance : Moins de overhead

**Inconvénients :**
- Risque de perte : Si le consumer crash entre le traitement et le commit, les messages peuvent être retraités

**Dans notre projet :** Auto-commit car les duplications sont acceptables (idempotence).

### `auto_commit_interval_ms`

**Valeur :** `1000` (1 seconde)

**Rôle :** Fréquence de commit automatique des offsets.

**Impact :**
- Plus petit = commits plus fréquents = moins de risque de retraitement
- Plus grand = moins d'overhead mais plus de risque

**Choix :** 1 seconde pour équilibrer performance et fiabilité.

### `consumer_timeout_ms`

**Valeur :** `100` ms (KAFKA_POLL_TIMEOUT_MS)

**Rôle :** Timeout pour la méthode `poll()`. Si aucun message n'arrive pendant ce temps, `poll()` retourne un batch vide.

**Pourquoi c'est important :**
- Permet de traiter le buffer même s'il n'y a pas de nouveaux messages
- Évite de bloquer indéfiniment si Kafka est lent
- Permet d'agréger périodiquement même sans nouveaux messages

**Dans notre projet :** 100ms pour réactivité et agrégation périodique.

### `max_poll_records`

**Valeur :** `1000`

**Rôle :** Nombre maximum de messages retournés par un seul `poll()`.

**Impact :**
- Plus grand = moins de round-trips Kafka mais plus de mémoire utilisée
- Plus petit = plus de round-trips mais moins de mémoire

**Choix :** 1000 pour optimiser le throughput tout en gardant une latence acceptable.

### `max_poll_interval_ms`

**Valeur :** `300000` (5 minutes)

**Rôle :** Temps maximum entre deux appels à `poll()` avant que Kafka considère le consumer comme mort.

**Pourquoi c'est important :**
- Si le traitement est très long, Kafka peut penser que le consumer est mort
- Il réassigne alors les partitions à d'autres consumers
- Ça permet de détecter les consumers bloqués

**Dans notre projet :** 5 minutes pour laisser le temps aux agrégations longues.

## 🎯 Pourquoi ces paramètres dans notre contexte ?

### Objectif : Minimiser le consumer lag

Le **consumer lag** est le nombre de messages non encore traités dans Kafka.

**Problème :** Si le lag monte, on accumule du retard.

**Solution :** Configuration optimisée pour le throughput :

1. **Poll fréquent** (100ms) : Lit souvent pour ne pas accumuler
2. **Batch petit** (100 messages) : Traite rapidement sans attendre beaucoup
3. **Agrégation fréquente** (1 seconde) : Vide le buffer régulièrement
4. **Commit fréquent** (1 seconde) : Évite les retraitements en cas de crash
5. **Max poll records élevé** (1000) : Lit beaucoup de messages par round-trip

### Scaling horizontal

Si le lag monte encore, on peut lancer plusieurs processors :

```bash
docker compose up --scale stream-processor=3
```

Kafka répartira automatiquement les partitions entre les 3 instances grâce au `group_id` partagé.

## 📊 Monitoring

### Kafka UI

Accessible sur http://localhost:9090

**Ce qu'on peut voir :**
- Lag des consumer groups
- Messages par partition
- Throughput
- Configuration des topics

### Consumer Lag

**Signification :**
- Lag = 0 : Tout est à jour, pas de retard
- Lag > 1000 : Accumulation de messages, il faut scaler ou optimiser

**Dans notre projet :** On surveille le lag pour décider si on doit lancer plus d'instances.

## 🔒 Garanties Kafka

### Ordering

- **Garantie** : Messages dans l'ordre au sein d'une partition
- **Pas garanti** : Ordre entre partitions différentes

**Dans notre projet :** On n'a pas besoin d'ordre global, juste par symbole (même partition = même symbole possible).

### Durability

- **Garantie** : Messages persistés sur disque (si replication factor > 0)
- **Garantie** : Pas de perte si Kafka crash (avec réplication)

**Dans notre projet :** Replication factor = 1 (acceptable pour dev/test).

### Delivery

- **Au moins une fois** : Avec auto-commit, un message peut être traité plusieurs fois
- **Exactement une fois** : Possible mais plus complexe (nécessite idempotence)

**Dans notre projet :** "Au moins une fois" accepté car les duplications sont minimes et l'agrégation est idempotente sur des fenêtres.

## 📊 Partitions

**Règle :** Nombre de partitions ≥ nombre de consumers pour scaling horizontal.

**Dans notre projet :** Topic `trade` avec 3 partitions (créé automatiquement par les scrapper).

**Vérifier :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic trade
```

**Augmenter (si nécessaire) :**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic trade --partitions 3
```

**⚠️ Important :** On peut augmenter les partitions, mais pas les réduire (nécessite suppression du topic).

