# Pourquoi Kafka ? Pourquoi KRaft ?

## 🔵 Pourquoi Kafka (et pas RabbitMQ/Redis/autres) ?

### 1. **Volume élevé et latence faible**
- Traitement de **milliers de trades/seconde** en temps réel
- Kafka peut gérer **des millions de messages/seconde** avec un débit élevé
- RabbitMQ/Redis sont limités pour ce type de charge

### 2. **Durabilité et découplage**
- Messages **persistés sur disque** : pas de perte si le processor crash
- **Buffer automatique** entre scrapper et processor (pas besoin de gérer manuellement)
- Redis en mémoire = perte de données si crash

### 3. **Scaling horizontal simple**
- **Partitions** = parallélisation naturelle
- Ajouter des consumers = répartition automatique de la charge
- RabbitMQ nécessite des configurations complexes (sharding, clusters)

### 4. **Streaming natif**
- Kafka est conçu pour le **streaming de données** (pas juste des queues)
- Replay possible (lecture depuis n'importe quel offset)
- Parfait pour l'agrégation temporelle (fenêtres de 60s)

---

## 🔴 Pourquoi KRaft (et pas ZooKeeper) ?

### 1. **Architecture simplifiée**
- **Pas besoin de ZooKeeper** = un service en moins à maintenir
- Kafka = broker + controller dans un seul processus
- Moins de points de défaillance

### 2. **Démarrage plus rapide**
- ZooKeeper ajoute du temps de démarrage (initialisation séparée)
- KRaft démarre directement, pas d'attente de ZooKeeper
- Métadonnées stockées directement dans Kafka

### 3. **Meilleure scalabilité des métadonnées**
- ZooKeeper limite le nombre de partitions (100k max par cluster)
- KRaft peut gérer **beaucoup plus de partitions** (millions)
- Gestion des métadonnées plus efficace

### 4. **Futur de Kafka**
- ZooKeeper est **déprécié** dans Kafka (removal prévu)
- KRaft est le mode natif depuis Kafka 3.3+
- Version moderne et supportée long terme

---

## 📊 Comparaison rapide

| Besoin | Kafka | RabbitMQ | Redis |
|--------|-------|----------|-------|
| Volume élevé (M msg/s) | ✅ | ❌ | ❌ |
| Durabilité (persisté) | ✅ | ✅ | ❌ |
| Streaming natif | ✅ | ❌ | ❌ |
| Scaling horizontal | ✅ | ⚠️ | ⚠️ |

| Aspect | KRaft | ZooKeeper |
|--------|-------|-----------|
| Services à maintenir | 1 | 2 |
| Démarrage | Rapide | Plus lent |
| Scalabilité métadonnées | ✅ Excellent | ⚠️ Limité |
| Support long terme | ✅ Futur | ❌ Déprécié |

Variable	Valeur	But
Election timeout	1s	Élection leader plus rapide
Fetch timeout	2s	Évite les blocages
Retry backoff	20ms	Récupération rapide
Segment bytes	1 Go	Moins de fichiers
Snapshot threshold	20 Mo	Snapshots fréquents
Session timeout	9s	Détection broker mort
Heartbeat interval	3s	Signal "je suis vivant"