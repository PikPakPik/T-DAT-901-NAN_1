# KRaft - Configuration actuelle

## 🎯 Qu'est-ce que KRaft ?

**KRaft** (Kafka Raft) est le mode de fonctionnement natif de Kafka depuis Kafka 2.8+.

**Avantages :**
- ✅ **Moins de dépendances** : Architecture simplifiée
- ✅ **Meilleure scalabilité** : Gestion des métadonnées plus efficace
- ✅ **Démarrage plus rapide** : Pas de dépendances externes
- ✅ **Moins de points de défaillance** : Architecture unifiée

## 📊 Configuration actuelle

Notre projet utilise KRaft. Kafka fonctionne en mode **broker + controller** sur un seul nœud.

**Configuration dans compose.yaml :**
```yaml
kafka:
  image: confluentinc/cp-kafka:7.4.0
  hostname: kafka
  ports:
    - 9092:9092
  environment:
    KAFKA_BROKER_ID: 1
    # KRaft configuration
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
    KAFKA_CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_LOG_DIRS: /var/lib/kafka/data
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    KAFKA_HEAP_OPTS: "-Xms512m -Xmx1g"
  volumes:
    - kafka_data:/var/lib/kafka/data
```

**Paramètres clés :**
- `KAFKA_PROCESS_ROLES: broker,controller` : Kafka agit comme broker ET controller
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093` : Un seul controller (nœud unique)
- `CONTROLLER://kafka:29093` : Port interne pour la communication controller

## ✅ Vérification

```bash
# Vérifier que Kafka fonctionne
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Vérifier les logs
docker logs kafka | tail -20

# Vérifier les métadonnées KRaft
docker logs kafka | grep -i kraft
```

## ⚠️ Notes importantes

1. **Single node** : Cette config est pour un seul broker. Pour production avec plusieurs brokers, il faut configurer plusieurs controllers.

2. **Clients** : Les clients (scrapper, stream-processor) n'ont **aucun changement** à faire. Ils continuent d'utiliser `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.

3. **Kafka UI** : Fonctionne parfaitement avec KRaft (pas de changement nécessaire).

4. **Version Kafka** : KRaft est stable depuis Kafka 3.3+. Notre version 7.4.0 le supporte pleinement.
