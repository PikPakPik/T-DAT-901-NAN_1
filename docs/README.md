# Documentation du projet

## 📚 Index rapide

### Architecture & Infrastructure
- **[KAFKA.md](./KAFKA.md)** - Kafka : pourquoi, comment, configuration, partitions
- **[KRAFT.md](./KRAFT.md)** - KRaft : configuration actuelle

### Composants
- **[STREAM_PROCESSOR.md](./STREAM_PROCESSOR.md)** - Stream processor : fonctionnement détaillé
- **[DUCKDB.md](./DUCKDB.md)** - DuckDB : pourquoi et comment

### Référence
- **[CODE.md](./CODE.md)** - Documentation du code (structure, fonctions, formats)

## 🎯 Par où commencer ?

**Pour comprendre l'architecture globale :**
1. Lire [KAFKA.md](./KAFKA.md) (sections "Pourquoi Kafka" et "Concepts de base")
2. Lire [KRAFT.md](./KRAFT.md) pour comprendre la configuration KRaft
3. Lire [STREAM_PROCESSOR.md](./STREAM_PROCESSOR.md) pour le traitement des données

**Pour configurer/scaler :**
- [KAFKA.md](./KAFKA.md) - Section "Partitions" pour augmenter les partitions
- [KAFKA.md](./KAFKA.md) - Section "Paramètres Kafka" pour la configuration

**Pour comprendre le code :**
- [CODE.md](./CODE.md) - Référence complète du code

## 📊 Vue d'ensemble rapide

```
Binance WebSocket → Scrapper → Kafka (topic "trade")
                                    ↓
                            Stream Processor
                                    ↓
                            DuckDB (agrégation)
                                    ↓
                            InfluxDB → Grafana
```

**Composants clés :**
- **Kafka (KRaft)** : Buffer persistant, découplage, scaling horizontal
- **Stream Processor** : Consomme Kafka, agrège avec DuckDB, écrit dans InfluxDB
- **DuckDB** : Agrégation SQL rapide en mémoire
- **InfluxDB** : Stockage time-series pour Grafana

