# CryptoViz - Docker Compose Setup

### Configuration

1) Copier le fichier d'exemple d'environnement et définir le token InfluxDB v2:

```bash
cp .env.example .env
```

Dans le fichier `.env`, renseigner la valeur:

```bash
INFLUXDBV2_ADMIN_TOKEN=<votre_token_influxdb_v2>
```

### Démarrage

```bash
./dat.sh
```

### Services disponibles

- **Grafana**: http://localhost:3000 (admin/admin)
- **InfluxDB**: http://localhost:8086
- **Spark Master**: http://localhost:8080
- **Kafka**: localhost:9092