from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import time
import sys

print("🔌 Démarrage du consumer Kafka → InfluxDB", flush=True)

# Attendre que Kafka soit prêt
max_retries = 60
consumer = None

for i in range(max_retries):
    try:
        print(f"⏳ Tentative de connexion à Kafka ({i+1}/{max_retries})...", flush=True)
        consumer = KafkaConsumer(
            "my-topic",
            group_id="my-group",
            bootstrap_servers=["kafka:29092"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
            api_version_auto_timeout_ms=10000,
            request_timeout_ms=30000
        )
        print("✅ Connecté à Kafka!", flush=True)
        break
    except Exception as e:
        print(f"❌ Erreur Kafka (tentative {i+1}): {e}", flush=True)
        if i == max_retries - 1:
            print("❌ Impossible de se connecter à Kafka après toutes les tentatives", flush=True)
            sys.exit(1)
        time.sleep(5)

# Configuration InfluxDB
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "changeme"
INFLUX_ORG = "cryptoviz"
INFLUX_BUCKET = "home"

print(f"🔌 Connexion à InfluxDB: {INFLUX_URL}", flush=True)

try:
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    health = client.health()
    print(f"✅ InfluxDB connecté: {health.status}", flush=True)
except Exception as e:
    print(f"❌ Erreur connexion InfluxDB: {e}", flush=True)
    sys.exit(1)

print("✅ Kafka + InfluxDB prêts - En attente de messages...", flush=True)

message_count = 0

try:
    for message in consumer:
        message_count += 1
        event = message.value
        print(f"📦 Message #{message_count} reçu: {event}", flush=True)

        if not isinstance(event, dict):
            print(f"⚠️  Message invalide (pas un dict): {event}", flush=True)
            continue

        # Utilisation de .get() pour éviter les KeyError
        event_date = event.get("date", "N/A")
        event_name = event.get("event_name", "unknown")
        event_time = event.get("time", "N/A")
        event_ref = event.get("reference", "")

        print(f"📩 [{event_date}] {event_name} | {event_time} | Ref: {event_ref}", flush=True)

        # Stockage direct dans InfluxDB sans parsing
        point = (
            Point("economic_event")
            .tag("source", "tradingeconomics")
            .tag("event_name", event_name)
            .tag("date", event_date)          # 🔹 Ajout de la date comme tag
            .field("time", event_time)        # 🔹 Stocker l'heure telle quelle comme champ
            .field("reference", event_ref)    # 🔹 Stocker la référence
            .field("value", 1)                # champ indicatif
        )

        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"✅ Stocké dans InfluxDB → {event_name} ({event_date} {event_time})", flush=True)
        except Exception as e:
            print(f"❌ ERREUR écriture InfluxDB: {e}", flush=True)

except KeyboardInterrupt:
    print("\n🛑 Arrêt du consumer", flush=True)
except Exception as e:
    print(f"❌ ERREUR FATALE: {e}", flush=True)
    sys.exit(1)
finally:
    consumer.close()
    client.close()
    print("👋 Consumer fermé proprement", flush=True)
