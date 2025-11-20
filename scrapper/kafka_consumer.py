import json
import os
import sys
import time
from datetime import datetime, time as dt_time, timezone

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

print("🔌 Démarrage du consumer Kafka → InfluxDB", flush=True)

# Configuration via variables d'environnement
RAW_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_BOOTSTRAP_SERVERS = [server.strip() for server in RAW_BOOTSTRAP.split(",") if server.strip()]
if not KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = ["kafka:29092"]

SCRAPPER_TOPIC = os.getenv("SCRAPPER_TOPIC", "economic-events")
SCRAPPER_CONSUMER_GROUP = os.getenv("SCRAPPER_CONSUMER_GROUP", "scrapper-consumer")

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "cryptoviz")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "home")

if not INFLUX_TOKEN:
    print("❌ INFLUX_TOKEN manquant - définissez la variable d'environnement correspondante", flush=True)
    sys.exit(1)

# Attendre que Kafka soit prêt
max_retries = 60
consumer = None

for i in range(max_retries):
    try:
        print(f"⏳ Tentative de connexion à Kafka ({i+1}/{max_retries})...", flush=True)
        consumer = KafkaConsumer(
            SCRAPPER_TOPIC,
            group_id=SCRAPPER_CONSUMER_GROUP,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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

DATE_FORMATS = [
    "%A %B %d %Y",
    "%B %d %Y",
]


def parse_event_datetime(date_str: str | None, time_str: str | None) -> datetime:
    fallback = datetime.now(tz=timezone.utc)

    if not date_str:
        return fallback

    parsed_date = None
    for fmt in DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            break
        except Exception:
            continue

    if parsed_date is None:
        return fallback

    parsed_time = dt_time(hour=12, minute=0)
    if time_str and "Aucune" not in time_str:
        for fmt in ("%I:%M %p", "%H:%M"):
            try:
                parsed_time = datetime.strptime(time_str, fmt).time()
                break
            except Exception:
                continue

    combined = datetime.combine(parsed_date.date(), parsed_time)
    return combined.replace(tzinfo=timezone.utc)


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

        event_dt = parse_event_datetime(event_date, event_time)

        point = (
            Point("economic_event")
            .tag("source", "tradingeconomics")
            .tag("event_name", event_name)
            .tag("date", event_date)
            .field("time", event_time)
            .field("reference", event_ref)
            .field("value", 1)
            .time(event_dt)
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
