from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime

consumer = KafkaConsumer(
    "my-topic",
    group_id="my-group",
    bootstrap_servers=["kafka:29092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "changeme"
INFLUX_ORG = "cryptoviz"
INFLUX_BUCKET = "home"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

print("Consumer connecté à Kafka et InfluxDB...")

def parse_event_time(event_time_str: str) -> datetime:
    try:
        clean_str = event_time_str.strip().upper()
        event_time = datetime.strptime(clean_str, "%I:%M%p").time()
        today = datetime.today().date()
        return datetime.combine(today, event_time)
    except Exception as e:
        print(f"⚠️ Erreur parsing heure '{event_time_str}': {e}")
        return datetime.now()

for message in consumer:
    event = message.value
    print(f"Reçu depuis Kafka: {event}")

    event_dt = parse_event_time(event["time"])

    point = (
        Point("economic_event")
        .tag("source", "tradingeconomics")
        .field("event_name", event["event_name"])
        .time(event_dt, WritePrecision.NS)
    )

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    print(f"Stocké dans InfluxDB: {event['event_name']} @ {event_dt}")