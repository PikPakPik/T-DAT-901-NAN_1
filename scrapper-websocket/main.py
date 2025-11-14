import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
import time

"""
Établissement d'une connexion WebSocket à Binance et envoi des données reçues à Kafka.
"""
async def listen_stream(url, stream_type, handler):
    print(f"🟡 Try to connect on {stream_type} stream: {url}", flush=True)
    async with websockets.connect(url) as websocket:
        print(f"✅ Connected to {stream_type} stream: {url}", flush=True)
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                await handler(data)
            except Exception as e:
                print(f"⚠️ Error in {stream_type} stream: {e}", flush=True)
                break

"""
Données retournées par le stream trade (Binance aggregated trade):
Documentation: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams
Exemple de payload reçu (format JSON):
{
  "e": "trade",    # Event type
  "E": 1672515782136,  # Event time
  "s": "BNBBTC",      # Symbol
  "a": 12345,          # Aggregate trade ID
  "p": "0.001",      # Price
  "q": "100",        # Quantity
  "f": 100,           # First trade ID
  "l": 105,           # Last trade ID
  "T": 1672515782136, # Trade time
  "m": true,          # Is the buyer the market maker?
  "M": true           # Ignore
}
Clés importantes : e, E, s, a, p, q, f, l, T, m, M
"""
def agg_trade_handler(producer):
    print("🟢 Starting trade handler", flush=True)
    async def handler(data):
        try:
            keys = ["e", "E", "s", "t", "p", "q", "T", "m"]
            filtered = {k: data['data'][k] for k in keys if k in data['data']}
            producer.send("trade", filtered)
            producer.flush()
            print(f"✅ Sent to Kafka: {filtered}", flush=True)
        except Exception as kafka_err:
            print(f"⚠️ Kafka send error: {kafka_err}", flush=True)
    return handler

"""
Création d'un producteur Kafka avec gestion de la reconnexion.
"""
def create_kafka_producer() -> KafkaProducer:
    # Treat empty env var as unset and fallback to docker-internal default
    bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS') or "kafka:29092"
    attempt = 1
    while True:
        try:
            print(f"🔌 Connecting to Kafka at {bootstrap} (attempt {attempt})", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip'
            )
            # Validate connection by fetching metadata
            producer.partitions_for('healthcheck-topic')
            print("✅ Kafka connection established", flush=True)
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)
            attempt += 1

async def main():
    wsServer = os.environ.get('BINANCE_WEBSOCKET_URL', "wss://stream.binance.com:443/stream?streams=")

    trade = os.environ.get('BINANCE_AGGREGATE_TRADE_STEAMS', "btcusdt@trade,ethusdt@trade").split(',')

    aggTradeFormattedUrl = wsServer + "/".join(trade)

    producer = create_kafka_producer()

    tasks = [
        listen_stream(aggTradeFormattedUrl, "trade", agg_trade_handler(producer))
    ]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print("🚀 Consumer service starting...", flush=True)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Service stopped by user", flush=True)
    except Exception as e:
        print(f"❌ Fatal error: {e}", flush=True)