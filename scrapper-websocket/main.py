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
Données retournées par le stream aggTrade (Binance aggregated trade):
Documentation: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams
Exemple de payload reçu (format JSON):
{
  "e": "aggTrade",    # Event type
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
    print("🟢 Starting aggTrade handler", flush=True)
    async def handler(data):
        try:
            keys = ["e", "E", "s", "t", "p", "q", "T", "m"]
            filtered = {k: data['data'][k] for k in keys if k in data['data']}
            producer.send("aggTrade", filtered)
            producer.flush()
            print(f"✅ Sent to Kafka: {filtered}", flush=True)
        except Exception as kafka_err:
            print(f"⚠️ Kafka send error: {kafka_err}", flush=True)
    return handler

"""
Données retournées par le stream kline (Binance candlestick/kline):
Documentation: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#klinecandlestick-streams-for-utc
Exemple de payload reçu (format JSON):
{
  "e": "kline",         // Event type
  "E": 1672515782136,   // Event time
  "s": "BNBBTC",        // Symbol
  "k": {
    "t": 1672515780000, // Kline start time
    "T": 1672515839999, // Kline close time
    "s": "BNBBTC",      // Symbol
    "i": "1m",          // Interval
    "f": 100,           // First trade ID
    "L": 200,           // Last trade ID
    "o": "0.0010",      // Open price
    "c": "0.0020",      // Close price
    "h": "0.0025",      // High price
    "l": "0.0015",      // Low price
    "v": "1000",        // Base asset volume
    "n": 100,           // Number of trades
    "x": false,         // Is this kline closed?
    "q": "1.0000",      // Quote asset volume
    "V": "500",         // Taker buy base asset volume
    "Q": "0.500",       // Taker buy quote asset volume
    "B": "123456"       // Ignore
  }
}
Clés importantes : e, E, s, t, T, s, i, o, c, h, l, v, n, x, q, V, Q
"""
def kline_handler(producer):
    print("🟢 Starting kline handler", flush=True)
    async def handler(data):
        try:
            root_keys = ["e", "E", "s"]
            k_keys = ["t", "T", "s", "i", "o", "c", "h", "l", "v", "n", "x", "q", "V", "Q"]
            filtered = {k: data['data'][k] for k in root_keys if k in data['data']}
            filtered = {k: data['data']["k"][k] for k in k_keys if k in data['data']["k"]}
            producer.send("kline", filtered)
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

    aggTrade = os.environ.get('BINANCE_AGGREGATE_TRADE_STEAMS', "btcusdt@aggTrade,ethusdt@aggTrade").split(',')
    kline = os.environ.get('BINANCE_KLINE_STREAMS', "btcusdt@kline_1m,ethusdt@kline_1m").split(',')

    aggTradeFormattedUrl = wsServer + "/".join(aggTrade)
    klineFormattedUrl = wsServer + "/".join(kline)

    producer = create_kafka_producer()

    tasks = [
        listen_stream(aggTradeFormattedUrl, "aggTrade", agg_trade_handler(producer)),
        listen_stream(klineFormattedUrl, "kline", kline_handler(producer))
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