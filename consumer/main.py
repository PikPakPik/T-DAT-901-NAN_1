import os
import json
import asyncio
import websockets
from kafka import KafkaProducer

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

async def main():
    wsServer = os.environ.get('BINANCE_WEBSOCKET_URL', "wss://stream.binance.com:443/stream?streams=")

    aggTrade = os.environ.get('BINANCE_AGGREGATE_TRADE_STEAMS', "btcusdt@aggTrade,ethusdt@aggTrade").split(',')
    kline = os.environ.get('BINANCE_KLINE_STREAMS', "btcusdt@kline_1m,ethusdt@kline_1m").split(',')

    aggTradeFormattedUrl = wsServer + "/".join(aggTrade)
    klineFormattedUrl = wsServer + "/".join(kline)

    producer = KafkaProducer(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        compression_type='gzip'
    )

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