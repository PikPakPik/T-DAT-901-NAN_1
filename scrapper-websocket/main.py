import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
import time


async def listen_stream(url: str, stream_type: str, handler) -> None:
    """
    Écoute un stream WebSocket Binance et appelle le handler pour chaque message.
    
    Args:
        url: URL du stream WebSocket Binance.
        stream_type: Type de stream (pour les logs).
        handler: Fonction async appelée avec chaque message reçu.
    """
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


def agg_trade_handler(producer: KafkaProducer):
    """
    Crée un handler pour les événements de trade agrégés Binance.
    
    Le handler extrait les champs nécessaires et envoie le trade dans Kafka.
    Format attendu : {"stream": "...", "data": {"e": "trade", "s": "...", ...}}
    
    Args:
        producer: Producteur Kafka configuré.
        
    Returns:
        Fonction async handler pour les messages WebSocket.
        
    Reference:
        https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams
    """
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


def create_kafka_producer() -> KafkaProducer:
    """
    Crée un producteur Kafka avec gestion de la reconnexion automatique.
    
    Tente de se connecter indéfiniment avec retry toutes les 3 secondes
    jusqu'à ce que Kafka soit disponible.
    
    Returns:
        KafkaProducer: Producteur configuré avec compression gzip.
    """
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
            producer.partitions_for('trade')
            print("✅ Kafka connection established", flush=True)
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)
            attempt += 1


async def main() -> None:
    """
    Point d'entrée principal : connecte aux streams Binance et envoie dans Kafka.
    
    Configure les streams depuis les variables d'environnement et lance
    l'écoute asynchrone des trades agrégés.
    """
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