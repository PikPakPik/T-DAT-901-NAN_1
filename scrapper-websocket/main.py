import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
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


def ensure_topic_exists(topic_name: str, partitions: int = 3, replication_factor: int = 1) -> None:
    """
    Vérifie si le topic existe et le crée avec le bon nombre de partitions si nécessaire.
    
    Si le topic existe déjà avec moins de partitions, augmente le nombre de partitions.
    Si le topic n'existe pas, le crée avec le nombre de partitions spécifié.
    
    Args:
        topic_name: Nom du topic Kafka.
        partitions: Nombre de partitions désiré (défaut: 3).
        replication_factor: Facteur de réplication (défaut: 1).
    """
    bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS') or "kafka:29092"
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap)
        
        # Vérifier si le topic existe déjà
        try:
            metadata = admin_client.describe_topics([topic_name])
            
            if topic_name in metadata:
                topic_metadata = metadata[topic_name]
                current_partitions = len(topic_metadata.partitions) if hasattr(topic_metadata, 'partitions') else 0
                
                print(f"📊 Topic '{topic_name}' existe avec {current_partitions} partitions", flush=True)
                
                if current_partitions < partitions:
                    print(f"🔄 Augmentation du nombre de partitions de {current_partitions} à {partitions}...", flush=True)
                    try:
                        # Augmenter le nombre de partitions - API attend un dict {topic_name: total_partitions}
                        future = admin_client.create_partitions({
                            topic_name: partitions
                        })
                        # Attendre la complétion
                        future[topic_name].result(timeout=10)
                        
                        # Vérifier que l'augmentation a bien fonctionné
                        metadata_after = admin_client.describe_topics([topic_name])
                        if topic_name in metadata_after:
                            topic_metadata_after = metadata_after[topic_name]
                            new_partitions = len(topic_metadata_after.partitions) if hasattr(topic_metadata_after, 'partitions') else 0
                            print(f"✅ Topic '{topic_name}' maintenant avec {new_partitions} partitions (vérifié)", flush=True)
                        else:
                            print(f"⚠️  Impossible de vérifier le nombre de partitions après augmentation", flush=True)
                    except Exception as e:
                        print(f"❌ Erreur lors de l'augmentation des partitions: {e}", flush=True)
                        import traceback
                        traceback.print_exc()
                        raise
                elif current_partitions == partitions:
                    print(f"✅ Topic '{topic_name}' a déjà {partitions} partitions", flush=True)
                else:
                    print(f"ℹ️  Topic '{topic_name}' a déjà {current_partitions} partitions (≥ {partitions})", flush=True)
            else:
                # Topic n'existe pas dans les métadonnées, le créer
                print(f"🆕 Création du topic '{topic_name}' avec {partitions} partitions...", flush=True)
                topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
                future = admin_client.create_topics([topic])
                future[topic_name].result()
                print(f"✅ Topic '{topic_name}' créé avec {partitions} partitions", flush=True)
                
        except UnknownTopicOrPartitionError:
            # Le topic n'existe pas, le créer
            print(f"🆕 Création du topic '{topic_name}' avec {partitions} partitions...", flush=True)
            topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
            try:
                future = admin_client.create_topics([topic])
                future[topic_name].result()
                print(f"✅ Topic '{topic_name}' créé avec {partitions} partitions", flush=True)
            except TopicAlreadyExistsError:
                print(f"ℹ️  Topic '{topic_name}' a été créé entre temps", flush=True)
            
    except TopicAlreadyExistsError:
        print(f"ℹ️  Topic '{topic_name}' existe déjà", flush=True)
    except Exception as e:
        print(f"⚠️  Erreur lors de la gestion du topic '{topic_name}': {e}", flush=True)
        # Continue quand même, le topic pourrait déjà exister


def create_kafka_producer() -> KafkaProducer:
    """
    Crée un producteur Kafka avec gestion de la reconnexion automatique.
    
    Crée également le topic 'trade' avec 3 partitions s'il n'existe pas.
    Tente de se connecter indéfiniment avec retry toutes les 3 secondes
    jusqu'à ce que Kafka soit disponible.
    
    Returns:
        KafkaProducer: Producteur configuré avec compression gzip.
    """
    bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS') or "kafka:29092"
    topic_name = os.environ.get('TOPIC_TRADE', 'trade')
    partitions = int(os.environ.get('KAFKA_PARTITIONS', '3'))
    
    attempt = 1
    while True:
        try:
            print(f"🔌 Connecting to Kafka at {bootstrap} (attempt {attempt})", flush=True)
            
            # S'assurer que le topic existe avec le bon nombre de partitions
            ensure_topic_exists(topic_name, partitions=partitions)
            
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip'
            )
            producer.partitions_for(topic_name)
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