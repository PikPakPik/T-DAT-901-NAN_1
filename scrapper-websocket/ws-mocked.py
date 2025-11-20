import json
import time
import os
from faker import Faker
import random
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

fake = Faker()

CRYPTO_PAIRS = [
    "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT",
    "XRPUSDT", "DOTUSDT", "UNIUSDT", "LTCUSDT", "LINKUSDT",
    "MATICUSDT", "SOLUSDT", "AVAXUSDT", "ATOMUSDT", "ETCUSDT",
    "XLMUSDT", "ALGOUSDT", "VETUSDT", "FILUSDT", "TRXUSDT",
    "AAVEUSDT", "EOSUSDT", "XMRUSDT", "THETAUSDT", "NEARUSDT",
    "FTMUSDT", "HBARUSDT", "ICPUSDT", "APTUSDT", "ARBUSDT"
]

PRICE_RANGES = {
    "ETHUSDT": (2000, 3000),
    "BNBUSDT": (200, 400),
    "ADAUSDT": (0.3, 0.6),
    "DOGEUSDT": (0.05, 0.15),
    "XRPUSDT": (0.4, 0.7),
    "DOTUSDT": (5, 10),
    "UNIUSDT": (5, 15),
    "LTCUSDT": (60, 100),
    "LINKUSDT": (6, 15),
    "MATICUSDT": (0.5, 1.2),
    "SOLUSDT": (20, 50),
    "AVAXUSDT": (15, 35),
    "ATOMUSDT": (7, 15),
    "ETCUSDT": (18, 30),
    "XLMUSDT": (0.08, 0.15),
    "ALGOUSDT": (0.15, 0.35),
    "VETUSDT": (0.02, 0.04),
    "FILUSDT": (3, 8),
    "TRXUSDT": (0.06, 0.12),
    "AAVEUSDT": (80, 150),
    "EOSUSDT": (0.6, 1.2),
    "XMRUSDT": (140, 180),
    "THETAUSDT": (0.8, 1.5),
    "NEARUSDT": (1.5, 4),
    "FTMUSDT": (0.3, 0.7),
    "HBARUSDT": (0.05, 0.1),
    "ICPUSDT": (4, 10),
    "APTUSDT": (5, 12),
    "ARBUSDT": (0.8, 1.5)
}

price_cache = {symbol: random.uniform(*PRICE_RANGES.get(symbol, (1, 100))) for symbol in CRYPTO_PAIRS}


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
    """Crée un producteur Kafka avec gestion de la reconnexion."""
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
            # Validate connection by fetching metadata
            producer.partitions_for(topic_name)
            print("✅ Kafka connection established", flush=True)
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)
            attempt += 1


def generate_aggtrade_data(symbol: str) -> dict:
    """
    Génère un événement de trade agrégé mocké au format Kafka.
    
    Simule une variation de prix réaliste (±0.1%) et génère des données
    cohérentes avec le format attendu par le stream-processor.
    
    Args:
        symbol: Symbole de la crypto (ex: "BTCUSDT").
        
    Returns:
        Dictionnaire avec les champs : e, E, s, t, p, q, T, m.
    """
    current_price = price_cache[symbol]
    price_variation = random.uniform(-0.001, 0.001)
    new_price = current_price * (1 + price_variation)
    price_cache[symbol] = new_price
    
    current_time = int(time.time() * 1000)
    
    return {
        "e": "trade",
        "E": current_time,
        "s": symbol,
        "t": fake.random_int(min=1000000, max=9999999),
        "p": f"{new_price:.8f}",
        "q": f"{random.uniform(0.001, 10):.8f}",
        "T": current_time,
        "m": random.choice([True, False])
    }


def main() -> None:
    """
    Point d'entrée : génère des trades mockés et les envoie dans Kafka.
    
    Génère des événements à la fréquence configurée (MOCK_EVENTS_PER_SECOND),
    sélectionne aléatoirement une crypto parmi les 30 configurées et affiche
    des statistiques toutes les 10 secondes.
    """
    events_per_second = int(os.environ.get('MOCK_EVENTS_PER_SECOND', '100'))
    delay = 1.0 / events_per_second
    
    print("🚀 MOCK DATA GENERATOR - Spamming Kafka", flush=True)
    print(f"💰 {len(CRYPTO_PAIRS)} paires crypto configurées", flush=True)
    print(f"📊 Volume: {events_per_second} événements/seconde", flush=True)
    
    kafka_producer = create_kafka_producer()
    
    event_count = 0
    start_time = time.time()
    
    try:
        while True:
            symbol = random.choice(CRYPTO_PAIRS)
            trade_data = generate_aggtrade_data(symbol)
            
            try:
                kafka_producer.send("trade", trade_data)
                event_count += 1
                
                if event_count % (events_per_second * 10) == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"📊 {event_count} events envoyés | {rate:.1f} events/s", flush=True)
                    
            except Exception as kafka_err:
                print(f"⚠️ Kafka send error: {kafka_err}", flush=True)
            
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du générateur par l'utilisateur", flush=True)
    except Exception as e:
        print(f"❌ Erreur fatale: {e}", flush=True)
    finally:
        kafka_producer.flush()
        kafka_producer.close()
        elapsed = time.time() - start_time
        print(f"✅ Arrêt: {event_count} événements envoyés en {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
