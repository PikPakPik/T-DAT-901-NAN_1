import json
import time
import os
from faker import Faker
import random
from kafka import KafkaProducer

fake = Faker()

CRYPTO_PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT",
    "XRPUSDT", "DOTUSDT", "UNIUSDT", "LTCUSDT", "LINKUSDT",
    "MATICUSDT", "SOLUSDT", "AVAXUSDT", "ATOMUSDT", "ETCUSDT",
    "XLMUSDT", "ALGOUSDT", "VETUSDT", "FILUSDT", "TRXUSDT",
    "AAVEUSDT", "EOSUSDT", "XMRUSDT", "THETAUSDT", "NEARUSDT",
    "FTMUSDT", "HBARUSDT", "ICPUSDT", "APTUSDT", "ARBUSDT"
]

PRICE_RANGES = {
    "BTCUSDT": (40000, 50000),
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


def create_kafka_producer() -> KafkaProducer:
    """Crée un producteur Kafka avec gestion de la reconnexion."""
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
            producer.partitions_for('trade')
            print("✅ Kafka connection established", flush=True)
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)
            attempt += 1


def generate_aggtrade_data(symbol: str) -> dict:
    """Génère un événement aggTrade au format Kafka (comme main.py)."""
    current_price = price_cache[symbol]
    # Variation légère du prix (marché réaliste)
    price_variation = random.uniform(-0.001, 0.001)
    new_price = current_price * (1 + price_variation)
    price_cache[symbol] = new_price
    
    current_time = int(time.time() * 1000)
    
    # Format identique à main.py
    return {
        "e": "trade",  # Event type
        "E": current_time,  # Event time
        "s": symbol,  # Symbol
        "t": fake.random_int(min=1000000, max=9999999),  # Trade ID
        "p": f"{new_price:.8f}",  # Price
        "q": f"{random.uniform(0.001, 10):.8f}",  # Quantity
        "T": current_time,  # Trade time
        "m": random.choice([True, False])  # Is buyer maker
    }


def main():
    """Génère des données mockées et les envoie à Kafka."""
    # Configuration
    events_per_second = int(os.environ.get('MOCK_EVENTS_PER_SECOND', '100'))
    delay = 1.0 / events_per_second
    
    print("🚀 MOCK DATA GENERATOR - Spamming Kafka", flush=True)
    print(f"💰 {len(CRYPTO_PAIRS)} paires crypto configurées", flush=True)
    print(f"📊 Volume: {events_per_second} événements/seconde", flush=True)
    
    # Création du producteur Kafka
    kafka_producer = create_kafka_producer()
    
    event_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Sélection aléatoire d'une crypto
            symbol = random.choice(CRYPTO_PAIRS)
            
            # Génération de l'événement aggTrade
            trade_data = generate_aggtrade_data(symbol)
            
            # Envoi à Kafka
            try:
                kafka_producer.send("trade", trade_data)
                event_count += 1
                
                # Statistiques toutes les 10 secondes
                if event_count % (events_per_second * 10) == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"📊 {event_count} events envoyés | {rate:.1f} events/s", flush=True)
                    
            except Exception as kafka_err:
                print(f"⚠️ Kafka send error: {kafka_err}", flush=True)
            
            # Attente pour respecter le taux d'événements/seconde
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
