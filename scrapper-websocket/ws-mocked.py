import asyncio
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import websockets
from faker import Faker
import random

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


def generate_aggtrade_event(symbol: str) -> Dict[str, Any]:
    current_price = price_cache[symbol]
    # Variation légère du prix (marché réaliste)
    price_variation = random.uniform(-0.001, 0.001)
    new_price = current_price * (1 + price_variation)
    price_cache[symbol] = new_price
    
    current_time = int(time.time() * 1000)
    
    return {
        "stream": f"{symbol.lower()}@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": current_time,
            "s": symbol,
            "a": fake.random_int(min=1000000, max=9999999),
            "p": f"{new_price:.8f}",
            "q": f"{random.uniform(0.001, 10):.8f}",
            "f": fake.random_int(min=100000, max=999999),
            "l": fake.random_int(min=100000, max=999999),
            "T": current_time,
            "m": random.choice([True, False]),
            "M": True
        }
    }


def generate_kline_event(symbol: str, interval: str = "1m") -> Dict[str, Any]:
    current_price = price_cache[symbol]
    current_time = int(time.time() * 1000)
    
    # Génération d'une bougie réaliste
    open_price = current_price * random.uniform(0.998, 1.002)
    close_price = open_price * random.uniform(0.995, 1.005)
    high_price = max(open_price, close_price) * random.uniform(1.0, 1.003)
    low_price = min(open_price, close_price) * random.uniform(0.997, 1.0)
    
    base_volume = random.uniform(100, 10000)
    quote_volume = base_volume * current_price
    
    return {
        "stream": f"{symbol.lower()}@kline_{interval}",
        "data": {
            "e": "kline",
            "E": current_time,
            "s": symbol,
            "k": {
                "t": current_time - 60000,
                "T": current_time + 60000,
                "s": symbol,
                "i": interval,
                "f": fake.random_int(min=100000, max=999999),
                "L": fake.random_int(min=100000, max=999999),
                "o": f"{open_price:.8f}",
                "c": f"{close_price:.8f}",
                "h": f"{high_price:.8f}",
                "l": f"{low_price:.8f}",
                "v": f"{base_volume:.8f}",
                "n": fake.random_int(min=50, max=500),
                "x": random.choice([True, False]),
                "q": f"{quote_volume:.8f}",
                "V": f"{base_volume * 0.6:.8f}",
                "Q": f"{quote_volume * 0.6:.8f}",
                "B": "0"
            }
        }
    }


async def data_generator(events_per_second: int = 100):
    delay = 1.0 / events_per_second
    
    while True:
        # Sélection aléatoire d'une crypto et d'un type d'événement
        symbol = random.choice(CRYPTO_PAIRS)
        event_type = random.choice(["aggTrade", "kline"])
        
        if event_type == "aggTrade":
            event = generate_aggtrade_event(symbol)
        else:
            event = generate_kline_event(symbol)
        
        yield event
        await asyncio.sleep(delay)


async def websocket_handler(websocket):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    print(f"🟢 Client connecté: {client_id}", flush=True)
    
    try:
        # Configuration de la volumétrie via query params (optionnel)
        # Par défaut: 2000 événements/seconde
        events_per_second = 2000
        
        event_count = 0
        start_time = time.time()
        
        async for event in data_generator(events_per_second):
            try:
                await websocket.send(json.dumps(event))
                event_count += 1
                
                # Statistiques toutes les 10 secondes
                if event_count % (events_per_second * 10) == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"📊 {client_id} | {event_count} events | {rate:.1f} events/s", flush=True)
                    
            except websockets.exceptions.ConnectionClosed:
                print(f"🔴 Client déconnecté: {client_id}", flush=True)
                break
                
    except Exception as e:
        print(f"⚠️ Erreur avec client {client_id}: {e}", flush=True)
    finally:
        print(f"🔌 Fermeture connexion: {client_id} ({event_count} events envoyés)", flush=True)


async def main():
    host = "0.0.0.0"
    port = 8765
    
    print("🚀 SERVEUR WEBSOCKET MOCK BINANCE", flush=True)
    print(f"📡 Écoute sur ws://{host}:{port}", flush=True)
    print(f"💰 {len(CRYPTO_PAIRS)} paires crypto configurées", flush=True)
    
    server = await websockets.serve(websocket_handler, host, port)
    print("✅ Serveur prêt à recevoir des connexions!", flush=True)
    await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du serveur par l'utilisateur", flush=True)
    except Exception as e:
        print(f"❌ Erreur fatale: {e}", flush=True)
