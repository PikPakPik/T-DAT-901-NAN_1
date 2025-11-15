"""
Service de traitement de stream avec DuckDB.

Ce service :
1. Lit les trades depuis Kafka (topic "trade") par micro-batches
2. Agrège les données par fenêtre temporelle avec DuckDB (max, min, prix moyen, volumes, volatilité)
3. Envoie les résultats dans InfluxDB

Format des données Kafka :
{
    "e": "trade",
    "E": 1763117480982,
    "s": "BTCUSDT",
    "t": 5488678497,
    "p": "97094.00000000",
    "q": "0.00006000",
    "T": 1763117480981,
    "m": true
}
"""
import os
import sys
import json
import time
import duckdb
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from datetime import datetime, timedelta
from collections import defaultdict

# Configuration depuis les variables d'environnement
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_TRADE = os.getenv('TOPIC_TRADE', 'trade')
INFLUX_URL = os.getenv('INFLUX_URL', 'http://influxdb:8086')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'cryptoviz')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', 'home')
WINDOW_SECONDS = int(os.getenv('WINDOW_SECONDS', '5'))
KAFKA_BATCH_SIZE = int(os.getenv('KAFKA_BATCH_SIZE', '100'))  # Batch encore plus petit pour traiter plus vite
KAFKA_POLL_TIMEOUT_MS = int(os.getenv('KAFKA_POLL_TIMEOUT_MS', '100'))  # Poll très fréquent
KAFKA_MAX_POLL_RECORDS = int(os.getenv('KAFKA_MAX_POLL_RECORDS', '1000'))  # Beaucoup plus de messages par poll
AGGR_INTERVAL_SECONDS = float(os.getenv('AGGR_INTERVAL_SECONDS', '1.0'))  # Agrégation toutes les secondes
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'duckdb-stream-processor')
# Identifiant unique pour cette instance (utile pour les logs avec plusieurs consumers)
INSTANCE_ID = os.getenv('HOSTNAME', os.getenv('INSTANCE_ID', 'stream-processor-1'))


def create_kafka_consumer():
    """Crée et configure le consumer Kafka."""
    print(f"🔌 [{INSTANCE_ID}] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
    
    consumer = KafkaConsumer(
        TOPIC_TRADE,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,  # Commit toutes les secondes pour réduire le lag
        group_id=KAFKA_GROUP_ID,
        consumer_timeout_ms=KAFKA_POLL_TIMEOUT_MS,
        max_poll_records=KAFKA_MAX_POLL_RECORDS,  # Plus de messages par poll
        max_poll_interval_ms=300000  # Timeout de 5 minutes pour le poll
    )
    
    print(f"✅ [{INSTANCE_ID}] Kafka consumer created (group_id: {KAFKA_GROUP_ID})", flush=True)
    return consumer


def create_influx_client():
    """Crée le client InfluxDB."""
    if not INFLUX_TOKEN:
        raise ValueError("INFLUX_TOKEN environment variable is required")
    
    return InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )


def create_duckdb_connection():
    """Crée une connexion DuckDB en mémoire."""
    conn = duckdb.connect(':memory:')
    
    # Créer la table pour les trades
    conn.execute("""
        CREATE TABLE trades (
            event_time BIGINT,
            symbol VARCHAR,
            trade_id BIGINT,
            price DOUBLE,
            quantity DOUBLE,
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            volume DOUBLE,
            volume_buy DOUBLE,
            volume_sell DOUBLE
        )
    """)
    
    return conn


def process_trades(conn, trades_data):
    """Charge les trades dans DuckDB et retourne les données agrégées."""
    if not trades_data:
        return []
    
    # Insérer les données dans DuckDB
    conn.executemany("""
        INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, trades_data)
    
    # Agrégation par fenêtre temporelle et symbole
    # On utilise des fenêtres de WINDOW_SECONDS basées sur trade_time
    # DuckDB utilise epoch_ms pour les timestamps en millisecondes
    query = f"""
        SELECT 
            CAST(FLOOR(trade_time / ({WINDOW_SECONDS} * 1000)) * ({WINDOW_SECONDS} * 1000) AS BIGINT) AS window_start,
            symbol,
            MAX(price) AS price_max,
            MIN(price) AS price_min,
            AVG(price) AS price_avg,
            SUM(volume_buy) AS volume_buy,
            SUM(volume_sell) AS volume_sell,
            SUM(volume) AS volume_total,
            STDDEV(price) AS price_stddev,
            COUNT(*) AS trade_count
        FROM trades
        GROUP BY 
            CAST(FLOOR(trade_time / ({WINDOW_SECONDS} * 1000)) * ({WINDOW_SECONDS} * 1000) AS BIGINT),
            symbol
        ORDER BY window_start, symbol
    """
    
    result = conn.execute(query).fetchall()
    
    # Vider la table pour le prochain batch
    conn.execute("DELETE FROM trades")
    
    return result


def write_to_influxdb(aggregated_data, write_api):
    """Écrit les données agrégées dans InfluxDB."""
    if not aggregated_data:
        return
    
    points = []
    for row in aggregated_data:
        window_start, symbol, price_max, price_min, price_avg, volume_buy, volume_sell, volume_total, price_stddev, trade_count = row
        
        # Calculer la volatilité (coefficient de variation en pourcentage)
        volatility = 0.0
        if price_stddev is not None and price_avg is not None and price_avg > 0:
            volatility = (price_stddev / price_avg) * 100
        
        # Convertir window_start (timestamp en ms) en datetime
        if isinstance(window_start, (int, float)):
            window_dt = datetime.fromtimestamp(window_start / 1000.0)
        elif isinstance(window_start, str):
            try:
                window_dt = datetime.fromisoformat(window_start.replace(' ', 'T'))
            except:
                window_dt = datetime.fromtimestamp(int(window_start) / 1000.0)
        else:
            window_dt = window_start
        
        point = Point("crypto_aggregates") \
            .tag("symbol", symbol) \
            .field("price_max", float(price_max)) \
            .field("price_min", float(price_min)) \
            .field("price_avg", float(price_avg)) \
            .field("volume_buy", float(volume_buy or 0)) \
            .field("volume_sell", float(volume_sell or 0)) \
            .field("volume_total", float(volume_total or 0)) \
            .field("volatility", float(volatility)) \
            .field("trade_count", int(trade_count)) \
            .time(window_dt, write_precision="s")
        
        points.append(point)
    
    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        print(f"✅ [{INSTANCE_ID}] Written {len(points)} points to InfluxDB", flush=True)


def main():
    """Point d'entrée principal."""
    print(f"🚀 [{INSTANCE_ID}] Starting DuckDB Stream Processor...", flush=True)
    print(f"📊 [{INSTANCE_ID}] Configuration:", flush=True)
    print(f"   - Instance ID: {INSTANCE_ID}", flush=True)
    print(f"   - Kafka: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"   - Topic: {TOPIC_TRADE}", flush=True)
    print(f"   - Consumer Group: {KAFKA_GROUP_ID}", flush=True)
    print(f"   - InfluxDB: {INFLUX_URL}", flush=True)
    print(f"   - Bucket: {INFLUX_BUCKET}", flush=True)
    print(f"   - Window: {WINDOW_SECONDS} seconds", flush=True)
    print(f"   - Batch size: {KAFKA_BATCH_SIZE}", flush=True)
    print(f"   - Poll timeout: {KAFKA_POLL_TIMEOUT_MS}ms", flush=True)
    print(f"   - Max poll records: {KAFKA_MAX_POLL_RECORDS}", flush=True)
    print(f"   - Aggregation interval: {AGGR_INTERVAL_SECONDS}s", flush=True)
    
    try:
        # Initialiser les connexions
        consumer = create_kafka_consumer()
        influx_client = create_influx_client()
        write_api = influx_client.write_api(write_options=ASYNCHRONOUS)  # Asynchrone pour ne pas bloquer
        conn = create_duckdb_connection()
        
        print(f"✅ [{INSTANCE_ID}] All connections established. Starting to process trades...", flush=True)
        
        trades_buffer = []
        last_aggregation_time = time.time()
        
        while True:
            try:
                # Lire les messages depuis Kafka
                message_batch = consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT_MS)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            data = message.value
                            
                            # Valider et extraire les données
                            if not all(k in data for k in ['s', 'p', 'q', 'T', 'm']):
                                continue
                            
                            symbol = data.get('s')
                            price_str = data.get('p')
                            quantity_str = data.get('q')
                            trade_time = data.get('T')
                            is_buyer_maker = data.get('m', False)
                            event_time = data.get('E', trade_time)
                            trade_id = data.get('t', 0)
                            
                            if not symbol or not price_str or not quantity_str:
                                continue
                            
                            try:
                                price = float(price_str)
                                quantity = float(quantity_str)
                                volume = price * quantity
                                volume_buy = volume if not is_buyer_maker else 0.0
                                volume_sell = volume if is_buyer_maker else 0.0
                                
                                trades_buffer.append((
                                    event_time,
                                    symbol,
                                    trade_id,
                                    price,
                                    quantity,
                                    trade_time,
                                    is_buyer_maker,
                                    volume,
                                    volume_buy,
                                    volume_sell
                                ))
                            except (ValueError, TypeError):
                                continue
                                
                        except Exception as e:
                            print(f"⚠️ [{INSTANCE_ID}] Error processing message: {e}", flush=True)
                            continue
                
                # Agrégation périodique ou quand le buffer est plein
                # Agrégation très fréquente pour réduire le lag
                current_time = time.time()
                should_aggregate = (
                    len(trades_buffer) >= KAFKA_BATCH_SIZE or
                    (current_time - last_aggregation_time) >= AGGR_INTERVAL_SECONDS or  # Agrégation très fréquente
                    (len(trades_buffer) > 0 and len(message_batch) == 0)  # Traiter immédiatement si pas de nouveaux messages
                )
                
                if should_aggregate and trades_buffer:
                    try:
                        # Traiter les trades avec DuckDB
                        aggregated_data = process_trades(conn, trades_buffer)
                        
                        # Écrire dans InfluxDB
                        if aggregated_data:
                            write_to_influxdb(aggregated_data, write_api)
                        
                        trades_buffer.clear()
                        last_aggregation_time = current_time
                        
                    except Exception as e:
                        print(f"❌ [{INSTANCE_ID}] Error in aggregation: {e}", flush=True)
                        import traceback
                        traceback.print_exc()
                        trades_buffer.clear()  # Vider le buffer en cas d'erreur
                
                # Pas de pause si on a des messages, pause très courte sinon
                if len(message_batch) == 0 and len(trades_buffer) == 0:
                    time.sleep(0.001)  # Pause minimale seulement si vraiment rien à faire
                
            except KeyboardInterrupt:
                print(f"\n🛑 [{INSTANCE_ID}] Stopping stream processor...", flush=True)
                break
            except Exception as e:
                print(f"❌ [{INSTANCE_ID}] Error in main loop: {e}", flush=True)
                import traceback
                traceback.print_exc()
                time.sleep(1)  # Attendre avant de réessayer
        
        # Nettoyage
        write_api.close()  # Fermer proprement l'API async
        consumer.close()
        influx_client.close()
        conn.close()
        print(f"✅ [{INSTANCE_ID}] Stream processor stopped", flush=True)
        
    except Exception as e:
        print(f"❌ Fatal error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
