from kafka import KafkaConsumer
import os
import json

try:
    # consumerAggTrade = KafkaConsumer(
    #     'aggTrade',
    #     bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092"),
    #     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    # )
    # print("🟢 Abonné au topic Kafka: aggTrade")
    # for msg in consumerAggTrade:
    #     print(f"📦 Message reçu: {msg.value}")

    consumerKline = KafkaConsumer(
        'kline',
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092"),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("🟢 Abonné au topic Kafka: kline")
    for msg in consumerKline:
        print(f"📦 Message reçu: {msg.value}")
except Exception as e:
    print(f"❌ Erreur Kafka: {e}")