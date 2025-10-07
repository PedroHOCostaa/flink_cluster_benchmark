#!/bin/bash
set -e

echo "🚀 Iniciando RiotBench Kafka Data Generator..."
echo "Broker Kafka: ${KAFKA_BROKER}"
echo "Tópico: ${TOPIC}"

DATA_PATH="$RIOTBENCH_HOME/modules/tasks/src/main/resources"

# Exemplo: envia dados do dataset TAXI via Kafka
echo "📊 Enviando dataset: TAXI_sample_data_senml.csv"

python3 - <<EOF
from kafka import KafkaProducer
import time, json, csv, os

broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = os.getenv("TOPIC", "riotbench-stream")

producer = KafkaProducer(
    bootstrap_servers=[broker],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

dataset_path = os.path.join("${DATA_PATH}", "TAXI_sample_data_senml.csv")

with open(dataset_path) as f:
    reader = csv.reader(f)
    header = next(reader)
    count = 0
    for row in reader:
        msg = dict(zip(header, row))
        producer.send(topic, msg)
        count += 1
        if count % 100 == 0:
            print(f"Enviadas {count} mensagens...")
        time.sleep(0.05)  # 20 mensagens/segundo
producer.flush()
print("✅ Envio concluído.")
EOF
