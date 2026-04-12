import csv
import json
import time
import sys
import os
from kafka import KafkaProducer

TOPIC = "orders_stream"
BOOTSTRAP_SERVERS = "localhost:9092"

# Create producer with retry logic and longer timeout
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    request_timeout_ms=120000,  # 120 second timeout
    connections_max_idle_ms=540000
)

def main():
    # Get the path relative to the project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    data_file = os.path.join(project_root, "data", "orders.csv")
    
    with open(data_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["order_id"] = int(row["order_id"])
            row["price"] = float(row["price"])
            row["quantity"] = int(row["quantity"])

            producer.send(TOPIC, row)
            print(f"Sent: {row}")
            time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()