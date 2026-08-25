import argparse
import json
import os
import time
import pandas as pd
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def run_producer(csv_file="data/user_logs.csv", topic="logs", bootstrap_servers="localhost:9092", delay=0.005, max_records=None):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Data file '{csv_file}' not found. Please run src/generate_logs.py first.")
        
    print(f"[Producer] Connecting to Redpanda/Kafka at {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=0 # Low latency delivery
    )
    print(f"[Producer] Connected successfully.")

    df = pd.read_csv(csv_file)
    if max_records:
        df = df.head(max_records)
        
    total_records = len(df)
    print(f"[Producer] Starting ingestion of {total_records} records into topic '{topic}' (delay={delay}s)...")
    
    start_time = time.time()
    sent_count = 0

    for idx, row in df.iterrows():
        event = row.to_dict()
        # Embed current high-precision epoch timestamp for accurate end-to-end latency measurement
        event["sent_timestamp"] = time.time()
        
        producer.send(topic, value=event)
        sent_count += 1
        
        if sent_count <= 5 or sent_count % 200 == 0 or sent_count == total_records:
            print(f"Sent event {event['event_id']} (User: {event['user']}, Action: {event['action']})")
            
        if delay > 0:
            time.sleep(delay)
            
    producer.flush()
    elapsed = time.time() - start_time
    throughput = sent_count / elapsed if elapsed > 0 else 0
    
    print("\n--- Producer Summary ---")
    print(f"All messages sent successfully.")
    print(f"Total messages: {sent_count}")
    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Producer Throughput: {throughput:.2f} msg/sec")
    print("------------------------\n")
    producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Telemetry Producer")
    parser.add_argument("--csv", type=str, default="data/user_logs.csv", help="Input CSV path")
    parser.add_argument("--topic", type=str, default="logs", help="Kafka topic name")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--delay", type=float, default=0.005, help="Delay between events in seconds")
    parser.add_argument("--max-records", type=int, default=None, help="Maximum records to send")
    args = parser.parse_args()

    run_producer(
        csv_file=args.csv,
        topic=args.topic,
        bootstrap_servers=args.broker,
        delay=args.delay,
        max_records=args.max_records
    )
