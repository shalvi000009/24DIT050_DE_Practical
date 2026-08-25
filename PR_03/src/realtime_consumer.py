import argparse
import json
import os
import time
import pandas as pd
from kafka import KafkaConsumer

def run_realtime_consumer(topic="logs", bootstrap_servers="localhost:9092", consumer_group="realtime_group", max_records=None, timeout_s=15, save_benchmark=False, benchmark_file="data/benchmark_results.csv"):
    print(f"[RealTime Consumer] Connecting to {bootstrap_servers} (Topic: {topic}, Group: {consumer_group})...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=int(timeout_s * 1000)
    )
    
    print("\nReal-Time Consumer Started\n" + "="*30)
    
    latencies = []
    processed_count = 0
    start_time = None

    try:
        for message in consumer:
            recv_time = time.time()
            if start_time is None:
                start_time = recv_time

            event = message.value
            
            # Calculate latency using embedded sent_timestamp (or message timestamp as fallback)
            sent_time = event.get("sent_timestamp", message.timestamp / 1000.0 if message.timestamp else recv_time)
            latency_ms = max(0.1, (recv_time - sent_time) * 1000.0)
            latencies.append(latency_ms)
            processed_count += 1
            
            # Detailed print for first few events, periodic for rest
            if processed_count <= 5 or processed_count % 200 == 0:
                print(f"Event: {event.get('event_id')}")
                print(f"User: {event.get('user')}")
                print(f"Action: {event.get('action')}")
                print(f"Latency: {latency_ms:.2f} ms\n")

            if max_records and processed_count >= max_records:
                print(f"[RealTime Consumer] Reached maximum requested records ({max_records}). Stopping.")
                break

    except Exception as e:
        print(f"[RealTime Consumer] Consumer loop ended/interrupted: {e}")
    finally:
        consumer.close()

    total_duration = (time.time() - start_time) if (start_time and processed_count > 0) else 0.001
    avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
    min_lat = min(latencies) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0.0
    throughput = processed_count / total_duration if total_duration > 0 else 0.0

    print("="*30)
    print("Real-Time Consumer Performance Summary")
    print(f"Records Processed: {processed_count}")
    print(f"Processing Duration: {total_duration:.2f} seconds")
    print(f"Average Latency: {avg_lat:.2f} ms")
    print(f"Minimum Latency: {min_lat:.2f} ms")
    print(f"Maximum Latency: {max_lat:.2f} ms")
    print(f"Throughput: {throughput:.2f} records/sec")
    print("="*30 + "\n")

    results = {
        "architecture": "Real-Time Streaming",
        "records": processed_count,
        "total_time_seconds": round(total_duration, 4),
        "average_latency_ms": round(avg_lat, 2),
        "min_latency_ms": round(min_lat, 2),
        "max_latency_ms": round(max_lat, 2),
        "throughput_records_per_second": round(throughput, 2)
    }

    if save_benchmark and processed_count > 0:
        save_results_to_csv(results, benchmark_file)

    return results

def save_results_to_csv(result_dict, file_path="data/benchmark_results.csv"):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_new = pd.DataFrame([result_dict])
    
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        # Filter out existing row with same architecture to update cleanly
        df_combined = pd.concat([df_existing[df_existing["architecture"] != result_dict["architecture"]], df_new], ignore_index=True)
        df_combined.to_csv(file_path, index=False)
    else:
        df_new.to_csv(file_path, index=False)
        
    print(f"[RealTime Consumer] Benchmark stats saved to '{file_path}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-Time Kafka Consumer")
    parser.add_argument("--topic", type=str, default="logs", help="Kafka topic name")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--group", type=str, default="realtime_group", help="Consumer group ID")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after processing N records")
    parser.add_argument("--timeout", type=float, default=15.0, help="Consumer timeout in seconds")
    parser.add_argument("--save-benchmark", action="store_true", help="Save metrics to CSV")
    args = parser.parse_args()

    run_realtime_consumer(
        topic=args.topic,
        bootstrap_servers=args.broker,
        consumer_group=args.group,
        max_records=args.max_records,
        timeout_s=args.timeout,
        save_benchmark=args.save_benchmark
    )
