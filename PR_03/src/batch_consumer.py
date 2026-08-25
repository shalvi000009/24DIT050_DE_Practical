import argparse
import json
import os
import time
import pandas as pd
from kafka import KafkaConsumer

def run_batch_consumer(topic="logs", bootstrap_servers="localhost:9092", consumer_group="batch_group", batch_size=100, max_records=None, timeout_s=15, save_benchmark=False, benchmark_file="data/benchmark_results.csv"):
    print(f"[Micro-Batch Consumer] Connecting to {bootstrap_servers} (Batch Size: {batch_size}, Topic: {topic})...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=int(timeout_s * 1000)
    )

    print("\nBatch Consumer Started\n" + "="*30)

    all_latencies = []
    total_records = 0
    start_time = None
    batch_buffer = []
    
    try:
        for message in consumer:
            recv_time = time.time()
            if start_time is None:
                start_time = recv_time

            batch_buffer.append((message.value, recv_time))
            
            # Process batch when buffer reaches batch_size
            if len(batch_buffer) >= batch_size:
                process_batch(batch_buffer, all_latencies)
                total_records += len(batch_buffer)
                batch_buffer = []
                
            if max_records and total_records + len(batch_buffer) >= max_records:
                if batch_buffer:
                    process_batch(batch_buffer, all_latencies)
                    total_records += len(batch_buffer)
                    batch_buffer = []
                print(f"[Micro-Batch Consumer] Reached target records ({max_records}). Stopping.")
                break

        # Process any remaining items in buffer upon consumer timeout/exit
        if batch_buffer:
            process_batch(batch_buffer, all_latencies)
            total_records += len(batch_buffer)
            batch_buffer = []

    except Exception as e:
        print(f"[Micro-Batch Consumer] Loop ended/interrupted: {e}")
    finally:
        consumer.close()

    total_duration = (time.time() - start_time) if (start_time and total_records > 0) else 0.001
    avg_lat = sum(all_latencies) / len(all_latencies) if all_latencies else 0.0
    min_lat = min(all_latencies) if all_latencies else 0.0
    max_lat = max(all_latencies) if all_latencies else 0.0
    throughput = total_records / total_duration if total_duration > 0 else 0.0

    print("="*30)
    print("Micro-Batch Consumer Performance Summary")
    print(f"Records Processed: {total_records}")
    print(f"Processing Duration: {total_duration:.2f} seconds")
    print(f"Average Latency: {avg_lat:.2f} ms")
    print(f"Minimum Latency: {min_lat:.2f} ms")
    print(f"Maximum Latency: {max_lat:.2f} ms")
    print(f"Throughput: {throughput:.2f} records/sec")
    print("="*30 + "\n")

    results = {
        "architecture": "Micro-Batch",
        "records": total_records,
        "total_time_seconds": round(total_duration, 4),
        "average_latency_ms": round(avg_lat, 2),
        "min_latency_ms": round(min_lat, 2),
        "max_latency_ms": round(max_lat, 2),
        "throughput_records_per_second": round(throughput, 2)
    }

    if save_benchmark and total_records > 0:
        save_results_to_csv(results, benchmark_file)

    return results

def process_batch(batch_buffer, latencies_list):
    b_start = time.time()
    events = [item[0] for item in batch_buffer]
    df = pd.DataFrame(events)
    
    # Simulate batch analytics (aggregation, grouping)
    _summary = df.groupby("action")["response_time"].mean()
    
    b_end = time.time()
    b_proc_time = b_end - b_start
    
    # Calculate end-to-end latency for events in this batch (sent_timestamp -> batch completion)
    batch_latencies = []
    for item, recv_t in batch_buffer:
        sent_t = item.get("sent_timestamp", recv_t)
        lat_ms = max(0.1, (b_end - sent_t) * 1000.0)
        batch_latencies.append(lat_ms)
        latencies_list.append(lat_ms)
        
    avg_batch_lat = sum(batch_latencies) / len(batch_latencies)
    
    print("\nBatch Received")
    print(f"Records Processed: {len(events)}")
    print(f"Batch Processing Time: {b_proc_time:.4f} seconds")
    print(f"Average Latency: {avg_batch_lat:.2f} ms")

def save_results_to_csv(result_dict, file_path="data/benchmark_results.csv"):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_new = pd.DataFrame([result_dict])
    
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        df_combined = pd.concat([df_existing[df_existing["architecture"] != result_dict["architecture"]], df_new], ignore_index=True)
        df_combined.to_csv(file_path, index=False)
    else:
        df_new.to_csv(file_path, index=False)
        
    print(f"[Micro-Batch Consumer] Benchmark stats saved to '{file_path}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Micro-Batch Kafka Consumer")
    parser.add_argument("--topic", type=str, default="logs", help="Kafka topic name")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--group", type=str, default="batch_group", help="Consumer group ID")
    parser.add_argument("--batch-size", type=int, default=100, help="Micro-batch record limit")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after processing N records")
    parser.add_argument("--timeout", type=float, default=15.0, help="Consumer timeout in seconds")
    parser.add_argument("--save-benchmark", action="store_true", help="Save metrics to CSV")
    args = parser.parse_args()

    run_batch_consumer(
        topic=args.topic,
        bootstrap_servers=args.broker,
        consumer_group=args.group,
        batch_size=args.batch_size,
        max_records=args.max_records,
        timeout_s=args.timeout,
        save_benchmark=args.save_benchmark
    )
