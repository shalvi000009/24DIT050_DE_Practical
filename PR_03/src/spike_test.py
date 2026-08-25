import os
import time
import threading
import pandas as pd
import matplotlib.pyplot as plt

from generate_logs import generate_telemetry_data
from create_topic import create_kafka_topic
from producer import run_producer
from realtime_consumer import run_realtime_consumer

def run_spike_experiment(records_count=3000, broker="localhost:9092"):
    os.makedirs("results", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    print("\n==================================================")
    print("      STARTING TRAFFIC SPIKE EXPERIMENT")
    print("==================================================\n")
    
    csv_file = "data/user_logs.csv"
    if not os.path.exists(csv_file):
        generate_telemetry_data(num_records=records_count, output_file=csv_file)
    
    # Run Test 1: Normal Workload (Paced sending: delay=0.01s ~100 msg/sec)
    print("\n>>> Scenario A: Normal Steady Ingestion Load (Delay=0.01s) <<<")
    create_kafka_topic(bootstrap_servers=broker, topic_name="logs_spike", num_partitions=1, recreate=True)
    
    normal_stats = {}
    def run_normal_consumer():
        nonlocal normal_stats
        normal_stats = run_realtime_consumer(
            topic="logs_spike",
            bootstrap_servers=broker,
            consumer_group="spike_normal_group",
            max_records=1000,
            timeout_s=15
        )
        
    c_thread1 = threading.Thread(target=run_normal_consumer)
    c_thread1.start()
    time.sleep(2)
    
    run_producer(csv_file=csv_file, topic="logs_spike", bootstrap_servers=broker, delay=0.01, max_records=1000)
    c_thread1.join(timeout=25)
    
    # Run Test 2: Unscaled Spike (0 delay rapid burst into single partition)
    print("\n>>> Scenario B: Unscaled Traffic Spike (Burst Ingestion, 1 Partition) <<<")
    create_kafka_topic(bootstrap_servers=broker, topic_name="logs_spike", num_partitions=1, recreate=True)
    
    spike_unscaled_stats = {}
    def run_unscaled_consumer():
        nonlocal spike_unscaled_stats
        spike_unscaled_stats = run_realtime_consumer(
            topic="logs_spike",
            bootstrap_servers=broker,
            consumer_group="spike_unscaled_group",
            max_records=2000,
            timeout_s=15
        )
        
    c_thread2 = threading.Thread(target=run_unscaled_consumer)
    c_thread2.start()
    time.sleep(2)
    
    # Zero delay burst load
    run_producer(csv_file=csv_file, topic="logs_spike", bootstrap_servers=broker, delay=0.0, max_records=2000)
    c_thread2.join(timeout=25)
    
    # Run Test 3: Scaled Traffic Spike (3 Partitions with 3 Parallel Consumers in same Group)
    print("\n>>> Scenario C: Scaled Traffic Spike (3 Partitions, 3 Consumer Threads) <<<")
    create_kafka_topic(bootstrap_servers=broker, topic_name="logs_spike_multi", num_partitions=3, recreate=True)
    
    consumer_results = []
    def run_scaled_consumer_instance(consumer_id):
        stats = run_realtime_consumer(
            topic="logs_spike_multi",
            bootstrap_servers=broker,
            consumer_group="scaled_group",
            max_records=None,
            timeout_s=12
        )
        consumer_results.append(stats)
        
    threads = []
    for cid in range(3):
        t = threading.Thread(target=run_scaled_consumer_instance, args=(cid,))
        t.start()
        threads.append(t)
        
    time.sleep(2) # Allow all 3 consumers to rebalance partitions
    run_producer(csv_file=csv_file, topic="logs_spike_multi", bootstrap_servers=broker, delay=0.0, max_records=2000)
    
    for t in threads:
        t.join(timeout=20)
        
    # Aggregate multi-consumer stats
    total_rec = sum(r.get("records", 0) for r in consumer_results)
    avg_lat = sum(r.get("average_latency_ms", 0) for r in consumer_results) / len(consumer_results) if consumer_results else 0.0
    tot_tp = sum(r.get("throughput_records_per_second", 0) for r in consumer_results)
    
    scaled_stats = {
        "scenario": "Scaled Spike (3 Partitions)",
        "records": total_rec,
        "average_latency_ms": round(avg_lat, 2),
        "throughput_records_per_second": round(tot_tp, 2)
    }
    
    normal_stats["scenario"] = "Normal Workload"
    spike_unscaled_stats["scenario"] = "Unscaled Traffic Spike"
    
    print("\n--- Traffic Spike Summary ---")
    print(f"Normal Load      : Latency = {normal_stats.get('average_latency_ms')} ms, Throughput = {normal_stats.get('throughput_records_per_second')} rec/s")
    print(f"Unscaled Spike   : Latency = {spike_unscaled_stats.get('average_latency_ms')} ms, Throughput = {spike_unscaled_stats.get('throughput_records_per_second')} rec/s")
    print(f"Scaled Spike (3P): Latency = {scaled_stats.get('average_latency_ms')} ms, Combined Throughput = {scaled_stats.get('throughput_records_per_second')} rec/s")
    print("------------------------------\n")
    
    generate_spike_chart(normal_stats, spike_unscaled_stats, scaled_stats)

def generate_spike_chart(normal, unscaled, scaled):
    scenarios = [normal["scenario"], unscaled["scenario"], scaled["scenario"]]
    latencies = [normal.get("average_latency_ms", 0), unscaled.get("average_latency_ms", 0), scaled.get("average_latency_ms", 0)]
    throughputs = [normal.get("throughput_records_per_second", 0), unscaled.get("throughput_records_per_second", 0), scaled.get("throughput_records_per_second", 0)]

    fig, ax1 = plt.subplots(figsize=(9, 5.5))
    
    color = '#1f77b4'
    ax1.set_xlabel('Workload Scenario', fontweight='bold', fontsize=11)
    ax1.set_ylabel('Average Latency (ms)', color=color, fontweight='bold', fontsize=11)
    bars = ax1.bar(scenarios, latencies, color=color, alpha=0.7, width=0.4, label='Latency (ms)')
    ax1.tick_params(axis='y', labelcolor=color)
    
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + (height*0.02),
                 f'{height:.1f} ms', ha='center', va='bottom', color=color, fontweight='bold')

    ax2 = ax1.twinx()
    color = '#ff7f0e'
    ax2.set_ylabel('Throughput (records/sec)', color=color, fontweight='bold', fontsize=11)
    line = ax2.plot(scenarios, throughputs, color=color, marker='o', linewidth=3, markersize=8, label='Throughput (rec/s)')
    ax2.tick_params(axis='y', labelcolor=color)

    for i, txt in enumerate(throughputs):
        ax2.annotate(f'{txt:.1f} rec/s', (scenarios[i], throughputs[i]), textcoords="offset points", xytext=(0,10), ha='center', fontweight='bold', color=color)

    plt.title("Traffic Spike Performance Impact & Multi-Partition Scaling", fontweight='bold', fontsize=13, pad=15)
    fig.tight_layout()
    
    chart_path = "results/spike_comparison.png"
    plt.savefig(chart_path, dpi=300)
    plt.close()
    print(f"[Visualizer] Traffic spike comparison chart saved -> {chart_path}")

if __name__ == "__main__":
    run_spike_experiment()
