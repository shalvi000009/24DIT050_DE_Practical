import os
import time
import threading
import pandas as pd
import matplotlib.pyplot as plt

from generate_logs import generate_telemetry_data
from create_topic import create_kafka_topic
from producer import run_producer
from realtime_consumer import run_realtime_consumer
from batch_consumer import run_batch_consumer

def run_benchmarks(records_count=2000, broker="localhost:9092"):
    os.makedirs("results", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    print("\n==================================================")
    print("      STARTING INGESTION BENCHMARK SUITE")
    print("==================================================\n")
    
    # Step 1: Generate Telemetry Dataset
    csv_file = generate_telemetry_data(num_records=records_count, output_file="data/user_logs.csv")
    
    # Step 2: Run Real-Time Ingestion Experiment
    print("\n>>> Phase 1: Real-Time Event Streaming Evaluation <<<")
    create_kafka_topic(bootstrap_servers=broker, topic_name="logs", num_partitions=1, recreate=True)
    
    rt_res = {}
    def run_rt_consumer_thread():
        nonlocal rt_res
        rt_res = run_realtime_consumer(
            topic="logs",
            bootstrap_servers=broker,
            consumer_group="rt_bench_group",
            max_records=records_count,
            timeout_s=20,
            save_benchmark=True
        )
        
    consumer_thread = threading.Thread(target=run_rt_consumer_thread)
    consumer_thread.start()
    time.sleep(2) # Give consumer time to register
    
    run_producer(csv_file=csv_file, topic="logs", bootstrap_servers=broker, delay=0.003)
    consumer_thread.join(timeout=30)
    
    # Step 3: Run Micro-Batch Ingestion Experiment
    print("\n>>> Phase 2: Micro-Batch Pipeline Evaluation <<<")
    create_kafka_topic(bootstrap_servers=broker, topic_name="logs", num_partitions=1, recreate=True)
    
    mb_res = {}
    def run_mb_consumer_thread():
        nonlocal mb_res
        mb_res = run_batch_consumer(
            topic="logs",
            bootstrap_servers=broker,
            consumer_group="mb_bench_group",
            batch_size=100,
            max_records=records_count,
            timeout_s=20,
            save_benchmark=True
        )
        
    consumer_thread2 = threading.Thread(target=run_mb_consumer_thread)
    consumer_thread2.start()
    time.sleep(2) # Give consumer time to register
    
    run_producer(csv_file=csv_file, topic="logs", bootstrap_servers=broker, delay=0.003)
    consumer_thread2.join(timeout=30)
    
    print("\n==================================================")
    print("      BENCHMARK SUITE COMPLETED SUCCESSFULLY")
    print("==================================================\n")
    
    # Step 4: Generate Charts
    generate_charts("data/benchmark_results.csv")

def generate_charts(benchmark_csv="data/benchmark_results.csv"):
    if not os.path.exists(benchmark_csv):
        print(f"[Visualizer] Error: benchmark results file '{benchmark_csv}' not found.")
        return
        
    df = pd.read_csv(benchmark_csv)
    print("\n--- Empirical Benchmark Results ---")
    print(df.to_string(index=False))
    print("-----------------------------------\n")

    plt.style.use('ggplot')
    colors = ['#1f77b4', '#ff7f0e']

    # Chart 1: Latency Comparison (Average, Min, Max)
    plt.figure(figsize=(9, 6))
    archs = df["architecture"]
    x = range(len(archs))
    width = 0.25

    plt.bar([i - width for i in x], df["min_latency_ms"], width=width, label="Min Latency (ms)", color='#2ca02c')
    plt.bar(x, df["average_latency_ms"], width=width, label="Avg Latency (ms)", color='#1f77b4')
    plt.bar([i + width for i in x], df["max_latency_ms"], width=width, label="Max Latency (ms)", color='#d62728')

    plt.title("Ingestion Latency Comparison: Micro-Batch vs Real-Time Streaming", fontsize=13, fontweight='bold', pad=15)
    plt.xlabel("Architecture", fontsize=11, fontweight='bold')
    plt.ylabel("Latency (milliseconds)", fontsize=11, fontweight='bold')
    plt.xticks(x, archs, fontsize=11)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)

    for i in x:
        plt.text(i - width, df["min_latency_ms"].iloc[i] + 2, f"{df['min_latency_ms'].iloc[i]:.1f}", ha='center', fontsize=9)
        plt.text(i, df["average_latency_ms"].iloc[i] + 2, f"{df['average_latency_ms'].iloc[i]:.1f}", ha='center', fontsize=9, fontweight='bold')
        plt.text(i + width, df["max_latency_ms"].iloc[i] + 2, f"{df['max_latency_ms'].iloc[i]:.1f}", ha='center', fontsize=9)

    plt.tight_layout()
    chart1_path = "results/latency_comparison.png"
    plt.savefig(chart1_path, dpi=300)
    plt.close()
    print(f"[Visualizer] Latency comparison chart saved -> {chart1_path}")

    # Chart 2: Throughput Comparison
    plt.figure(figsize=(8, 5.5))
    bars = plt.bar(df["architecture"], df["throughput_records_per_second"], color=colors, width=0.45)
    plt.title("Ingestion Throughput Comparison", fontsize=13, fontweight='bold', pad=15)
    plt.xlabel("Architecture", fontsize=11, fontweight='bold')
    plt.ylabel("Throughput (records / sec)", fontsize=11, fontweight='bold')
    plt.grid(True, linestyle='--', alpha=0.5)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + (height * 0.02),
                 f'{height:.2f} rec/s', ha='center', va='bottom', fontweight='bold', fontsize=10)

    plt.tight_layout()
    chart2_path = "results/throughput_comparison.png"
    plt.savefig(chart2_path, dpi=300)
    plt.close()
    print(f"[Visualizer] Throughput comparison chart saved -> {chart2_path}")

if __name__ == "__main__":
    run_benchmarks()
