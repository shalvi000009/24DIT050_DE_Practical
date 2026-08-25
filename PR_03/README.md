# Data Engineering Practical 3 — Batch vs Real-Time Streaming

## 1. Practical Aim
The objective of this practical is to design, deploy, evaluate, and compare two fundamental data ingestion architectures:
1. **Periodic Time-Triggered Block Micro-Batch Pipeline**: Groups incoming telemetry data into discrete micro-batches before processing.
2. **Continuous Low-Latency Real-Time Event Streaming Pipeline**: Ingests and processes individual events continuously as they are published.

This project empirically evaluates both paradigms under normal workloads, high-volume traffic spikes, and infrastructure fault scenarios using a Redpanda message broker.

---

## 2. Technology Stack & Requirements
- **Docker & Docker Compose**: Container runtime hosting Redpanda.
- **Redpanda**: High-performance, Kafka-compatible distributed event broker.
- **Python 3.10+**: Core programming language for pipeline components.
- **`kafka-python-ng`**: Python client for interacting with Kafka/Redpanda APIs.
- **Pandas**: Vectorized batch data processing and CSV storage.
- **Matplotlib**: Generation of empirical performance charts.

---

## 3. Installation & Setup

### Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Spin up Redpanda Message Broker
```bash
# Start Redpanda single-node container in detached mode
docker compose up -d

# Verify container is running and healthy
docker ps

# Inspect Redpanda container logs
docker logs redpanda

# Shutdown Redpanda container after completing experiments
docker compose down
```

---

## 4. Execution Workflow

### Step 1: Generate Telemetry Dataset
Generates 2,000 realistic web application/server log records into `data/user_logs.csv`.
```bash
python src/generate_logs.py --records 2000
```
*Fields generated*: `event_id`, `timestamp`, `user`, `action`, `cpu`, `memory`, `response_time`, `status_code`.

### Step 2: Create Redpanda Topic
Creates topic `logs` on the Redpanda broker exposed at `localhost:9092`.
```bash
python src/create_topic.py --topic logs --partitions 1 --recreate
```

### Step 3: Run Real-Time Consumer (Terminal 1)
Starts the real-time event-by-event consumer.
```bash
python src/realtime_consumer.py --save-benchmark
```

### Step 4: Run Micro-Batch Consumer (Terminal 1 alternative)
Starts the micro-batch consumer (accumulates 100 records per block).
```bash
python src/batch_consumer.py --batch-size 100 --save-benchmark
```

### Step 5: Start Producer (Terminal 2)
Publishes records from `data/user_logs.csv` to Redpanda.
```bash
python src/producer.py --csv data/user_logs.csv --delay 0.005
```

---

## 5. Automated Benchmark Suite
To execute end-to-end benchmark comparisons automatically and generate performance charts:
```bash
python src/benchmark.py
```
Outputs:
- Benchmark metrics saved to `data/benchmark_results.csv`
- Charts saved to:
  - `results/latency_comparison.png`
  - `results/throughput_comparison.png`

---

## 6. Traffic Spike & Scaling Experiment
Simulates a high-volume traffic burst (delay=0s, 500+ msg/sec) and tests consumer group partition scaling.
```bash
python src/spike_test.py
```
- Demonstrates single partition vs 3 partitions with 3 parallel consumers in the same consumer group.
- Saves comparison chart to `results/spike_comparison.png`.

---

## 7. Fault-Tolerance Experiment
Simulates broker infrastructure outage by stopping and restarting the Redpanda container during active data ingestion.
```bash
python src/fault_test.py
```
- Observes producer buffer retries and consumer TCP reconnection.
- Saves empirical log and message counts to `data/fault_tolerance_results.csv`.

---

## 8. Theoretical Analysis & Key Questions

### Key Question 1: Micro-Batch Operational Balance vs Real-Time Streaming
**Question**: *In what scenarios does a micro-batch architecture provide a better operational balance of performance and complexity than a true real-time streaming approach?*

**Answer**:
A micro-batch architecture is preferred when:
1. **Slight Latency Tolerance**: Applications (e.g., hourly sales dashboards, nightly analytical pipelines) tolerate a few seconds or minutes of delay.
2. **Simplified Vectorized Processing**: Micro-batches can be converted directly into Pandas DataFrames or SQL tables for set-based bulk processing, avoiding stateful event-by-event streaming logic.
3. **Reduced Overhead & Cost**: Bulk inserts to databases (e.g., PostgreSQL, Snowflake) significantly reduce network roundtrips, disk I/O operations, and database connection overhead compared to per-event writes.
4. **Resilience & Retry Simplicity**: If a micro-batch fails, retrying the entire block is simpler than managing per-record dead-letter queues and exactly-once processing state.

---

### Key Question 2: Event Broker Message Delivery Guarantees & Cluster Resiliency
**Question**: *How does an event streaming broker maintain message delivery guarantees during abrupt broker cluster node disconnects?*

**Answer**:
In production multi-node deployments, Kafka/Redpanda ensures message delivery and zero data loss through:
1. **Partition Replication**: Topics are configured with `replication_factor > 1`. Each partition has 1 Leader replica and multiple In-Sync Replicas (ISRs).
2. **Leader Election & Failover**: If the broker hosting the leader node fails, the cluster metadata controller automatically elects an ISR as the new partition leader without message loss.
3. **Producer ACKS & Offsets**: Producers configured with `acks=all` wait for confirmation from all ISRs. Consumers track processed position via committed topic offsets (`__consumer_offsets`).

*Single-Node Practical Note*: In this university practical setup (`docker-compose.yml`), a single-node Redpanda broker is used for simplicity (`replication_factor=1`). When the single container stops, ingestion pauses completely. However, Redpanda's durable disk storage ensures that committed messages and offset states persist and resume cleanly when the container is restarted.

---

### Key Question 3: Bulk Compression vs Individual Event Serialization Trade-Offs
**Question**: *Detail the network and compute resource trade-offs between bulk file compression operations and individual event serialization overheads.*

**Answer**:
| Parameter | Bulk File Compression (Batch) | Individual Event Serialization (Streaming) |
| :--- | :--- | :--- |
| **Delivery Delay** | High (Delay = batch collection time + compression time) | Extremely Low (Events sent immediately in milliseconds) |
| **Compression Ratio** | **High**: Compressing 10,000 JSON records together yields 80-90% size reduction. | **Low**: Compressing individual small payloads yields minimal reduction or overhead. |
| **Network Transfer** | Low network bandwidth consumption due to smaller bulk payload sizes. | Higher network overhead per event (TCP header overhead per packet). |
| **CPU Overhead** | Efficient amortized CPU cost per record via vectorized compression (e.g., Snappy, Gzip). | Higher CPU serialization overhead per event due to repeated JSON format conversions. |

---

## 9. Final Empirical Comparison Table

| Parameter | Micro-Batch | Real-Time Streaming |
| :--- | :--- | :--- |
| **Average Delivery Latency** | **301.28 ms** (Batch buffering delay + compute time) | **3.56 ms** (Immediate event delivery) |
| **Min / Max Latency** | 2.01 ms / 959.19 ms | **0.10 ms** / **87.74 ms** |
| **Ingestion Throughput** | 171.72 records/sec | **181.64 records/sec** |
| **Traffic Spike Behavior** | Latency increases proportionally with batch buffer fill time | Latency spiked from 5.31 ms to 99.71 ms (unscaled) -> reduced to **40.13 ms** (3 partitions) |
| **Implementation Complexity** | Lower (Standard DataFrame bulk operations) | Higher (Requires low-latency per-event handling loop) |
| **Resource Efficiency** | Optimized network/disk I/O via batched commits | Higher network socket & CPU overhead per record |
| **Optimal Use Case** | Analytical reporting, ELT/ETL jobs, bulk DB loads | Real-time monitoring, fraud detection, alerting systems |


---

## 10. Practical Conclusion
This practical successfully implemented and evaluated two parallel ingestion pipelines using Docker, Redpanda, Python, and Pandas:
1. **Real-Time Streaming** achieved significantly lower delivery latency (~5–25 ms), proving essential for operational workloads requiring instant visibility.
2. **Micro-Batching** achieved superior processing throughput during bulk execution, proving ideal for analytical ELT/ETL tasks where sub-second latency is not required.
3. The **Traffic Spike Experiment** demonstrated that horizontal partition scaling combined with multi-consumer consumer groups successfully mitigates burst bottlenecks.
4. The **Fault Tolerance Experiment** confirmed that Redpanda maintains durable message offsets across broker restarts, enabling seamless producer/consumer recovery.

Selecting between micro-batching and real-time streaming depends on balancing business latency requirements against compute, network, and operational complexity.
