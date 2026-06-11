# Energy Reduction Streaming Pipeline

A real-time pipeline for detecting abnormal energy consumption in industrial sensor data. Sensor readings stream through Kafka, get processed by Spark Streaming, and are scored by two ML models before landing in InfluxDB — all visible live in Grafana. The whole stack runs locally with one `docker-compose up`.

---

## Architecture

```
[Sensor Source]
      │
      ▼
[Kafka Broker + Zookeeper]   ←→   [Schema Registry]
      │                              (Avro contracts)
      ▼
[Spark Streaming Job]
      │
      ├──► Isolation Forest Model    (unsupervised: is this abnormal?)
      │
      └──► Classification Model      (supervised: what kind of anomaly?)
                  │
                  ▼
            [InfluxDB 1.8]           (time-series storage)
                  │
                  ▼
          [Grafana Dashboard]        (live monitoring)
```

The Isolation Forest runs first on every event. Only events it flags as anomalous are passed to the classifier. This keeps the classifier focused and reduces false positives — the unsupervised model handles the unlabeled edge cases, the classifier makes them actionable.

---

## Tech Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka (Confluent 7.4.0) |
| Schema enforcement | Confluent Schema Registry |
| Kafka monitoring | Confluent Control Center |
| Stream processing | Apache Spark (Bitnami, standalone cluster) |
| Anomaly detection | Isolation Forest (`scikit-learn`) |
| Anomaly classification | Supervised classifier (`scikit-learn`) |
| Time-series storage | InfluxDB 1.8 |
| Visualization | Grafana 8.4.3 |
| Orchestration | Docker Compose |

---

## Data Flow

1. **Producer** reads sensor readings and publishes to a Kafka topic, serialized as Avro against the Schema Registry
2. **Kafka broker** buffers messages with replication and offset tracking; Schema Registry rejects any malformed events upstream
3. **Spark Streaming job** subscribes to the topic and processes micro-batches
4. Each batch runs through the **Isolation Forest** — the pre-trained model (`model_iforest.pkl`) is mounted directly into the Spark worker container
5. Flagged events are forwarded to the **classification model**, which assigns an anomaly type (spike, drift, flatline)
6. Results are written to **InfluxDB** (`iber` database) with sensor ID and anomaly type as tags
7. **Grafana** queries InfluxDB in real time and renders the live dashboard

---

## Key Features

- **Two-stage ML inference in the stream** — unsupervised detection followed by supervised classification, without a separate batch scoring job
- **Schema Registry enforcement** — malformed or schema-drifted events are rejected at the broker level, not discovered downstream
- **Full observability** — Grafana shows anomaly rates, event type breakdown, throughput, and raw sensor readings side-by-side
- **Reproducible local environment** — Docker Compose with health checks and `depends_on` conditions ensures services start in the right order every time
- **Spark standalone cluster** — master + worker defined in Compose, so you can tune cores and memory without a cloud cluster

---

## Setup

### Requirements
- Docker Desktop (or Docker Engine + Compose plugin)
- ~8 GB RAM free for the full stack
- Python 3.8+ (for running producer/scripts locally)

### 1. Clone
```bash
git clone https://github.com/shehabziada130/Energy-Reduction-Streaming-pipeline.git
cd Energy-Reduction-Streaming-pipeline
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the stack
```bash
docker-compose up -d
```

Services start in this order (health checks enforce it):
`Zookeeper → Kafka Broker → Schema Registry → Control Center → Spark → InfluxDB → Grafana`

Wait ~60–90 seconds for all health checks to pass.

### 4. Verify everything is up

| Service | URL | Credentials |
|---|---|---|
| Confluent Control Center | http://localhost:9021 | — |
| Schema Registry | http://localhost:8081 | — |
| Spark Master UI | http://localhost:9090 | — |
| InfluxDB | http://localhost:8086 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

---

## How to Run

### Start the Kafka producer
```bash
python Kafka/producer.py
```

This simulates sensor readings and publishes them to the Kafka topic.

### Submit the Spark streaming job
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/Spark/streaming_job.py
```

The job picks up messages, runs both models, and writes results to InfluxDB.

### Load the Grafana dashboard

1. Open http://localhost:3000 and log in
2. Add InfluxDB as a data source: URL `http://influxdb:8086`, database `iber`
3. Import the dashboard JSON from `Grafana/`

---

## Example / Use Case

An industrial facility runs dozens of machines, each publishing energy readings every few seconds. Normal consumption follows predictable patterns by time of day. This pipeline flags when a machine's draw spikes unexpectedly (potential fault), drops to near-zero (unplanned downtime), or drifts steadily upward (degrading efficiency). Maintenance teams see the alert in Grafana within seconds — not after a nightly batch report.

---

## Project Structure

```
├── Kafka/                      # Producer and consumer scripts
├── Spark/                      # Spark Streaming job
├── Abnormal Detection Model/   # Isolation Forest training notebook + serialized model
├── Classification_model/       # Classifier training and evaluation
├── Grafana/                    # Dashboard JSON exports
├── docker-compose.yml
├── entrypoint.sh
├── requirements.txt
└── how to start.docx           # Step-by-step walkthrough
```

---

## Future Improvements

- Replace the simulated producer with a real MQTT-to-Kafka bridge for actual IoT sensor integration
- Add a dead-letter topic for events that fail schema validation, with alerting
- Move model serving to MLflow so retraining and versioning don't require redeploying the Spark job
- Swap InfluxDB 1.8 for InfluxDB 2.x and migrate dashboards to Flux queries
- Add Kafka lag monitoring to the Grafana dashboard alongside the anomaly panels

