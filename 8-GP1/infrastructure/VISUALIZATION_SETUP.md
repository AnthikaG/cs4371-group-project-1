
# Named Entity Visualization Setup

## Overview

This project visualizes cumulative named-entity counts using:

Kafka topic1 → Spark Structured Streaming → Kafka topic2 → Logstash → Elasticsearch → Kibana

## Prerequisites

- Docker Desktop
- Python environment with project dependencies
- Java 17 for Apache Spark

## 1. Start the infrastructure

From the repository root:

```bash
docker compose up -d
```

Verify that Kafka, Elasticsearch, Logstash, and Kibana are running:

```bash
docker compose ps
```

## 2. Create Kafka topics

```bash
for topic in topic1 topic2 topic3; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic"
done
```

## 3. Start Spark entity extraction

After installing the Python dependencies, run:

```bash
python -m src.streaming.entity_stream \
  --bootstrap-servers localhost:9092 \
  --input-topic topic1 \
  --output-topic topic2 \
  --checkpoint-dir checkpoints/entities \
  --trigger-seconds 10
```

Spark consumes validated JSON messages from topic1 and publishes cumulative named-entity counts to topic2.

## 4. Open Kibana

Open http://localhost:5601.

Navigate to Stack Management → Saved Objects → Import.

Import:

`infrastructure/kibana/named-entity-dashboard.ndjson`

Open the dashboard:

`CS4371 - Named Entity Dashboard`

## 5. View the latest entities

The dashboard uses the Elasticsearch index:

`named-entities-latest`

Set the time range to include the current Spark run.

Optionally filter by:

`run_id: "<your-spark-run-id>"`

The visualization displays the Top 10 named entities using the maximum cumulative count for each entity.

## 6. Verify Elasticsearch

```bash
curl -s "http://localhost:9200/named-entities-latest/_count?pretty"
```

## Evidence

A synthetic-data integration-test screenshot is stored at:

`evidence/visualization/spark-entity-dashboard.png`

The final group demonstration should use the team's intended input data source and capture the required time-interval screenshots.

## Notes

- The Logstash pipeline uses one worker to preserve processing order.
- Elasticsearch document upserts prevent duplicate documents for repeated snapshots of the same entity and run.
- Delayed or out-of-order snapshots may still require additional handling.
- Do not delete Spark checkpoints when restarting an existing run.

## Interval screenshots

Install Playwright and its headless Chromium browser in the Python environment used to run the capture script:

```bash
pip install playwright
playwright install chromium
```

The dashboard URL, Elasticsearch URL, and output directory are supplied as command-line arguments or environment variables. Kibana and Elasticsearch must be reachable before capture. The default schedule is 15, 30, 45, and 60 minutes after the script starts:

```bash
python scripts/capture_kibana_intervals.py \
  --dashboard-url <KIBANA_DASHBOARD_URL> \
  --elasticsearch-url <ELASTICSEARCH_URL> \
  --output-dir <INTERVAL_OUTPUT_DIR>
```

Take an immediate one-shot screenshot to check browser and service connectivity before the timed run:

```bash
python scripts/capture_kibana_intervals.py \
  --dashboard-url <KIBANA_DASHBOARD_URL> \
  --elasticsearch-url <ELASTICSEARCH_URL> \
  --output-dir <TEST_OUTPUT_DIR> \
  --test
```

Use a different output directory for the timed run; existing screenshots and CSVs are not overwritten. Each capture writes a timestamped `run_log.txt`, `top10_<N>min.png`, and `top10_<N>min.csv`. The saved dashboard currently has no run-ID filter, so filter its Kibana search bar to the actual Spark `run_id` before the run.
