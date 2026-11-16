# CS4371 Group Project 1: Spark Streaming with Kafka + Text Classification

Team: `8`, Ashi, Tulasi, Anthika, Sristi. See `contract.md` for the shared interfaces.

```
live source (Bluesky Jetstream) -> producer.py -> Kafka topic1
   topic1 -> Spark entity_stream.py -> topic2 -> Logstash -> Elasticsearch -> Kibana (top-10 bar plot)
   topic1 -> Spark classification_stream.py (saved AG News PipelineModel) -> console / topic3
   captured topic1 JSONL -> train.py / batch_predict.py / manual_review.py (Part 2)
```

## 1. prerequisites
- Windows 11 + WSL2 (Ubuntu) **or** macOS/Linux
- Docker Desktop (with WSL integration turned on for Ubuntu)
- Python 3.10+ and Java 17 (Java is needed by Spark): `sudo apt install -y openjdk-17-jdk python3-venv`
- Git

## 2. setup (from the repo root)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # edit only if your Kafka is not on localhost:9092
```

## 3. start Kafka and create the topics
```bash
docker compose up -d kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Stop with `docker compose stop` (keeps the data). `docker compose down` **deletes** the topics.

## 4. run the producer (Part 1 data source)
The data source is **Bluesky Jetstream**: public posts in real time over a WebSocket, with no account or API key.
English posts only (`langs` field), randomly sampled (default 20%) because the full firehose is very high volume.
```bash
python -m src.ingestion.producer --source bluesky --capture-file outputs/topic1_capture.jsonl
```
- `ctrl+c` stops it cleanly (pending messages are flushed).
- `--capture-file` also saves every message to JSONL so Part 2 has a copy even if Kafka is reset.
- Other useful flags: `--max-messages N`, `--duration-minutes M`, `--sample-rate 0.05`, `--dry-run` (print only, no Kafka).
- Fallback source: `--source hackernews` (Hacker News API, no key; much slower, roughly a few items per minute).
- Teammates can test with the synthetic fixture (not real data):
  `python -m src.ingestion.producer --source replay --replay-file data/sample/topic1_sample.jsonl`

Check messages are arriving:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 5
```

Count raw vs cleaned messages (need >= 1,000 cleaned for Part 2):
```bash
python -m src.ingestion.count_messages outputs/topic1_capture.jsonl --output evidence/ingestion_counts.json
```

## 5. entity stream (Tulasi)
Use Python 3.11 and Java 17 in WSL/Linux/macOS. The normal requirements file includes
spaCy and its trained English model. Start Kafka and create `topic1`/`topic2` as above.

```bash
python -m src.streaming.entity_stream --checkpoint-dir checkpoints/entities
```

The query reads `topic1`, filters invalid messages, extracts entities with spaCy,
and uses Spark `groupBy`/`count` to maintain cumulative totals. Every 10 seconds with
new input, it publishes a complete snapshot to `topic2`. Names are lowercased with
Unicode casefolding and extra whitespace is removed; entity types remain separate.
Every recognized mention counts, including repeated mentions and all spaCy entity types.

Restart with the same checkpoint directory to continue the same run. To start a new
run, use a new directory; its run ID is generated automatically. Keep `run.json` and
the `spark/` directory together. Kafka publishing is at least once, so consumers must
upsert the latest count for `(run_id, entity_type, entity)` rather than sum snapshots.
Kafka keys identify that combination. This simple version does not deduplicate new
Kafka records by `message_id`: replaying a message adds its mentions again.

Check the output:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning --max-messages 5
```

`--trigger-seconds`, `--max-offsets-per-trigger`, `--bootstrap-servers`, `--input-topic`
and `--output-topic` are configurable. `--available-now` processes current input and exits.
The matching connector is `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`, loaded
by the shared Spark helper. Equivalent explicit launch:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/streaming/entity_stream.py --checkpoint-dir checkpoints/entities
```

Sristi can call `start_entity_query(spark, args)` with the shared Spark session, then
start her independent classification query on `topic1` with a separate checkpoint.
Stop both queries before stopping the shared session. Verified results and limitations
are in `evidence/tulasi_results.json`; the output sample is `data/sample/topic2_sample.jsonl`.

## 6. Logstash / Elasticsearch / Kibana (Ashi)
`[ANTHIKA: compose services, pipeline.conf, Kibana steps]`

## 7. classification (Sristi)
`[SRISTI: train.py, batch_predict.py, manual_review.py, classification_stream.py commands]`

## 8. tests
```bash
python -m pytest
```

## topic1 message format
```json
{"message_id": "at://did:plc:.../app.bsky.feed.post/3k...", "text": "post text", "source": "bluesky_jetstream", "event_time": "2026-11-13T16:01:00.000000Z"}
```

## troubleshooting
- `cannot reach Kafka at localhost:9092`: Docker Desktop not running, or run `docker compose up -d kafka`.
- `docker: command not found` inside Ubuntu: Docker Desktop > Settings > Resources > WSL integration > turn on Ubuntu.
- producer logs `jetstream error ... reconnecting`: network hiccup; it retries on its own and resumes from the last position.
