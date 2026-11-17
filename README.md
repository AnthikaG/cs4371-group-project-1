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
`[TULASI: command for src/streaming/entity_stream.py]`

## 6. Logstash / Elasticsearch / Kibana (Ashi)
`[ANTHIKA: compose services, pipeline.conf, Kibana steps]`

## 7. Part 2: AG News text classification (Sristi)

The [AG News dataset card](https://huggingface.co/datasets/fancyzhx/ag_news) confirms the four-class mapping: `0=World`, `1=Sports`, `2=Business`, `3=Sci/Tech`. The trainer prints the observed class counts and a fixed-seed 10% holdout accuracy; fill report values from the actual output.

Run these commands from the repository root, in order. Replace each angle-bracket placeholder with your own path. The Kafka connector coordinate for this repo's PySpark 3.5.3 / Scala 2.12 build is `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`.

1. Train and save the PipelineModel:

   ```bash
   python -m src.part2.train_model <AG_NEWS_PARQUET> <MODEL_DIR>
   ```

2. Clean and classify the captured topic1 JSONL:

   ```bash
   python -m src.part2.batch_predict <CAPTURE_JSONL> <MODEL_DIR> <PREDICTIONS_CSV>
   ```

   To read topic1 directly instead, omit the JSONL positional argument and select Kafka:

   ```bash
   python -m src.part2.batch_predict --source kafka --bootstrap-servers localhost:9092 --input-topic topic1 --model-path <MODEL_DIR> --output-csv-path <PREDICTIONS_CSV>
   ```

3. Make the blind 50-message label sheet, fill `my_label` yourself using `World`, `Sports`, `Business`, `Sci/Tech`, or `None of these`, then evaluate:

   ```bash
   python -m src.part2.make_label_sheet <PREDICTIONS_CSV> <LABEL_SHEET_CSV>
   python -m src.part2.evaluate_labels <PREDICTIONS_CSV> <COMPLETED_LABEL_SHEET_CSV> <HAND_LABELS_VS_MODEL_CSV>
   ```

4. Start the independent live classification stream. It prints per-class running counts every 30 seconds; use a new checkpoint path for a new run:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/part2/stream_classify.py --bootstrap-servers localhost:9092 --input-topic topic1 --model-path <MODEL_DIR> --checkpoint-dir <CHECKPOINT_DIR>
   ```

   For a one-off query, `--checkpoint-dir` may be omitted. The optional topic3 sink is commented in the script because it needs coordination with the entity-stream owner.

See [src/part2/REPORT_NOTES.md](src/part2/REPORT_NOTES.md) for the report skeleton and [RUNBOOK_60MIN.md](RUNBOOK_60MIN.md) for the coordinated live run.

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
