# CS4371 Group Project 1 Team 8

Run all commands from this directory. Use Python 3.11, Java 17 and Docker Desktop or Docker on Linux. Windows users should run Spark in WSL/Linux or a compatible container. Paths below are relative and can be replaced by CLI arguments.

## Install and start services

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
docker compose up -d
```

Host programs use localhost:9092; containers on the Compose network use kafka:29092. Keep real keys out of Git. The supplied security-disabled Elastic configuration is for local development.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic3
```

## Producer and Spark entity query

Run each command in a separate activated terminal:

```bash
python -m src.streaming.entity_stream --checkpoint-dir checkpoints/entities --trigger-seconds 10
python -m src.ingestion.producer --source bluesky --capture-file outputs/new_capture.jsonl
```

To replay actual collected data instead of collecting new input:

```bash
python -m src.ingestion.producer --source replay --replay-file outputs/demo-live.jsonl
```

The query uses spaCy en_core_web_sm and checkpointed Spark cumulative counts. Keep the same checkpoint to resume; choose a new checkpoint/run ID for a new experiment. Replayed Kafka records contribute again. Topic2 contains entity, entity_type, count, window_end and run_id. Publishing is at least once; successive snapshot totals must not be summed.

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --max-messages 5
```

## Kibana and bar plots

Open http://localhost:5601. In Stack Management > Saved Objects > Import, import infrastructure/kibana/named-entity-dashboard.ndjson. Open CS4371 - Named Entity Dashboard and filter the current run_id/time range. See infrastructure/VISUALIZATION_SETUP.md for details.

The four bar plots in evidence/entity_intervals/ show the latest real retained topic2 snapshot as of 15, 30, 45 and 60 minutes from the run's first published snapshot. They are retrospective plots, not contemporaneous Kibana screenshots. Source timestamps, Kafka offsets and full snapshot data are retained alongside them. Input collection paused after about 30 minutes, so the 45/60-minute plots correctly repeat the last published state. They do not establish an uninterrupted 60-minute live collection. The older screenshot under evidence/visualization/ is synthetic test evidence.

## Data and classification outputs

outputs/demo-live.jsonl contains 4,394 real collected messages. Removing duplicate texts gives 4,335 predictions in outputs/predictions.csv. The confidence column is the predicted-class probability. data/collected/demo_cleaned.jsonl contains those exact predicted message IDs. Class counts and five actual examples per class are in outputs/class_distribution.csv and outputs/prediction_examples.csv. Model identity and cleaning totals are in outputs/prediction_summary.json.

The original 146-message NER validation and Sristi's reported 2,958-message experiment are separate experiments. Do not mix their counts with the new 4,335-message inference output.

## Saved model and reproduction

The supplied Spark 3.5.3 PipelineModel is models/ag_lr. Batch and streaming inference both load this same saved model; no retraining was performed to generate the added prediction CSV.

```bash
python -m src.part2.batch_predict outputs/demo-live.jsonl models/ag_lr outputs/predictions_new.csv
```

For training from scratch, download the AG News training Parquet from https://huggingface.co/datasets/fancyzhx/ag_news and supply its actual relative path:

```bash
python -m src.part2.train_model data/ag_news/train.parquet models/ag_lr_new
```

The training Parquet is not bundled. Training prints class counts and holdout results. The model has RegexTokenizer, StopWordsRemover, HashingTF, IDF and LogisticRegression stages.

## Manual labels

outputs/hand_labels_vs_model_manual.csv transcribes all 50 human labels/model predictions from the user's report Table 5.5. It gives 35 None-of-these, 15 in-scope, 2 correct and 13 incorrect: 13.3333% in-scope accuracy. It contains the supplied sheet-row numbers; original message IDs/text/probabilities were not supplied. These labels belong to the original report sample and are NOT silently joined to the new prediction dataset. See outputs/manual_evaluation_summary.json.

To label a new random 50-message sample yourself:

```bash
python -m src.part2.make_label_sheet outputs/predictions.csv outputs/new_label_sheet.csv
python -m src.part2.label_cli outputs/new_label_sheet.csv
python -m src.part2.evaluate_labels outputs/predictions.csv outputs/new_label_sheet.csv outputs/new_label_comparison.csv
```

## Real-time classification

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/part2/stream_classify.py --bootstrap-servers localhost:9092 --input-topic topic1 --model-path models/ag_lr --checkpoint-dir checkpoints/classification-new
```

Use a new checkpoint directory as required by this classifier. It independently reads topic1 and prints cumulative class counts every 30 seconds. evidence/classification/live_classification.log and its JSON summary verify counts updating from 4,540 to 4,546 Kafka records after six previously collected real messages were replayed. This was a replay-arrival test, not six freshly collected posts. Raw live counts differ from deduplicated batch counts.

## Tests and final files

```bash
python -m pytest -q
```

All four members' supplied AI exports are in ai_transcripts/. Anthika_AI_Transcript.pdf is a direct conversion of the included original DOCX, preserving its text and nine images. Members must verify that their full relevant conversations and all AI assistance are disclosed.

The team will add the final report PDF and accessible demo video link manually. Put the video link on report page 1 with team number, names, NetIDs and contribution table. Then ZIP the contents as 8-GP1.zip. Do not include .env, .venv or active checkpoints. branch_versions/ preserves conflicting original documentation without overwriting it.
