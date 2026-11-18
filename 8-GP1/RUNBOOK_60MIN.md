# 60-minute live run checklist

Use a new capture-file path and output directory for each run. Keep existing Kafka data, Elasticsearch indices, and checkpoints unless the responsible teammate has explicitly approved a preservation/cleanup plan.

1. **Start the infrastructure.** From the repository root, run:

   ```bash
   docker compose up -d
   docker compose ps
   ```

   Verify Kafka, Elasticsearch, Kibana, and Logstash are running; Elasticsearch should be healthy. If a service is unhealthy, inspect `docker compose logs <service>` and resolve that before continuing.

2. **Start the producer with no message limit.** In a separate terminal, use a unique capture path:

   ```bash
   python -m src.ingestion.producer --source bluesky --bootstrap-servers localhost:9092 --topic topic1 --capture-file outputs/topic1_capture_<RUN_ID>.jsonl
   ```

   Do not add `--max-messages` or `--duration-minutes`. Verify the producer reports a Jetstream connection and periodically increasing sent count; verify the JSONL file grows. If Kafka is unreachable, check `docker compose ps` and the host listener at `localhost:9092`. If Jetstream cannot connect, let its retry loop continue or resolve the network issue.

3. **Start the existing entity Spark app.** The app’s source is not in this checkout, despite references in README and the visualization setup guide. Replace this placeholder with the entity teammate’s confirmed command; do not treat the placeholder as executable:

   ```text
   <ENTITY_APP_COMMAND>
   ```

   Verify it reads `topic1`, publishes `topic2`, and reports its `run_id`. Confirm the checkpoint and starting-offset behavior with its owner; do not delete or overwrite a checkpoint. If this app is unavailable, the Kibana entity chart will not receive new counts, so pause the interval run rather than substituting synthetic messages.

4. **Start the Part 2 classification query during the same hour.** This query independently reads `topic1`, loads the trained AG News PipelineModel, and prints its cumulative per-class counts every 30 seconds:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/part2/stream_classify.py --bootstrap-servers localhost:9092 --input-topic topic1 --model-path <MODEL_DIR> --checkpoint-dir <CLASSIFICATION_CHECKPOINT_DIR>
   ```

   Verify each trigger prints class counts. Use a new checkpoint directory for a new independent run; preserve existing checkpoint directories. If the connector cannot be resolved, verify Java/Spark setup and the connector coordinate shown above.

5. **Start the interval capture only after Kibana is showing live entities.** Verify that the dashboard is filtered to the entity app’s real `run_id`; this saved dashboard does not include that filter by default. In another terminal, set a writable, run-specific output directory and run:

   ```bash
   python scripts/capture_kibana_intervals.py --dashboard-url <KIBANA_DASHBOARD_URL> --elasticsearch-url <ELASTICSEARCH_URL> --output-dir <INTERVAL_OUTPUT_DIR>
   ```

   The default schedule captures at 15, 30, 45, and 60 minutes from script start. Verify each `top10_<N>min.png`, matching CSV, and timestamped `run_log.txt` appears. For an immediate one-shot connectivity/screenshot check before the timed run, use:

   ```bash
   python scripts/capture_kibana_intervals.py --dashboard-url <KIBANA_DASHBOARD_URL> --elasticsearch-url <ELASTICSEARCH_URL> --output-dir <TEST_OUTPUT_DIR> --test
   ```

   Do not reuse the test output directory for the timed run. If a service is unreachable, the capture log reports which URL failed; fix it and rerun into a new output directory. Keep the producer, entity query, and classification query running until after the 60-minute capture completes.
