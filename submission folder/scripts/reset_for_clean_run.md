# Safely preparing a clean visualization run

This is a manual procedure only. It does not delete Kafka messages, an Elasticsearch index, or a Spark checkpoint.

1. Stop the producer with `Ctrl+C` and wait for its shutdown/flush message. Stop the entity Spark query with `Ctrl+C` as well. Confirm both have stopped before starting replacements.
2. Keep the existing `named-entities-latest` index unless the teammate responsible for Elasticsearch confirms a cleanup plan and an approved backup/retention procedure. The Logstash pipeline writes to this shared index; do not delete or recreate it as a routine reset.
3. Keep the existing Spark checkpoint. Reusing it resumes the same query state and Kafka offsets; it is not a clean new run. If a separate run is required, ask the Spark owner to use a new checkpoint directory and confirm how that app chooses Kafka starting offsets and creates/logs a new `run_id`. Do not remove or overwrite the old checkpoint.
4. The producer’s capture file is opened in append mode. Choose a new capture-file path for a fresh recording rather than reusing a file whose contents you need to preserve.
5. In Kibana, open the dashboard’s search bar and filter to the new Spark run, for example `run_id : "<RUN_ID>"`. Confirm the field/value in Discover first if that query does not match. The saved dashboard currently has no `run_id` filter configured; the saved chart itself groups by entity and takes the maximum `count`.
6. If synthetic replay data has already been published, its documents remain in Elasticsearch. Exclude it using the actual live run ID; do not clear the index to hide it.

## Confirm with the entity-stream teammate

The entity app is referenced in the README and visualization setup guide, but its source file is absent from this checkout. Before the live run, ask the teammate for:

- the exact command and module/script path to start the entity stream;
- whether it can start from the current Kafka end rather than replaying old `topic1` messages;
- the new checkpoint directory and the `run_id` it will report;
- whether the dashboard field should be queried as `run_id` or `run_id.keyword`.

Do not guess these values or start a run with a checkpoint/index reset.
