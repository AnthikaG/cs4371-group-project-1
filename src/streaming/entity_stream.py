"""Count named entities from Kafka topic1 and publish cumulative totals to topic2."""

import argparse
import json
import logging
import signal
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.settings import get_settings
from src.common.schemas import utc_iso
from src.common.spark_utils import build_spark_session
from src.streaming.ner import extract_entities, load_model, parse_text

LOGGER = logging.getLogger(__name__)


def get_run_id(checkpoint: Path, configuration: dict, requested_id: str | None) -> str:
    """Keep the same run identity and settings when restoring Spark state."""
    checkpoint.mkdir(parents=True, exist_ok=True)
    metadata_file = checkpoint / "run.json"
    if metadata_file.exists():
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        if metadata["configuration"] != configuration:
            raise ValueError("Settings changed; use a new checkpoint directory and run ID")
        if requested_id is not None and requested_id != metadata["run_id"]:
            raise ValueError("run ID does not match the checkpoint")
        if not (checkpoint / "spark" / "metadata").exists():
            raise ValueError("Spark state is missing; restore the complete checkpoint or start a new run")
        return metadata["run_id"]
    if (checkpoint / "spark").exists():
        raise ValueError("run.json is missing; restore the complete checkpoint")
    run_id = requested_id or str(uuid.uuid4())
    metadata_file.write_text(json.dumps({"run_id": run_id, "configuration": configuration}, indent=2) + "\n",
                             encoding="utf-8")
    return run_id


def start_entity_query(spark, args):
    """Start the entity query using an existing Spark session."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    model = load_model()
    configuration = {"bootstrap_servers": args.bootstrap_servers, "input_topic": args.input_topic,
                     "output_topic": args.output_topic, "model_version": model.meta["version"]}
    run_id = get_run_id(args.checkpoint_dir, configuration, args.run_id)
    entity_schema = ArrayType(StructType([
        StructField("entity", StringType()), StructField("entity_type", StringType()),
    ]))
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap_servers)
              .option("subscribe", args.input_topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true")
              .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger).load())
    texts = source.select(F.udf(parse_text, StringType())("value").alias("text")).where("text IS NOT NULL")
    entities = (texts.select(F.explode(F.udf(extract_entities, entity_schema)("text")).alias("mention"))
                .select("mention.*"))
    counts = entities.groupBy("entity", "entity_type").count()

    def publish_snapshot(batch, batch_id):
        timestamp = utc_iso(datetime.now(timezone.utc))
        snapshot = batch.withColumn("window_end", F.lit(timestamp)).withColumn("run_id", F.lit(run_id))
        identity = F.struct("run_id", "entity_type", "entity")
        payload = F.struct("entity", "entity_type", "count", "window_end", "run_id")
        output = snapshot.select(F.sha2(F.to_json(identity), 256).alias("key"),
                                 F.to_json(payload).alias("value"))
        output.write.format("kafka").option("kafka.bootstrap.servers", args.bootstrap_servers).option(
            "topic", args.output_topic).save()
        LOGGER.info("published cumulative snapshot: batch=%d run_id=%s", batch_id, run_id)

    writer = (counts.writeStream.queryName("entity_counts").outputMode("complete")
              .option("checkpointLocation", str(args.checkpoint_dir / "spark"))
              .foreachBatch(publish_snapshot))
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")
    LOGGER.info("starting entity counts: run_id=%s", run_id)
    return writer.start()


def parse_args(argv=None):
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--input-topic", default=settings.kafka_input_topic)
    parser.add_argument("--output-topic", default=settings.kafka_entity_topic)
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("checkpoints/entities"))
    parser.add_argument("--run-id", help="optional on first start; must match on restart")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=500)
    parser.add_argument("--available-now", action="store_true", help="process current input and exit")
    args = parser.parse_args(argv)
    if args.trigger_seconds <= 0 or args.max_offsets_per_trigger <= 0:
        parser.error("trigger seconds and offset limit must be positive")
    if args.input_topic == args.output_topic:
        parser.error("input and output topics must differ")
    if args.run_id is not None and not args.run_id.strip():
        parser.error("run ID must not be blank")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    spark = build_spark_session("cs4371-entities")
    spark.sparkContext.setLogLevel("WARN")
    query = None

    def stop(signum, frame):
        if query is not None:
            query.stop()

    try:
        query = start_entity_query(spark, args)
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
