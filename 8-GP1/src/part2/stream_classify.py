"""Classify topic1 messages and print running counts by AG News class."""

import argparse
import os
import uuid
from pathlib import Path
from typing import Optional

from config.settings import get_settings
from src.common.schemas import AG_NEWS_LABELS
from src.common.spark_utils import build_spark_session


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--input-topic", default=settings.kafka_input_topic)
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--checkpoint-dir")
    parser.add_argument("--class-topic", default=settings.kafka_class_topic)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.checkpoint_dir and Path(args.checkpoint_dir).exists():
        raise FileExistsError(
            f"Checkpoint path already exists; choose a new path to preserve it: {args.checkpoint_dir}"
        )

    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = build_spark_session("ag-news-live-classification")
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        input_schema = StructType(
            [
                StructField("message_id", StringType(), True),
                StructField("text", StringType(), True),
                StructField("source", StringType(), True),
                StructField("event_time", StringType(), True),
            ]
        )
        topic_messages = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), input_schema).alias("record"))
            .select("record.message_id", "record.text", "record.source", "record.event_time")
            .filter(F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0))
        )

        model = PipelineModel.load(args.model_path)
        predicted = (
            model.transform(topic_messages)
            .withColumn(
                "predicted_class",
                F.create_map(
                    *[
                        item
                        for label, name in AG_NEWS_LABELS.items()
                        for item in (F.lit(float(label)), F.lit(name))
                    ],
                )[F.col("prediction")],
            )
            .select(F.col("predicted_class").alias("class_name"))
        )
        counts = predicted.groupBy("class_name").count()

        console_writer = (
            counts.writeStream.format("console")
            .outputMode("complete")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
        )
        if args.checkpoint_dir:
            console_writer = console_writer.option("checkpointLocation", args.checkpoint_dir)
        console_query = console_writer.start()

        # optional topic3 sink: use foreachBatch because the Kafka streaming sink cannot directly accept complete-mode output
        # to_json(struct(...)) supplies the Kafka value string; each batch writes the full current class-count snapshot
        # uncomment the function and query below when a topic3 consumer is coordinated with the entity app
        #
        # stream_run_id = uuid.uuid4().hex
        #
        # def write_complete_counts_to_kafka(batch_df, batch_id):
        #     kafka_rows = (
        #         batch_df.withColumn("window_end", F.current_timestamp())
        #         .withColumn("run_id", F.lit(stream_run_id))
        #         .selectExpr(
        #             "CAST(class_name AS STRING) AS key",
        #             "to_json(struct(class_name, count, window_end, run_id)) AS value",
        #         )
        #     )
        #     kafka_rows.write.format("kafka").option(
        #         "kafka.bootstrap.servers", args.bootstrap_servers
        #     ).option("topic", args.class_topic).save()
        #
        # kafka_writer = (
        #     counts.writeStream.foreachBatch(write_complete_counts_to_kafka)
        #     .outputMode("complete")
        #     .trigger(processingTime="30 seconds")
        # )
        # if args.checkpoint_dir:
        #     kafka_writer = kafka_writer.option(
        #         "checkpointLocation", os.path.join(args.checkpoint_dir, "topic3")
        #     )
        # kafka_query = kafka_writer.start()

        console_query.awaitTermination()
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
