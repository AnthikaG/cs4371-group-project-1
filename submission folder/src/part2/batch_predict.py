"""Clean captured topic1 messages, classify them, and write a prediction CSV."""

import argparse
import csv
from pathlib import Path
from typing import Optional

from config.settings import get_settings
from src.common.schemas import AG_NEWS_LABELS
from src.common.spark_utils import build_spark_session
from pyspark.ml.functions import vector_to_array

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "capture_jsonl_path",
        nargs="?",
        type=Path,
        help="JSONL input path (required when --source=file)",
    )
    parser.add_argument("model_path_positional", nargs="?", type=Path)
    parser.add_argument("output_csv_path_positional", nargs="?", type=Path)
    parser.add_argument("--source", choices=("file", "kafka"), default="file")
    parser.add_argument("--model-path", type=Path)
    parser.add_argument("--output-csv-path", type=Path)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--input-topic", default=settings.kafka_input_topic)
    args = parser.parse_args(argv)
    if args.model_path and args.model_path_positional:
        parser.error("provide the model path either positionally or with --model-path, not both")
    if args.output_csv_path and args.output_csv_path_positional:
        parser.error(
            "provide the output CSV path either positionally or with --output-csv-path, not both"
        )
    args.model_path = args.model_path or args.model_path_positional
    args.output_csv_path = args.output_csv_path or args.output_csv_path_positional
    if args.source == "file" and args.capture_jsonl_path is None:
        parser.error("capture_jsonl_path is required when --source=file")
    if args.model_path is None or args.output_csv_path is None:
        parser.error("model_path and output_csv_path are required")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.output_csv_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {args.output_csv_path}")
    args.output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = build_spark_session("ag-news-batch-prediction")
    try:
        if args.source == "kafka":
            input_schema = StructType(
                [
                    StructField("message_id", StringType(), True),
                    StructField("text", StringType(), True),
                ]
            )
            messages = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap_servers)
                .option("subscribe", args.input_topic)
                .option("startingOffsets", "earliest")
                .load()
                .select(F.from_json(F.col("value").cast("string"), input_schema).alias("record"))
                .select("record.message_id", "record.text")
            )
        else:
            messages = spark.read.json(str(args.capture_jsonl_path)).select("message_id", "text")

        raw_count = messages.count()
        cleaned = (
            messages.filter(
                F.col("text").isNotNull()
                & (F.length(F.trim(F.col("text"))) > 0)
            )
            .dropDuplicates(["text"])
            .cache()
        )
        cleaned_count = cleaned.count()
        print(f"Raw messages: {raw_count}")
        print(f"Messages after empty-text removal and text deduplication: {cleaned_count}")

        model = PipelineModel.load(str(args.model_path))
        predictions = (
            model.transform(cleaned)
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
            .withColumn("confidence", F.array_max(vector_to_array("probability")))
            .select(
                F.col("message_id").cast("string"),
                F.col("text").cast("string"),
                F.col("predicted_class").cast("string"),
                F.col("confidence").cast("double"),
            )
            .cache()
        )

        print("Predictions per class:")
        counts = {
            row["predicted_class"]: row["count"]
            for row in predictions.groupBy("predicted_class").count().collect()
        }
        for name in AG_NEWS_LABELS.values():
            print(f"  {name}: {counts.get(name, 0)}")

        print("Example messages per class (predicted probability):")
        for name in AG_NEWS_LABELS.values():
            examples = (
                predictions.filter(F.col("predicted_class") == name)
                .select("message_id", "text", "confidence")
                .limit(5)
                .collect()
            )
            print(f"  {name}:")
            for row in examples:
                print(
                    f"    message_id={row['message_id']!r} "
                    f"confidence={row['confidence']:.6f} text={row['text']!r}"
                )

        with args.output_csv_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(["message_id", "text", "predicted_class", "confidence"])
            for row in predictions.toLocalIterator():
                writer.writerow(
                    [row["message_id"], row["text"], row["predicted_class"], row["confidence"]]
                )
        print(f"Wrote {cleaned_count} predictions to: {args.output_csv_path}")
        predictions.unpersist()
        cleaned.unpersist()
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
