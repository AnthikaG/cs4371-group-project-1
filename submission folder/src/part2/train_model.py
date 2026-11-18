"""Train and evaluate an AG News text-classification pipeline."""

import argparse
from pathlib import Path
from typing import Optional

from src.common.schemas import AG_NEWS_LABELS
from src.common.spark_utils import build_spark_session

SEED = 4371
NUM_FEATURES = 1 << 18
MIN_DOC_FREQ = 2
MAX_ITER = 20
REG_PARAM = 0.01


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ag_news_parquet_path", type=Path)
    parser.add_argument("model_output_path", type=Path)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
    from pyspark.sql import functions as F

    spark = build_spark_session("ag-news-training")
    try:
        raw_data = spark.read.parquet(str(args.ag_news_parquet_path))
        missing_columns = {"text", "label"} - set(raw_data.columns)
        if missing_columns:
            raise ValueError(f"AG News Parquet is missing columns: {sorted(missing_columns)}")

        data = (
            raw_data.select(
                F.col("text").cast("string").alias("text"),
                F.col("label").cast("double").alias("label"),
            )
            .filter(F.col("text").isNotNull() & (F.length(F.trim("text")) > 0))
            .filter(F.col("label").isin(*[float(label) for label in AG_NEWS_LABELS]))
        )

        print("AG News articles per class:")
        counts = {int(row["label"]): row["count"] for row in data.groupBy("label").count().collect()}
        for label, name in AG_NEWS_LABELS.items():
            print(f"  {label} ({name}): {counts.get(label, 0)}")

        training_data, validation_data = data.randomSplit([0.9, 0.1], seed=SEED)
        if training_data.limit(1).count() == 0 or validation_data.limit(1).count() == 0:
            raise ValueError("The fixed-seed 90/10 split must have training and validation rows")

        # 2^18 feature bins provide room for the large training corpus
        pipeline = Pipeline(
            stages=[
                RegexTokenizer(
                    inputCol="text",
                    outputCol="tokens",
                    pattern=r"\W+",
                    toLowercase=True,
                    minTokenLength=2,
                ),
                StopWordsRemover(
                    inputCol="tokens",
                    outputCol="filtered_tokens",
                    stopWords=StopWordsRemover.loadDefaultStopWords("english"),
                ),
                HashingTF(
                    inputCol="filtered_tokens",
                    outputCol="raw_features",
                    numFeatures=NUM_FEATURES,
                ),
                IDF(
                    inputCol="raw_features",
                    outputCol="features",
                    minDocFreq=MIN_DOC_FREQ,
                ),
                LogisticRegression(
                    featuresCol="features",
                    labelCol="label",
                    maxIter=MAX_ITER,
                    regParam=REG_PARAM,
                ),
            ]
        )

        model = pipeline.fit(training_data)
        model.write().overwrite().save(str(args.model_output_path))

        validation_predictions = model.transform(validation_data)
        accuracy = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        ).evaluate(validation_predictions)
        print(f"Fixed-seed holdout rows: {validation_predictions.count()}")
        print(f"Fixed-seed holdout accuracy: {accuracy:.6f}")
        print(f"Saved PipelineModel to: {args.model_output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
