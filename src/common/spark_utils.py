"""Spark helpers shared by the streaming and ML modules."""

import os

# must match the installed pyspark version and its scala build
SPARK_VERSION = "3.5.3"
KAFKA_CONNECTOR_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def build_spark_session(app_name: str):
    """Create a local SparkSession with the Kafka connector on the classpath."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
