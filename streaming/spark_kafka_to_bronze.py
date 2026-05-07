from pathlib import Path

from pyspark.sql import functions as F

from streaming.taxi_schema import taxi_raw_schema
from utils.spark_session import get_spark

PROJECT_ROOT = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "taxi-trips-raw"

BRONZE_STREAMING_PATH = PROJECT_ROOT / "data" / "bronze_streaming" / "taxi_trips_raw"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "taxi_trips_raw"

def run_streaming_to_bronze() -> None:
    spark = get_spark("KafkaToBronzeStreaming")

    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_kafka_df
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "json_data",
            F.from_json(F.col("message_value"), taxi_raw_schema)
        )
        .select(
            "message_key",
            "kafka_timestamp",
            "json_data.*"
        )
    )

    query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(BRONZE_STREAMING_PATH))
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming from Kafka topic '{KAFKA_TOPIC}' to: {BRONZE_STREAMING_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_to_bronze()