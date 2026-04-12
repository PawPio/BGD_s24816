from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.config import BRONZE_PATH, SILVER_PATH
from utils.spark_session import get_spark


def extract_bronze() -> DataFrame:
    spark = get_spark("SilverLayer")
    return spark.read.parquet(str(BRONZE_PATH))


def transform_silver(df: DataFrame) -> DataFrame:

    typed_df = (
        df.select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            F.to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
            F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("pickup_longitude").cast("double").alias("pickup_longitude"),
            F.col("pickup_latitude").cast("double").alias("pickup_latitude"),
            F.col("RatecodeID").cast("int").alias("rate_code_id"),
            F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
            F.col("dropoff_longitude").cast("double").alias("dropoff_longitude"),
            F.col("dropoff_latitude").cast("double").alias("dropoff_latitude"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("extra").cast("double").alias("extra"),
            F.col("mta_tax").cast("double").alias("mta_tax"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("tolls_amount").cast("double").alias("tolls_amount"),
            F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            F.col("total_amount").cast("double").alias("total_amount"),
        )
    )

    enriched_df = (
        typed_df
        .withColumn(
            "trip_duration_minutes",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.date_format("pickup_datetime", "E"))
        .withColumn(
            "payment_type_label",
            F.when(F.col("payment_type") == 1, "Credit card")
             .when(F.col("payment_type") == 2, "Cash")
             .when(F.col("payment_type") == 3, "No charge")
             .when(F.col("payment_type") == 4, "Dispute")
             .when(F.col("payment_type") == 5, "Unknown")
             .when(F.col("payment_type") == 6, "Voided trip")
             .otherwise("Other")
        )
    )

    cleaned_df = (
        enriched_df
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("passenger_count").isNotNull())
        .filter(F.col("trip_distance").isNotNull())
        .filter(F.col("fare_amount").isNotNull())
        .filter(F.col("total_amount").isNotNull())
        .filter(F.col("trip_duration_minutes").isNotNull())
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") <= 180)
        .filter(F.col("trip_distance") <= 100)
    )

    return cleaned_df.dropDuplicates()

def load_silver(df: DataFrame) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(str(SILVER_PATH))
    )


def run_silver() -> None:
    bronze_df = extract_bronze()
    silver_df = transform_silver(bronze_df)
    load_silver(silver_df)
    print(f"Silver layer saved to: {SILVER_PATH}")


if __name__ == "__main__":
    run_silver()