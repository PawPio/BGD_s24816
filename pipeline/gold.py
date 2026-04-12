from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.config import (
    SILVER_PATH,
    GOLD_REVENUE_PER_DAY_PATH,
    GOLD_TRIPS_PER_HOUR_PATH,
    GOLD_PAYMENT_TYPE_SUMMARY_PATH,
)
from utils.spark_session import get_spark


def extract_silver() -> DataFrame:
    spark = get_spark("GoldLayer")
    return spark.read.parquet(str(SILVER_PATH))


def build_revenue_per_day(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance")
        )
        .orderBy("pickup_date")
    )


def build_trips_per_hour(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount")
        )
        .orderBy("pickup_hour")
    )


def build_payment_type_summary(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("payment_type_label")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount")
        )
        .orderBy(F.desc("trip_count"))
    )


def load_gold(
    revenue_per_day_df: DataFrame,
    trips_per_hour_df: DataFrame,
    payment_type_summary_df: DataFrame
) -> None:
    revenue_per_day_df.write.mode("overwrite").parquet(str(GOLD_REVENUE_PER_DAY_PATH))
    trips_per_hour_df.write.mode("overwrite").parquet(str(GOLD_TRIPS_PER_HOUR_PATH))
    payment_type_summary_df.write.mode("overwrite").parquet(str(GOLD_PAYMENT_TYPE_SUMMARY_PATH))


def run_gold() -> None:
    silver_df = extract_silver()

    revenue_per_day_df = build_revenue_per_day(silver_df)
    trips_per_hour_df = build_trips_per_hour(silver_df)
    payment_type_summary_df = build_payment_type_summary(silver_df)

    load_gold(revenue_per_day_df, trips_per_hour_df, payment_type_summary_df)

    print(f"Gold revenue_per_day saved to: {GOLD_REVENUE_PER_DAY_PATH}")
    print(f"Gold trips_per_hour saved to: {GOLD_TRIPS_PER_HOUR_PATH}")
    print(f"Gold payment_type_summary saved to: {GOLD_PAYMENT_TYPE_SUMMARY_PATH}")


if __name__ == "__main__":
    run_gold()