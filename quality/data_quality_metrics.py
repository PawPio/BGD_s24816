import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from utils.config import PROJECT_ROOT, SILVER_PATH
from utils.spark_session import get_spark


QUALITY_OUTPUT_PATH = PROJECT_ROOT / "data" / "quality" / "data_quality_metrics.json"


EXPECTED_PAYMENT_TYPES = [
    "Credit card",
    "Cash",
    "No charge",
    "Dispute",
    "Unknown",
    "Voided trip",
    "Other",
]


def calculate_percentage(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0

    return round((numerator / denominator) * 100, 2)


def build_metric(
    name: str,
    definition: str,
    current_value,
    expected_threshold: str,
    update_cadence: str = "Every pipeline run",
) -> dict:
    return {
        "metric_name": name,
        "definition": definition,
        "current_value": current_value,
        "expected_threshold": expected_threshold,
        "update_cadence": update_cadence,
    }


def calculate_quality_metrics() -> list[dict]:
    spark = get_spark("DataQualityMetrics")

    silver_df = spark.read.parquet(str(SILVER_PATH))

    total_rows = silver_df.count()

    pickup_datetime_not_null = (
        silver_df
        .filter(F.col("pickup_datetime").isNotNull())
        .count()
    )

    total_amount_valid = (
        silver_df
        .filter(F.col("total_amount") > 0)
        .count()
    )

    trip_distance_valid = (
        silver_df
        .filter(F.col("trip_distance") > 0)
        .count()
    )

    payment_type_consistent = (
        silver_df
        .filter(F.col("payment_type_label").isin(EXPECTED_PAYMENT_TYPES))
        .count()
    )

    latest_pickup_datetime = (
        silver_df
        .agg(F.max("pickup_datetime").alias("latest_pickup_datetime"))
        .collect()[0]["latest_pickup_datetime"]
    )

    metrics = [
        build_metric(
            name="Silver row count",
            definition="Number of records available in the Silver layer after cleaning.",
            current_value=total_rows,
            expected_threshold="> 0",
        ),
        build_metric(
            name="Pickup datetime completeness",
            definition="Percentage of Silver rows where pickup_datetime is not null.",
            current_value=f"{calculate_percentage(pickup_datetime_not_null, total_rows)}%",
            expected_threshold="> 99%",
        ),
        build_metric(
            name="Total amount validity",
            definition="Percentage of Silver rows where total_amount is greater than 0.",
            current_value=f"{calculate_percentage(total_amount_valid, total_rows)}%",
            expected_threshold="> 98%",
        ),
        build_metric(
            name="Trip distance validity",
            definition="Percentage of Silver rows where trip_distance is greater than 0.",
            current_value=f"{calculate_percentage(trip_distance_valid, total_rows)}%",
            expected_threshold="> 98%",
        ),
        build_metric(
            name="Payment type consistency",
            definition="Percentage of Silver rows where payment_type_label matches expected categories.",
            current_value=f"{calculate_percentage(payment_type_consistent, total_rows)}%",
            expected_threshold="> 99%",
        ),
        build_metric(
            name="Data freshness",
            definition="Latest pickup_datetime available in the Silver layer.",
            current_value=str(latest_pickup_datetime),
            expected_threshold="Within selected dataset period",
        ),
    ]

    return metrics


def save_quality_metrics(metrics: list[dict]) -> None:
    QUALITY_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    output = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_product": "NYC Yellow Taxi Analytical Gold Dataset",
        "metrics": metrics,
    }

    with QUALITY_OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)


def run_quality_checks() -> None:
    metrics = calculate_quality_metrics()
    save_quality_metrics(metrics)

    print(f"Data quality metrics saved to: {QUALITY_OUTPUT_PATH}")

    for metric in metrics:
        print(
            f"{metric['metric_name']}: "
            f"{metric['current_value']} "
            f"(threshold: {metric['expected_threshold']})"
        )


if __name__ == "__main__":
    run_quality_checks()