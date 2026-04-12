from pathlib import Path

from pyspark.sql import DataFrame

from utils.config import RAW_DATA_PATH, BRONZE_PATH
from utils.spark_session import get_spark


def extract_raw_csv() -> DataFrame:

    spark = get_spark("BronzeLayer")

    if not Path(RAW_DATA_PATH).exists():
        raise FileNotFoundError(f"Raw input file not found: {RAW_DATA_PATH}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(RAW_DATA_PATH))
    )

    return df


def load_bronze(df: DataFrame) -> None:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(BRONZE_PATH))
    )


def run_bronze() -> None:
    df = extract_raw_csv()
    load_bronze(df)
    print(f"Bronze layer saved to: {BRONZE_PATH}")


if __name__ == "__main__":
    run_bronze()