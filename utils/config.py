from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2016-02.csv"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "yellow_taxi_tripdata"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "yellow_taxi_tripdata"
GOLD_REVENUE_PER_DAY_PATH = PROJECT_ROOT / "data" / "gold" / "revenue_per_day"
GOLD_TRIPS_PER_HOUR_PATH = PROJECT_ROOT / "data" / "gold" / "trips_per_hour"
GOLD_PAYMENT_TYPE_SUMMARY_PATH = PROJECT_ROOT / "data" / "gold" / "payment_type_summary"

APP_NAME = "TaxiMedallionPipeline"