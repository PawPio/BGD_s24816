from pyspark.sql import SparkSession

from utils.config import APP_NAME


def get_spark(app_name: str = APP_NAME) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark