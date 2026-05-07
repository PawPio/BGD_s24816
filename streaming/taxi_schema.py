from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


taxi_raw_schema = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("pickup_longitude", StringType(), True),
    StructField("pickup_latitude", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", StringType(), True),
    StructField("dropoff_latitude", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
])