# NYC Yellow Taxi – ELT in Medallion Architecture

## Project overview

This project presents an ELT pipeline implemented using PySpark and the medallion architecture approach.

The source data comes from the NYC Yellow Taxi Trip dataset (February 2016).
The project demonstrates how raw data can be ingested from a CSV file, processed in multiple layers, and transformed into analytical datasets.

The pipeline uses:

**PySpark** as a scalable processing engine

**Parquet files** as storage for all layers

a **layered medallion architecture** (Bronze, Silver, Gold)

The design intentionally avoids SQLite in favor of a file-based analytical approach better aligned with Big Data processing.

---

## Analytical goal

The goal of the project is to analyze taxi trip data and derive insights such as:

daily revenue trends

trip distribution across hours

payment method usage and revenue contribution

---

## Medallion architecture

The project follows the medallion architecture:

### Bronze
Raw data loaded directly from CSV without enforcing schema.
This layer preserves the original structure of the data.

### Silver
Cleaned and standardized data:

data type casting

timestamp parsing

filtering invalid or missing values

deriving additional features (e.g. trip duration, pickup hour)

### Gold
Aggregated business-level data ready for analysis:

revenue per day

trips per hour

payment type summary

All transformations are implemented in PySpark following the ELT approach.

---

## Data storage

All layers are stored as **Parquet files**:

data/bronze/

data/silver/

data/gold/

This allows efficient processing and compatibility with Big Data tools.

---

## Run

Insert file into:

```text
data/raw/yellow_tripdata_2016-02.csv
```

Run pipeline:

```bash
python main.py
```

---

## Output

The pipeline generates the following datasets:

```text
data/gold/revenue_per_day/
data/gold/trips_per_hour/
data/gold/payment_type_summary/
```

These datasets can be used for further analysis or visualization.

---

## Streaming extenstion
The project was extended with a streaming ingestion option that moves source data into the Bronze layer using a queue-based architecture.

The streaming extension uses:

Apache Kafka as the queue system

Python Kafka Producer to publish taxi trip records from CSV files

Spark Structured Streaming to consume records from Kafka

Parquet files as the Bronze Streaming storage format

## Streaming data storage
Streaming output is written into
```text
data/bronze_streaming/taxi_trips_raw/
```
Spark also creates technical metadata and checkpoint directories
```text
data/bronze_streaming/
data/checkpoints/
```

## Run streaming

1. Start Kafka 
```text
docker compose up -d
```
2. Create Kafka Topic
```text
docker exec -it taxi-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic taxi-trips-raw \
  --partitions 1 \
  --replication-factor 1
```
3. Start Spark streaming
```text
./scripts/run_streaming_consumer.sh
```
4. Start folder watcher
```text
python -m streaming.folder_watcher
```

## Local Kafka connector
The Spark Kafka connector is provided through local JAR files because Maven dependency resolution was unstable in the local WSL environment.

The following JAR files are expected locally in the `jars/` directory:
```text
spark-sql-kafka-0-10_2.12-3.5.0.jar
spark-token-provider-kafka-0-10_2.12-3.5.0.jar
kafka-clients-3.4.1.jar
commons-pool2-2.11.1.jar
```
