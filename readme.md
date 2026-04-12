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