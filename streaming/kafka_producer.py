import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "taxi-trips-raw"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
    )


def get_csv_files(raw_data_dir: Path = RAW_DATA_DIR) -> list[Path]:
    if not raw_data_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_data_dir}")

    csv_files = sorted(raw_data_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in directory: {raw_data_dir}")

    return csv_files


def stream_file_to_kafka(
    producer: KafkaProducer,
    file_path: Path,
    topic: str,
    max_rows_per_file: int | None = 10000,
    sleep_seconds: float = 0.01,
) -> int:
    sent_records = 0

    with file_path.open(mode="r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        for row_number, row in enumerate(reader, start=1):
            if max_rows_per_file is not None and row_number > max_rows_per_file:
                break

            message_key = f"{file_path.stem}-{row_number}"

            producer.send(
                topic=topic,
                key=message_key,
                value=row,
            )

            sent_records += 1

            if sent_records % 1000 == 0:
                print(f"Sent {sent_records} records from {file_path.name} to Kafka topic: {topic}")

            time.sleep(sleep_seconds)

    return sent_records


def stream_csv_directory_to_kafka(
    raw_data_dir: Path = RAW_DATA_DIR,
    topic: str = KAFKA_TOPIC,
    max_rows_per_file: int | None = 10000,
    sleep_seconds: float = 0.01,
) -> None:
    csv_files = get_csv_files(raw_data_dir)
    producer = create_producer()

    total_sent_records = 0

    try:
        for file_path in csv_files:
            print(f"Processing source file: {file_path.name}")

            sent_records = stream_file_to_kafka(
                producer=producer,
                file_path=file_path,
                topic=topic,
                max_rows_per_file=max_rows_per_file,
                sleep_seconds=sleep_seconds,
            )

            total_sent_records += sent_records
            print(f"Finished file: {file_path.name}. Records sent: {sent_records}")

        producer.flush()

    finally:
        producer.close()

    print(f"Finished streaming CSV directory. Total records sent: {total_sent_records}")


if __name__ == "__main__":
    stream_csv_directory_to_kafka()