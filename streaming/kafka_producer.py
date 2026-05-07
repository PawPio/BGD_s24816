import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips-raw")

DEFAULT_MAX_ROWS_PER_FILE = int(os.getenv("MAX_ROWS_PER_FILE", "10000"))
DEFAULT_SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.01"))


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
    file_path: Path,
    topic: str = KAFKA_TOPIC,
    max_rows_per_file: int | None = DEFAULT_MAX_ROWS_PER_FILE,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> int:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    producer = create_producer()
    sent_records = 0

    try:
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
                    print(
                        f"Sent {sent_records} records from {file_path.name} "
                        f"to Kafka topic: {topic}"
                    )

                time.sleep(sleep_seconds)

        producer.flush()

    finally:
        producer.close()

    print(f"Finished file: {file_path.name}. Records sent: {sent_records}")
    return sent_records


def stream_csv_directory_to_kafka(
    raw_data_dir: Path = RAW_DATA_DIR,
    topic: str = KAFKA_TOPIC,
    max_rows_per_file: int | None = DEFAULT_MAX_ROWS_PER_FILE,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> None:
    csv_files = get_csv_files(raw_data_dir)

    total_sent_records = 0

    for file_path in csv_files:
        print(f"Processing source file: {file_path.name}")

        sent_records = stream_file_to_kafka(
            file_path=file_path,
            topic=topic,
            max_rows_per_file=max_rows_per_file,
            sleep_seconds=sleep_seconds,
        )

        total_sent_records += sent_records

    print(f"Finished streaming CSV directory. Total records sent: {total_sent_records}")


if __name__ == "__main__":
    stream_csv_directory_to_kafka()