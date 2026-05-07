import json
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from streaming.kafka_producer import stream_file_to_kafka


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PRODUCER_STATE_DIR = PROJECT_ROOT / "data" / "producer_state"
PROCESSED_FILES_PATH = PRODUCER_STATE_DIR / "processed_files.json"


def load_processed_files() -> set[str]:
    if not PROCESSED_FILES_PATH.exists():
        return set()

    with PROCESSED_FILES_PATH.open("r", encoding="utf-8") as file:
        return set(json.load(file))


def save_processed_files(processed_files: set[str]) -> None:
    PRODUCER_STATE_DIR.mkdir(parents=True, exist_ok=True)

    with PROCESSED_FILES_PATH.open("w", encoding="utf-8") as file:
        json.dump(sorted(processed_files), file, indent=2)


def build_file_fingerprint(file_path: Path) -> str:
    stat = file_path.stat()
    return f"{file_path.name}|{stat.st_size}|{int(stat.st_mtime)}"


def wait_until_file_is_stable(
    file_path: Path,
    checks: int = 3,
    sleep_seconds: float = 1.0,
) -> None:
    previous_size = -1
    stable_checks = 0

    while stable_checks < checks:
        current_size = file_path.stat().st_size

        if current_size == previous_size:
            stable_checks += 1
        else:
            stable_checks = 0
            previous_size = current_size

        time.sleep(sleep_seconds)


def process_csv_file(file_path: Path, processed_files: set[str]) -> None:
    if file_path.suffix.lower() != ".csv":
        return

    wait_until_file_is_stable(file_path)

    fingerprint = build_file_fingerprint(file_path)

    if fingerprint in processed_files:
        print(f"Skipping already processed file: {file_path.name}")
        return

    print(f"Detected new source file: {file_path.name}")
    print(f"Starting Kafka ingestion for: {file_path.name}")

    stream_file_to_kafka(file_path=file_path)

    processed_files.add(fingerprint)
    save_processed_files(processed_files)

    print(f"File marked as processed: {file_path.name}")


class CsvFileCreatedHandler(FileSystemEventHandler):
    def __init__(self, processed_files: set[str]) -> None:
        self.processed_files = processed_files

    def on_created(self, event) -> None:
        if event.is_directory:
            return

        file_path = Path(event.src_path)

        try:
            process_csv_file(file_path, self.processed_files)
        except Exception as error:
            print(f"Failed to process file {file_path.name}: {error}")


def process_existing_files(processed_files: set[str]) -> None:
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))

    for file_path in csv_files:
        process_csv_file(file_path, processed_files)


def watch_raw_data_folder(process_existing_on_startup: bool = True) -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PRODUCER_STATE_DIR.mkdir(parents=True, exist_ok=True)

    processed_files = load_processed_files()

    if process_existing_on_startup:
        print("Checking existing CSV files before starting watcher...")
        process_existing_files(processed_files)

    event_handler = CsvFileCreatedHandler(processed_files)
    observer = Observer()
    observer.schedule(event_handler, str(RAW_DATA_DIR), recursive=False)

    observer.start()

    print(f"Watching folder for new CSV files: {RAW_DATA_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Folder watcher stopped.")

    observer.join()


if __name__ == "__main__":
    watch_raw_data_folder()