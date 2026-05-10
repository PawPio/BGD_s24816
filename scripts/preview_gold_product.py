from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DATASETS = {
    "revenue_per_day": PROJECT_ROOT / "data" / "gold" / "revenue_per_day",
    "trips_per_hour": PROJECT_ROOT / "data" / "gold" / "trips_per_hour",
    "payment_type_summary": PROJECT_ROOT / "data" / "gold" / "payment_type_summary",
}


def find_parquet_files(dataset_path: Path) -> list[Path]:
    return sorted(
        file_path
        for file_path in dataset_path.rglob("*.parquet")
        if file_path.is_file()
    )


def read_spark_parquet_folder(dataset_path: Path) -> pd.DataFrame:
    parquet_files = find_parquet_files(dataset_path)

    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found in: {dataset_path}")

    dataframes = [pd.read_parquet(file_path) for file_path in parquet_files]
    return pd.concat(dataframes, ignore_index=True)


def preview_dataset(dataset_name: str, dataset_path: Path) -> None:
    if not dataset_path.exists():
        print(f"\nDataset not found: {dataset_name}")
        print(f"Expected path: {dataset_path}")
        return

    print("\n" + "=" * 80)
    print(f"Dataset: {dataset_name}")
    print(f"Path: {dataset_path}")
    print("=" * 80)

    try:
        df = read_spark_parquet_folder(dataset_path)
    except Exception as error:
        print(f"Could not read dataset: {dataset_name}")
        print(f"Reason: {error}")
        return

    print("\nColumns:")
    print(df.dtypes)

    print("\nPreview:")
    print(df.head(10).to_string(index=False))

    print(f"\nRow count: {len(df)}")


def main() -> None:
    for dataset_name, dataset_path in GOLD_DATASETS.items():
        preview_dataset(dataset_name, dataset_path)


if __name__ == "__main__":
    main()