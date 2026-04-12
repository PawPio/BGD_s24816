from pipeline.bronze import run_bronze
from pipeline.silver import run_silver
from pipeline.gold import run_gold


def main() -> None:
    run_bronze()
    run_silver()
    run_gold()
    print("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()