from pipeline.bronze import run_bronze
from pipeline.silver import run_silver
from pipeline.gold import run_gold
from quality.data_quality_metrics import run_quality_checks

def main() -> None:
    run_bronze()
    run_silver()
    run_gold()
    # run_quality_checks()
    print("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()