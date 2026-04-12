from dagster import Definitions, asset

from pipeline.bronze import run_bronze
from pipeline.silver import run_silver
from pipeline.gold import run_gold


@asset
def bronze_layer() -> str:
    run_bronze()
    return "Bronze layer created successfully."


@asset(deps=[bronze_layer])
def silver_layer() -> str:
    run_silver()
    return "Silver layer created successfully."


@asset(deps=[silver_layer])
def gold_layer() -> str:
    run_gold()
    return "Gold layer created successfully."


defs = Definitions(
    assets=[bronze_layer, silver_layer, gold_layer]
)