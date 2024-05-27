# Databricks notebook source
import os

import dlt

# COMMAND ----------
ENV: str = os.getenv("ENVIRONMENT")
BRONZE_TABLE = f"bronze_{ENV}.maru.vessel_battery"
SILVER_TABLE = "vessel_battery"
# COMMAND ----------


@dlt.view
def vessel_battery_bronze_table():
    """
    Reads bronze table
    """
    return spark.readStream.table(BRONZE_TABLE)


dlt.create_streaming_table(
    name=SILVER_TABLE,
    table_properties={"medallion": "silver"},
)
dlt.apply_changes(
    target=SILVER_TABLE,
    source="vessel_battery_bronze_table",
    keys=["mmsi"],
    sequence_by="bi_ingested_timestamp",
)
