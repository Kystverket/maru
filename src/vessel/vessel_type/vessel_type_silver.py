# Databricks notebook source
import os

import dlt

# COMMAND ----------
ENV: str = os.getenv("ENVIRONMENT")
BRONZE_TABLE = f"bronze_{ENV}.maru.vessel_type"
SILVER_TABLE = "vessel_type"
# COMMAND ----------


@dlt.view
def v_bronze():
    """
    Reads bronze table
    """
    return spark.readStream.table(BRONZE_TABLE)


dlt.create_streaming_table(
    name=SILVER_TABLE,
    comment="MarU vessel type in silver table. Enriched with metadata. Unique key is Statcode 5",
    table_properties={"medallion": "silver"},
)
dlt.apply_changes(
    target=SILVER_TABLE,
    source="v_bronze",
    keys=["statcode5"],
    sequence_by="bi_ingested_timestamp",
    stored_as_scd_type=1,
)
