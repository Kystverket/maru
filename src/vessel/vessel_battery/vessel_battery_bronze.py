# Databricks notebook source
import os
import sys

SP_ID: str = os.getenv("SP_ID")
ENV: str = os.getenv("ENVIRONMENT")

sys.path.append(f"/Workspace/{SP_ID}/bundle_{ENV}/ais-flows/files/notebooks/")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import dlt
from transformations import change_data_types

# pylint: disable=wrong-import-order
from utilities.transformers.bronze import add_bi_metadata_columns

# COMMAND ----------

RAW_FILE_PATH = (
    f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/vessel_battery"
)
BATTERY_ELECTRIC_TABLE_NAME = "vessel_battery"

# COMMAND ----------

SCHEMA = """
        mmsi string,
        imo string,
        vessel_name string,
        route string,
        battery_installation_year int,
        degree_of_electrification string,
        battery_pack_kwh string
        """

# COMMAND ----------


@dlt.create_table(
    name=BATTERY_ELECTRIC_TABLE_NAME,
    comment="Battery vessels in bronze table. Enriched with metadata",
    table_properties={"medallion": "bronze"},
)
def raw_to_bronze():
    """
    Reads raw data from a cloud file in CSV format with semicolon delimiter, encoding UTF-8 and user defined schema and adds business intelligence fields.
    Returns a DataFrame in bronze format.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("rescuedDataColumn", "_rescue")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .option("skipRows", 1)
        .schema(SCHEMA)
        .load(RAW_FILE_PATH)
        .transform(change_data_types)
        .transform(add_bi_metadata_columns)
    )
