# Databricks notebook source
import os
import sys

SP_ID: str = os.getenv("SP_ID")
ENV: str = os.getenv("ENVIRONMENT")

sys.path.append(f"/Workspace/{SP_ID}/bundle_{ENV}/ais-flows/files/notebooks/")

# COMMAND ----------

import dlt
from utilities.transformers.bronze import add_bi_metadata_columns

# COMMAND ----------

RAW_FILE_PATH = (
    f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/vessel_type"
)
TABLE_NAME = "vessel_type"

# COMMAND ----------

SCHEMA = """
        statcode5 string,
        vessel_type string
        """

# COMMAND ----------


@dlt.create_table(
    name=TABLE_NAME,
    comment="MarU vessel type in bronze table. Enriched with metadata",
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
        .transform(add_bi_metadata_columns)
    )
