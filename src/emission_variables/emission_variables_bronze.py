# Databricks notebook source
import sys

sys.path.append("/Workspace/")

# COMMAND ----------

import dlt
from utilities.transformers.bronze import add_bi_metadata_columns

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")
RAW_FILE_PATH = (
    f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/emission_variables"
)
BRONZE_TABLE_NAME = "emission_variables"
# COMMAND ----------


@dlt.create_table(
    name=BRONZE_TABLE_NAME,
    comment="Manual input for MarU emission variables in bronze table. Enriched with metadata",
    table_properties={"medallion": "bronze"},
)
def raw_to_bronze():
    """
    Reads raw data from a cloud file in CSV format with semicolon delimiter, encoding UTF-8, infer schema and adds business intelligence fields.
    Returns a DataFrame in bronze format.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("rescuedDataColumn", "_rescue")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .option("multiline", "true")
        .load(RAW_FILE_PATH)
        .transform(add_bi_metadata_columns)
    )
