# Databricks notebook source
import os
import sys

# COMMAND ----------

SP_ID: str = os.getenv("SP_ID")
ENV: str = os.getenv("ENVIRONMENT")

sys.path.append(f"/Workspace/{SP_ID}/bundle_{ENV}/ais-flows/files/notebooks/")

# COMMAND ----------

import dlt
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# pylint: disable=wrong-import-order
from utilities.transformers.bronze import add_bi_metadata_columns

# COMMAND ----------

RAW_FILE_PATH = f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/vessel_aux_boiler_kw"
SOURCE_TABLE_NAME = "vessel_aux_boiler_kw"

# COMMAND ----------

SCHEMA = StructType(
    [
        StructField("vessel_type", StringType(), True),
        StructField("vessel_type_id", IntegerType(), True),
        StructField("low_limit", IntegerType(), True),
        StructField("high_limit", IntegerType(), True),
        StructField("size_bin", IntegerType(), True),
        StructField("unit", StringType(), True),
        StructField("boiler_berth", IntegerType(), True),
        StructField("boiler_anchor", IntegerType(), True),
        StructField("boiler_maneuver", IntegerType(), True),
        StructField("boiler_cruise", IntegerType(), True),
        StructField("aux_berth", IntegerType(), True),
        StructField("aux_anchor", IntegerType(), True),
        StructField("aux_maneuver", IntegerType(), True),
        StructField("aux_cruise", IntegerType(), True),
    ]
)

# COMMAND ----------


@dlt.create_table(
    name=SOURCE_TABLE_NAME,
    comment="Vessel aux and boiler in bronze table. Enriched with metadata",
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
