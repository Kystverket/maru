# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------

sys.path.append(os.path.abspath("../../../"))
sys.path.append(os.path.abspath("../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utils.config import MUNICIPALITY_CURRENT_YEAR

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SOURCE = "vessel_type_port_electric"
RAW_FILE_PATH = (
    f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/{SOURCE}"
)

BRONZE_TABLE = f"bronze_{ENV}.maru.{SOURCE}"
SILVER_TABLE = f"silver_{ENV}.maru.{SOURCE}"
GOLD_TABLE = f"gold_{ENV}.maru.vessel_type_port_electric"

TABLE_NAME_VESSEL_TYPE = f"silver_{ENV}.maru.vessel_type"
TABLE_NAME_MUNICIPALITY = f"gold_{ENV}.area.municipality_historical"

# COMMAND ----------

# MAGIC %md #Bronze

# COMMAND ----------

SCHEMA = """
        vessel_type string,
        municipality_name string,
        usage_percentage string,
        start_date date,
        end_date date
        """

# COMMAND ----------

df = spark.read.csv(
    RAW_FILE_PATH, header=True, sep=";", schema=SCHEMA, dateFormat="dd.MM.yyyy"
)

# Add bi metadata
df = df.withColumn("bi_source", F.lit(SOURCE))
df = df.withColumn("bi_filepath", F.lit(RAW_FILE_PATH))
df = add_processing_timestamp(df, "bi_ingested_timestamp")

# Write table
df.write.mode("overwrite").saveAsTable(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md #Silver

# COMMAND ----------

df = spark.table(BRONZE_TABLE)

# Data types
df = df.withColumn(
    "usage_percentage",
    F.regexp_replace("usage_percentage", ",", ".").cast(DoubleType()),
)

# Handle null values
df = df.replace({"null": None}, subset=["municipality_name", "municipality_name"])

# Remove duplicates
df = df.dropDuplicates(subset=["vessel_type", "municipality_name", "start_date"])

# Add bi metadata
df = add_processing_timestamp(df)

# Write table
df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md #Gold

# COMMAND ----------

#  Read tables
df_municipality_current = spark.table(TABLE_NAME_MUNICIPALITY).where(
    f"year = {MUNICIPALITY_CURRENT_YEAR}"
)

df_vessel_type = spark.table(TABLE_NAME_VESSEL_TYPE).select("vessel_type").distinct()

df_vessel_type_port_electric_in = spark.table(SILVER_TABLE)

# COMMAND ----------

# Add municipality_no
df_vessel_type_port_electric = (
    df_vessel_type_port_electric_in.where("start_date IS NOT NULL")
    .join(df_municipality_current, "municipality_name", "inner")
    .join(df_vessel_type, "vessel_type", "inner")
    .select(
        df_vessel_type_port_electric_in["vessel_type"],
        df_municipality_current["municipality_no"],
        df_vessel_type_port_electric_in["municipality_name"],
        df_vessel_type_port_electric_in["usage_percentage"],
        df_vessel_type_port_electric_in["start_date"],
        df_vessel_type_port_electric_in["end_date"],
    )
    .dropDuplicates(subset=["vessel_type", "municipality_no", "start_date"])
)

# Write table
df_vessel_type_port_electric.write.mode("overwrite").saveAsTable(GOLD_TABLE)

# COMMAND ----------

# MAGIC %md #Data without match

# COMMAND ----------

# MAGIC %md ##Vessel type

# COMMAND ----------

display(
    df_vessel_type_port_electric_in.join(df_vessel_type, "vessel_type", "leftanti")
    .select("vessel_type")
    .distinct()
)

# COMMAND ----------

# MAGIC %md ## Municipality

# COMMAND ----------

display(
    df_vessel_type_port_electric_in.join(
        df_municipality_current, "municipality_name", "leftanti"
    )
    .select("municipality_name")
    .distinct()
)
