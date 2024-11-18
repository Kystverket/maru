# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utils.config import MUNICIPALITY_CURRENT_YEAR

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SOURCE = "vessel_port_electric"
RAW_FILE_PATH = f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/vessel_port_electric"

BRONZE_TABLE = f"bronze_{ENV}.maru.{SOURCE}"
SILVER_TABLE = f"silver_{ENV}.maru.{SOURCE}"
GOLD_TABLE = f"gold_{ENV}.maru.vessel_port_electric"

TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI = (
    f"gold_{ENV}.shipdata.combined_imputated_imo_mmsi"
)
TABLE_NAME_MUNICIPALITY = f"gold_{ENV}.area.municipality_historical"

# COMMAND ----------

# MAGIC %md #Bronze

# COMMAND ----------

SCHEMA = """
        ship_name string,
        imo long,
        mmsi long,
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
df = df.replace({"null": None}, subset=["ship_name", "municipality_name"]).fillna(
    0, subset=["imo", "mmsi"]
)

# Remove duplicates
df = df.dropDuplicates(subset=["mmsi", "imo", "start_date"])

# Add bi metadata
df = add_processing_timestamp(df)

# Write table
df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md #Gold

# COMMAND ----------

#  Read silver table shipdata combined
df_shipdata_combined_imo_mmsi = spark.table(
    TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI
).where("row_number_mmsi_imo = 1")

df_municipality_current = spark.table(TABLE_NAME_MUNICIPALITY).where(
    f"year = {MUNICIPALITY_CURRENT_YEAR}"
)

df_vessel_port_electric_in = spark.table(SILVER_TABLE)

# COMMAND ----------

# Add vessel_id and municipality_no
df_vessel_port_electric_vessel_id = (
    df_vessel_port_electric_in.join(
        df_shipdata_combined_imo_mmsi, on=["imo", "mmsi"], how="inner"
    )
    .join(df_municipality_current, "municipality_name", "inner")
    .select(
        df_shipdata_combined_imo_mmsi["vessel_id"],
        df_vessel_port_electric_in["mmsi"],
        df_vessel_port_electric_in["imo"],
        df_municipality_current["municipality_no"],
        df_vessel_port_electric_in["municipality_name"],
        df_vessel_port_electric_in["usage_percentage"],
        df_vessel_port_electric_in["start_date"],
        df_vessel_port_electric_in["end_date"],
    )
    .orderBy("vessel_id", "start_date")
    .dropDuplicates(subset=["vessel_id", "municipality_no", "start_date"])
)

# Write table
df_vessel_port_electric_vessel_id.write.mode("overwrite").saveAsTable(GOLD_TABLE)
