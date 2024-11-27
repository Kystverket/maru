# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SOURCE = "vessel_battery"
RAW_FILE_PATH = (
    f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/vessel/vessel_battery"
)

BRONZE_TABLE = f"bronze_{ENV}.maru.vessel_battery"
SILVER_TABLE = f"silver_{ENV}.maru.vessel_battery"
GOLD_TABLE = f"gold_{ENV}.maru.vessel_battery"
TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI = (
    f"gold_{ENV}.shipdata.combined_imputated_imo_mmsi"
)

# COMMAND ----------

# MAGIC %md #Bronze

# COMMAND ----------

SCHEMA = """
        mmsi long,
        imo long,
        vessel_name string,
        route string,
        degree_of_electrification string,
        battery_pack_kwh string,
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
    "degree_of_electrification",
    F.regexp_replace("degree_of_electrification", ",", ".").cast(DoubleType()),
).withColumn(
    "battery_pack_kwh",
    F.col("battery_pack_kwh").cast(IntegerType()),
)

# Handle null values
df = df.replace({"null": None}, subset=["route", "vessel_name"]).fillna(0, subset="imo")

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

# Read silver table battery_electric
df_battery_electric = spark.table(SILVER_TABLE)

# Add vessel_id
df_battery_electric_vessel_id = (
    df_battery_electric.join(
        df_shipdata_combined_imo_mmsi, on=["imo", "mmsi"], how="inner"
    )
    .select(
        df_shipdata_combined_imo_mmsi["vessel_id"],
        df_battery_electric["mmsi"],
        df_battery_electric["imo"],
        df_battery_electric["degree_of_electrification"],
        df_battery_electric["battery_pack_kwh"],
        df_battery_electric["start_date"],
        df_battery_electric["end_date"],
    )
    .orderBy("vessel_id", "start_date")
    .dropDuplicates(subset=["vessel_id", "start_date"])
)


# Write table
df_battery_electric_vessel_id.write.mode("overwrite").saveAsTable(GOLD_TABLE)
