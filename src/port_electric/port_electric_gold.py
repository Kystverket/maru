# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utilities.transformers.h3 import (
    H3_HEX_KRING_SHORE_POWER,
    H3_RESOLUTION_SHORE_POWER,
    set_h3_hex_from_lat_lon,
    set_h3_kring_with_distance,
)

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_TABLE_FULL_PATH = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells_v1"
TARGET_TABLE_FULL_PATH = f"gold_{ENV}.area.shore_power"

# COMMAND ----------

# Read silver Dataverse table
df_silver_source = spark.table(SOURCE_TABLE_FULL_PATH).where(
    "cr944_datasource = 'port_electric'"
)

df_gold = df_silver_source.select(
    F.col("cr944_rowid").alias("id"),
    F.col("cr944_anlegg").alias("name"),
    F.col("cr944_latitude").alias("lat"),
    F.col("cr944_longitude").alias("lon"),
    F.col("cr944_etableringsdato").alias("date_established"),
    F.col("cr944_bruksgrad").alias("usage_percentage"),
    F.col("bi_ingested_timestamp").alias("bi_ingested_timestamp"),
)

# COMMAND ----------

df_gold = set_h3_hex_from_lat_lon(df_gold, H3_RESOLUTION_SHORE_POWER)
df_gold = set_h3_kring_with_distance(
    df_gold, H3_RESOLUTION_SHORE_POWER, H3_HEX_KRING_SHORE_POWER
)

df_gold = df_gold.dropDuplicates(
    [
        "name",
        f"hex_kring_{H3_RESOLUTION_SHORE_POWER}_{H3_HEX_KRING_SHORE_POWER}",
    ]
)

df_gold = add_processing_timestamp(df_gold)

# COMMAND ----------

df_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    TARGET_TABLE_FULL_PATH
)
