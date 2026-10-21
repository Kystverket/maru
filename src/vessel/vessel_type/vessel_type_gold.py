# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append(os.path.abspath("../../../../"))  # notebooks/
sys.path.append(os.path.abspath("../../"))  # maru_v1_10/

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utils.config import VERSION

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_TABLE_FULL_PATH = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells_v1"
TARGET_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_type_{VERSION}"

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE_FULL_PATH} (
  statcode5 STRING COMMENT 'Statcode5 code',
  vessel_type STRING COMMENT 'Vessel type according to Maru classification',
  bi_processing_time TIMESTAMP COMMENT 'Last processing time'
)
COMMENT 'MarU vessel types'
"""
)

# COMMAND ----------

df_silver_source = spark.table(SOURCE_TABLE_FULL_PATH).where(
    "cr944_datasource = 'vessel_type_statcode5'"
)

df_gold = (
    df_silver_source.select(
        F.col("cr944_statcode5").alias("statcode5"),
        F.col("cr944_vesseltype").alias("vessel_type"),
    )
    .dropDuplicates(subset=["statcode5"])
    .transform(add_processing_timestamp)
    .orderBy("statcode5")
)

# COMMAND ----------

# Write table
df_gold.write.mode("overwrite").saveAsTable(TARGET_TABLE_FULL_PATH)
