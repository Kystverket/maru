# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SILVER_TABLE = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells"
GOLD_TABLE = f"gold_{ENV}.maru.vessel_type"

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
  statcode5 STRING COMMENT 'Statcode5 code',
  vessel_type STRING COMMENT 'Vessel type according to Maru classification',
  bi_processing_time TIMESTAMP COMMENT 'Last processing time'
)
COMMENT 'MarU vessel types'
"""
)

# COMMAND ----------

df_silver_source = spark.table(SILVER_TABLE).where(
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
df_gold.write.mode("overwrite").saveAsTable(GOLD_TABLE)
