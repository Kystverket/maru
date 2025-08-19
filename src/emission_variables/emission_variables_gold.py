# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append(os.path.abspath("../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SILVER_TABLE = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells"
GOLD_TABLE = f"gold_{ENV}.maru.emission_variables"

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS gold_{ENV}.maru.emission_variables (
  feature STRING COMMENT 'Feature description',
  variable STRING COMMENT 'Variable description',
  start_date DATE COMMENT 'Start date of the record',
  end_date DATE COMMENT 'End date of the record',
  reference STRING COMMENT 'Reference information',
  value DOUBLE COMMENT 'Value of the variable',
  unit STRING COMMENT 'Unit of the value',
  comment STRING COMMENT 'Additional comments',
  bi_processing_time TIMESTAMP COMMENT 'Last processing time'
)
COMMENT 'Emission variables table'
"""
)

# COMMAND ----------

# Read silver Dataverse table
df_silver_source = spark.table(SILVER_TABLE).where(
    "cr944_datasource = 'emission_variables'"
)

df_gold = (
    df_silver_source.select(
        F.col("cr944_feature").alias("feature"),
        F.col("cr944_variable").alias("variable"),
        F.col("cr944_startdate").alias("start_date"),
        F.col("cr944_enddate").alias("end_date"),
        F.col("cr944_reference").alias("reference"),
        F.col("cr944_value").alias("value"),
        F.col("cr944_unit").alias("unit"),
        F.col("cr944_comment").alias("comment"),
    )
    .orderBy("variable", "start_date")
    .transform(add_processing_timestamp)
)

# Write table
df_gold.write.mode("overwrite").saveAsTable(GOLD_TABLE)
