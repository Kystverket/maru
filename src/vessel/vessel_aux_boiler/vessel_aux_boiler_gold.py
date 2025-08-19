# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath("../../../"))
ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

TABLE_NAME_SOURCE = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells"
TABLE_NAME_DESTINATION = f"gold_{ENV}.maru.vessel_aux_boiler_kw"

# COMMAND ----------

MAX_VALUE = 9999999
UNKNOWN_UNIT_VALUE_LOW_LIMIT = -99
UNKNOWN_UNIT_VALUE_HIGH_LIMIT = 0

# COMMAND ----------

# MAGIC %md #Setup

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS gold_{ENV}.maru.vessel_aux_boiler_kw (
  vessel_type STRING COMMENT 'Type of vessel',
  unit STRING COMMENT 'Unit of measurement',
  low_limit INT COMMENT 'Lower limit of the size bin',
  high_limit INT COMMENT 'Upper limit of the size bin',
  size_bin INT COMMENT 'Size bin category',
  aux_berth_kw INT COMMENT 'Auxiliary power at berth in kW',
  aux_anchor_kw INT COMMENT 'Auxiliary power at anchor in kW',
  aux_maneuver_kw INT COMMENT 'Auxiliary power during maneuvering in kW',
  aux_cruise_kw INT COMMENT 'Auxiliary power during cruising in kW',
  boiler_berth_kw INT COMMENT 'Boiler power at berth in kW',
  boiler_anchor_kw INT COMMENT 'Boiler power at anchor in kW',
  boiler_maneuver_kw INT COMMENT 'Boiler power during maneuvering in kW',
  boiler_cruise_kw INT COMMENT 'Boiler power during cruising in kW',
  bi_processing_time TIMESTAMP COMMENT 'Last processing timestamp'
)
COMMENT 'Table containing auxiliary and boiler power data for different vessel types'
"""
)

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

df = spark.table(TABLE_NAME_SOURCE).where("cr944_datasource ='vessel_aux_boiler_kw'")

# COMMAND ----------

df = df.select(
    F.col("cr944_vesseltype").alias("vessel_type"),
    F.col("cr944_unit").alias("unit"),
    F.col("cr944_lowlim").alias("low_limit"),
    F.col("cr944_highlim").alias("high_limit"),
    F.col("cr944_auxberth").alias("aux_berth_kw"),
    F.col("cr944_auxanchor").alias("aux_anchor_kw"),
    F.col("cr944_auxmaneuver").alias("aux_maneuver_kw"),
    F.col("cr944_auxcruise").alias("aux_cruise_kw"),
    F.col("cr944_boilerberth").alias("boiler_berth_kw"),
    F.col("cr944_boileranchor").alias("boiler_anchor_kw"),
    F.col("cr944_boilermaneuver").alias("boiler_maneuver_kw"),
    F.col("cr944_boilercruise").alias("boiler_cruise_kw"),
)

# COMMAND ----------

# MAGIC %md #Transformations

# COMMAND ----------

# DBTITLE 1,Add size bin column
window_size_bin = Window.partitionBy(F.col("vessel_type")).orderBy(F.col("low_limit"))

df = df.withColumn("size_bin", F.row_number().over(window_size_bin))

# COMMAND ----------

# DBTITLE 1,Set MAX_VALUE for highest value of vessel_type group
# Fill na values with MAX_VALUE for highest value of low_limit for each vessel_type

# Create a window with group by vessel_type order by low_limit
window = Window.partitionBy(F.col("vessel_type"))

# Add a new column indicating the max low_limit for this group
df = df.withColumn("low_limit_max", F.max(F.col("low_limit")).over(window))

# Update the high_limit value where low_limit is equal to the max low_limit for this group
df = df.withColumn(
    "high_limit",
    F.when((F.col("low_limit") == F.col("low_limit_max")), MAX_VALUE).otherwise(
        F.col("high_limit")
    ),
)

# COMMAND ----------

# DBTITLE 1,Create size_bin group 0 to account for missing vessel data
# Create a new size_bin group (0) for each vessel_type. Fill this with UNKNOWN values for low_limit and hig_lim
df_unknown_unit = df.groupBy(["vessel_type", "unit"]).agg(
    F.lit(UNKNOWN_UNIT_VALUE_LOW_LIMIT).alias("low_limit"),
    F.lit(UNKNOWN_UNIT_VALUE_HIGH_LIMIT).alias("high_limit"),
    F.lit(0).alias("size_bin"),
    F.lit(None).alias("aux_berth_kw"),
    F.lit(None).alias("aux_anchor_kw"),
    F.lit(None).alias("aux_maneuver_kw"),
    F.lit(None).alias("aux_cruise_kw"),
    F.lit(None).alias("boiler_berth_kw"),
    F.lit(None).alias("boiler_anchor_kw"),
    F.lit(None).alias("boiler_maneuver_kw"),
    F.lit(None).alias("boiler_cruise_kw"),
    F.lit(None).alias("low_limit_max"),
)

# Add new size_bin group to original dataframe
columns = df_unknown_unit.columns
df = df[columns].union(df_unknown_unit[columns])

# Sort and select
df = df.sort(F.col("vessel_type"), F.col("size_bin")).drop("low_limit_max")

# COMMAND ----------

# Add metadata
df = add_processing_timestamp(df)

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(name=TABLE_NAME_DESTINATION)
