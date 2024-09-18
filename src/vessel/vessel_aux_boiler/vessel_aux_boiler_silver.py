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

CATALOG_DESTINATION = f"silver_{ENV}"
SCHEMA_DESTINATION = "maru"
TABLE_NAME_VESSEL_AUX_BOILER_KW_BRONZE = f"bronze_{ENV}.maru.vessel_aux_boiler_kw"
TABLE_NAME_VESSEL_AUX_BOILER_KW_SILVER = f"silver_{ENV}.maru.vessel_aux_boiler_kw"

# COMMAND ----------

MAX_VALUE = 9999999
UNKNOWN_UNIT_VALUE_LOW_LIMIT = -99
UNKNOWN_UNIT_VALUE_HIGH_LIMIT = 0

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_DESTINATION}.{SCHEMA_DESTINATION}")

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

df = spark.table(TABLE_NAME_VESSEL_AUX_BOILER_KW_BRONZE)

# Keep only latest record from bronze:
window_latest_data = Window.orderBy(F.col("bi_ingested_timestamp").desc())
df = (
    df.withColumn("rank_processing_time", F.dense_rank().over(window_latest_data))
    .where("rank_processing_time = 1 AND vessel_type IS NOT NULL")
    .drop("rank_processing_time")
)

# COMMAND ----------

# MAGIC %md #Transformations

# COMMAND ----------

# DBTITLE 1,Renaming
df = (
    df.withColumnRenamed("boiler_berth", "boiler_berth_kw")
    .withColumnRenamed("boiler_anchor", "boiler_anchor_kw")
    .withColumnRenamed("boiler_maneuver", "boiler_maneuver_kw")
    .withColumnRenamed("boiler_cruise", "boiler_cruise_kw")
    .withColumnRenamed("aux_berth", "aux_berth_kw")
    .withColumnRenamed("aux_anchor", "aux_anchor_kw")
    .withColumnRenamed("aux_maneuver", "aux_maneuver_kw")
    .withColumnRenamed("aux_cruise", "aux_cruise_kw")
)

# COMMAND ----------

# DBTITLE 1,Remove null values
df = df.dropna(how="all")

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
df_unknown_unit = df.groupBy(["vessel_type", "vessel_type_id", "unit"]).agg(
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


df.write.mode("overwrite").saveAsTable(name=TABLE_NAME_VESSEL_AUX_BOILER_KW_SILVER)
