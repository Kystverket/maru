# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------

sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SILVER_TABLE = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells"
GOLD_TABLE = f"gold_{ENV}.maru.vessel_battery"

TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI = (
    f"gold_{ENV}.shipdata.combined_exploded_imputation"
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS gold_{ENV}.maru.vessel_battery (
  vessel_id INT COMMENT 'Unique identifier for the vessel',
  mmsi BIGINT COMMENT 'Maritime Mobile Service Identity number',
  imo BIGINT COMMENT 'International Maritime Organization number',
  degree_of_electrification DOUBLE COMMENT 'Degree of electrification of the vessel',
  battery_pack_kwh INT COMMENT 'Battery pack capacity in kilowatt-hours',
  start_date DATE COMMENT 'Start date of the battery usage period',
  end_date DATE COMMENT 'End date of the battery usage period',
  bi_processing_time TIMESTAMP COMMENT 'Last processing time'
)
COMMENT 'Table containing vessel battery information'
"""
)

# COMMAND ----------

#  Read silver table shipdata combined
window_partitionby_vesselid_orderby_vesselid = Window.partitionBy("vessel_id").orderBy(
    "bi_processing_time"
)

df_shipdata_combined_imo_mmsi = (
    spark.table(TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI)
    .withColumn(
        "row_number_vesselid",
        F.row_number().over(window_partitionby_vesselid_orderby_vesselid),
    )
    .where("row_number_vesselid = 1")
    .drop("row_number_vesselid")
    .withColumn("imo", F.replace("imo", F.lit(-1), F.lit(0)))
)

# Read silver table battery_electric
df_battery_electric = spark.table(SILVER_TABLE).where(
    "cr944_datasource = 'vessel_battery'"
)

# Add vessel_id
df_battery_electric_vessel_id = (
    df_battery_electric.join(
        df_shipdata_combined_imo_mmsi,
        on=(df_shipdata_combined_imo_mmsi.imo == df_battery_electric.cr944_imo)
        & (df_shipdata_combined_imo_mmsi.mmsi == df_battery_electric.cr944_mmsi),
        how="inner",
    )
    .select(
        df_shipdata_combined_imo_mmsi["vessel_id"],
        F.col("cr944_mmsi").alias("mmsi"),
        F.col("cr944_imo").alias("imo"),
        F.col("cr944_degreeofelectrification").alias("degree_of_electrification"),
        F.col("cr944_batterypackkwh").alias("battery_pack_kwh"),
        F.col("cr944_startdate").alias("start_date"),
        F.col("cr944_enddate").alias("end_date"),
    )
    .orderBy("vessel_id", "start_date")
    .dropDuplicates(subset=["vessel_id", "start_date"])
    .transform(add_processing_timestamp)
)


# Write table
df_battery_electric_vessel_id.write.mode("overwrite").saveAsTable(GOLD_TABLE)
