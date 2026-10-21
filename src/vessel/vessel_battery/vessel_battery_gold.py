# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import Window

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
TARGET_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_battery_{VERSION}"

SOURCE_VESSEL_COMBINED_IMPUTATED_IMO_MMSI_TABLE_FULL_PATH = (
    "gold_prod.ais.qsr_shipdata_combined_scd2_v3"
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE_FULL_PATH} (
  vessel_id BIGINT COMMENT 'Identifier for the vessel (hash of mmsi, imo and call sign)',
  mmsi INT COMMENT 'Maritime Mobile Service Identity number',
  imo INT COMMENT 'International Maritime Organization number',
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
window_partitionby_mmsiimocallsignhash_orderby_biprocessingtime = Window.partitionBy(
    "vessel_id_hash"
).orderBy("bi_processing_time")

df_shipdata_combined_imo_mmsi = (
    spark.table(SOURCE_VESSEL_COMBINED_IMPUTATED_IMO_MMSI_TABLE_FULL_PATH)
    .withColumn(
        "row_number_mmsiimocallsignhash",
        F.row_number().over(
            window_partitionby_mmsiimocallsignhash_orderby_biprocessingtime
        ),
    )
    .where("row_number_mmsiimocallsignhash = 1")
    .drop("row_number_mmsiimocallsignhash")
    .withColumn(
        "imo_ais", F.when(F.col("imo_ais") == -1, 0).otherwise(F.col("imo_ais"))
    )
    .withColumn(
        "imo",
        F.coalesce(
            F.when(F.col("imo_ais") != 0, F.col("imo_ais")),
            F.col("imo_spglobal"),
        ),
    )
    .drop("imo_ais", "imo_spglobal")
)

# Read silver table battery_electric
df_battery_electric = spark.table(SOURCE_TABLE_FULL_PATH).where(
    "cr944_datasource = 'vessel_battery'"
)

# Add vessel_id_hash
df_battery_electric_vessel_hash = (
    df_battery_electric.join(
        df_shipdata_combined_imo_mmsi,
        on=(df_shipdata_combined_imo_mmsi.imo == df_battery_electric.cr944_imo)
        & (df_shipdata_combined_imo_mmsi.mmsi == df_battery_electric.cr944_mmsi),
        how="inner",
    )
    .select(
        F.col("vessel_id_hash").alias("vessel_id"),
        F.col("cr944_mmsi").cast("int").alias("mmsi"),
        F.col("cr944_imo").cast("int").alias("imo"),
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
df_battery_electric_vessel_hash.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TARGET_TABLE_FULL_PATH)
