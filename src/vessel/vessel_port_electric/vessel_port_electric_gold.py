# Databricks notebook source
import os
import sys

import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append(os.path.abspath("../../../../"))  # notebooks/
sys.path.append(os.path.abspath("../../"))  # maru_v1_10/

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utils.config import MUNICIPALITY_CURRENT_YEAR, VERSION

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_DATAVERSE_TABLE_FULL_PATH = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells_v1"
SOURCE_VESSEL_COMBINED_IMPUTATED_IMO_MMSI_TABLE_FULL_PATH = (
    "gold_prod.ais.qsr_shipdata_combined_scd2_v3"
)
SOURCE_MUNICIPALITY_TABLE_FULL_PATH = "gold_prod.area.municipality_historical"

TARGET_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_port_electric_{VERSION}"

# COMMAND ----------

# MAGIC %md #Setup

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE_FULL_PATH} (
    vessel_id BIGINT COMMENT 'Identifier for the vessel (hash of mmsi, imo and call sign)',
    mmsi INT COMMENT 'Maritime Mobile Service Identity number',
    imo INT COMMENT 'International Maritime Organization number',
    municipality_no STRING COMMENT 'Municipality number',
    municipality_name STRING COMMENT 'Name of the municipality',
    usage_percentage DOUBLE COMMENT 'Percentage of shore power usage',
    start_date DATE COMMENT 'Start date of the record',
    end_date DATE COMMENT 'End date of the record',
    bi_processing_time TIMESTAMP COMMENT 'Last processing timestamp'
)
COMMENT 'Table containing vessel port electric usage data';
          """)

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

df_shipdata_combined_imo_mmsi = (
    spark.table(SOURCE_VESSEL_COMBINED_IMPUTATED_IMO_MMSI_TABLE_FULL_PATH)
    .withColumn(
        "imo",
        F.coalesce(
            F.when(F.col("imo_ais") != 0, F.col("imo_ais")),
            F.col("imo_spglobal"),
        ),
    )
    .drop("imo_ais", "imo_spglobal")
)

df_municipality_current = spark.table(SOURCE_MUNICIPALITY_TABLE_FULL_PATH).where(
    f"year = {MUNICIPALITY_CURRENT_YEAR}"
)

df_vessel_port_electric_in = (
    spark.table(SOURCE_DATAVERSE_TABLE_FULL_PATH)
    .where("cr944_datasource = 'vessel_port_electric'")
    .selectExpr(
        "cast(cr944_mmsi as int) as mmsi",
        "cast(cr944_imo as int) as imo",
        "cr944_startdate as start_date",
        "cr944_enddate as end_date",
        "cr944_usagepercentage as usage_percentage",
        "cr944_municipalityname as municipality_name",
    )
    .dropDuplicates(subset=["mmsi", "imo", "start_date"])
)

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------

# Add vessel_id_hash and municipality_no
df_vessel_port_electric_vessel_hash = (
    df_vessel_port_electric_in.join(
        df_shipdata_combined_imo_mmsi,
        on=(
            (df_shipdata_combined_imo_mmsi.imo == df_vessel_port_electric_in.imo)
            & (df_shipdata_combined_imo_mmsi.mmsi == df_vessel_port_electric_in.mmsi)
        ),
        how="inner",
    )
    .join(df_municipality_current, "municipality_name", "inner")
    .select(
        df_shipdata_combined_imo_mmsi["vessel_id_hash"].alias("vessel_id"),
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

# Add metadata
df_vessel_port_electric_vessel_hash = add_processing_timestamp(
    df_vessel_port_electric_vessel_hash
)

# COMMAND ----------

# MAGIC %md #Write table

# COMMAND ----------

df_vessel_port_electric_vessel_hash.write.mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable(TARGET_TABLE_FULL_PATH)
