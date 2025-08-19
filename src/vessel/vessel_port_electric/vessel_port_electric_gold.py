# Databricks notebook source
import os
import sys

# COMMAND ----------

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("../../../"))

# COMMAND ----------

from utilities.transformers.common import add_processing_timestamp
from utils.config import MUNICIPALITY_CURRENT_YEAR

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

TABLE_NAME_DATAVERSE = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells"
TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI = (
    f"gold_{ENV}.shipdata.combined_exploded_imputation"
)
TABLE_NAME_MUNICIPALITY = f"gold_{ENV}.area.municipality_historical"

TABLE_NAME_DESTINATION = f"gold_{ENV}.maru.vessel_port_electric"

# COMMAND ----------

# MAGIC %md #Setup

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DESTINATION} (
    vessel_id INT COMMENT 'Unique identifier for the vessel',
    mmsi BIGINT COMMENT 'Maritime Mobile Service Identity number',
    imo BIGINT COMMENT 'International Maritime Organization number',
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

df_shipdata_combined_imo_mmsi = spark.table(
    TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI
)

df_municipality_current = spark.table(TABLE_NAME_MUNICIPALITY).where(
    f"year = {MUNICIPALITY_CURRENT_YEAR}"
)

df_vessel_port_electric_in = (
    spark.table(TABLE_NAME_DATAVERSE)
    .where("cr944_datasource = 'vessel_port_electric'")
    .selectExpr(
        "cr944_mmsi as mmsi",
        "cr944_imo as imo",
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

# Add vessel_id and municipality_no
df_vessel_port_electric_vessel_id = (
    df_vessel_port_electric_in.join(
        df_shipdata_combined_imo_mmsi, on=["imo", "mmsi"], how="inner"
    )
    .join(df_municipality_current, "municipality_name", "inner")
    .select(
        df_shipdata_combined_imo_mmsi["vessel_id"],
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
df_processing_time = add_processing_timestamp(df_vessel_port_electric_vessel_id)

# COMMAND ----------

# MAGIC %md #Write table

# COMMAND ----------

df_processing_time.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    TABLE_NAME_DESTINATION
)
