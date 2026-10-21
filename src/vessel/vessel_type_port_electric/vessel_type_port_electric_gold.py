# Databricks notebook source
import os
import sys

# COMMAND ----------

sys.path.append(os.path.abspath("../../../../"))  # notebooks/
sys.path.append(os.path.abspath("../../"))  # maru_v1_10/

# COMMAND ----------

import pyspark.sql.functions as F
from utilities.transformers.common import add_processing_timestamp
from utils.config import VERSION

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_DATAVERSE_TABLE_FULL_PATH = f"silver_{ENV}.dataverse.cr944_maru_hovedtabells_v1"

TARGET_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_type_port_electric_{VERSION}"

# COMMAND ----------

# MAGIC %md #Setup

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE_FULL_PATH} (
    start_date DATE COMMENT 'From date of the record',
    end_date DATE COMMENT 'To date of the record',
    low_lim INT COMMENT 'Lower limit of the range',
    high_lim INT COMMENT 'Upper limit of the range',
    vessel_type STRING COMMENT 'Type of the vessel',
    municipality_name STRING COMMENT 'Name of the municipality',
    usage_percentage DOUBLE COMMENT 'Percentage of power usage',
    municipality_no STRING COMMENT 'Municipality number extracted from name',
    bi_processing_time TIMESTAMP COMMENT 'Last processing timestamp'
)
COMMENT 'Table containing vessel port electric usage data';
          """)

# COMMAND ----------

df_vessel_type_port_electric = (
    spark.table(SOURCE_DATAVERSE_TABLE_FULL_PATH)
    .where("cr944_datasource = 'vessel_type_port_electric'")
    .select(
        F.col("cr944_startdate").alias("start_date"),
        F.col("cr944_enddate").alias("end_date"),
        F.col("cr944_lowlim").alias("low_lim"),
        F.col("cr944_highlim").alias("high_lim"),
        F.col("cr944_vesseltype").alias("vessel_type"),
        F.col("cr944_municipalityname").alias("municipality_name"),
        F.col("cr944_usagepercentagevesseltypeportelectric")
        .cast("double")
        .alias("usage_percentage"),
    )
    .withColumns(
        {
            "end_date": F.coalesce(F.col("end_date"), F.current_date()),
            "municipality_no": F.regexp_extract(
                F.col("municipality_name"), r"\((\d+)\)", 1
            ),
        }
    )
)

# COMMAND ----------

df_processing_time = add_processing_timestamp(df_vessel_type_port_electric)

# COMMAND ----------

df_processing_time.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TARGET_TABLE_FULL_PATH)
