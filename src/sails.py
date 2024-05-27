# Databricks notebook source
# MAGIC %pip install databricks-mosaic==0.3.11

# COMMAND ----------

import pyspark.sql.functions as F
from mosaic import enable_mosaic

# COMMAND ----------

enable_mosaic(spark, dbutils)

# COMMAND ----------

# #For development
# dbutils.widgets.text("table_name_silver_maru", "")
# dbutils.widgets.text("table_name_silver_maru_sails", "")
# dbutils.widgets.text("file_path_silver_maru_sails", "")

# COMMAND ----------

TABLE_NAME_SILVER_MARU = dbutils.widgets.get("table_name_silver_maru")
TABLE_NAME_SILVER_MARU_SAILS = dbutils.widgets.get("table_name_silver_maru_sails")
FILE_PATH_SILVER_MARU_SAILS = dbutils.widgets.get("file_path_silver_maru_sails")

# COMMAND ----------

df = spark.table(TABLE_NAME_SILVER_MARU)

# COMMAND ----------

df_grouped = df.groupBy("vessel_id", "mmsi", "sail_id").agg(
    F.expr("st_makeline(collect_list(st_astext(st_point(lon, lat))))").alias(
        "geometry"
    ),
    F.sum("main_engine_kwh").alias("main_engine_kwh"),
    F.sum("main_engine_fuel_kg").alias("main_engine_fuel_kg"),
    F.sum("bc_kg").alias("bc_kg"),
    F.sum("ch4_kg").alias("ch4_kg"),
    F.sum("co_kg").alias("co_kg"),
    F.sum("co2_kg").alias("co2_kg"),
    F.sum("n2o_kg").alias("n2o_kg"),
    F.sum("nmvoc_kg").alias("nmvoc_kg"),
    F.sum("nox_kg").alias("nox_kg"),
    F.sum("pm10_kg").alias("pm10_kg"),
    F.sum("sox_kg").alias("sox_kg"),
)

# COMMAND ----------

df_grouped.write.mode("overwrite").saveAsTable(
    name=TABLE_NAME_SILVER_MARU_SAILS, path=FILE_PATH_SILVER_MARU_SAILS
)
