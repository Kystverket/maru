# Databricks notebook source
import os

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")
MARU_FULL_TABLE_NAME = f"gold_{ENV}.maru.maru_raw"

# COMMAND ----------

spark.sql(f" OPTIMIZE {MARU_FULL_TABLE_NAME} ZORDER BY(mmsi, date_time_utc)")
