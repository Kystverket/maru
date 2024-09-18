# Databricks notebook source
import os

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.maru.maru_raw ZORDER BY(mmsi, date_time_utc)")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.ais.ais_job_stats")
