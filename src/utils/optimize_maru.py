# Databricks notebook source
import os

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.maru.fact_emission")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.ais.ais_job_stats")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.maru.dim_vessel_current")
spark.sql(f"OPTIMIZE gold_{ENV}.maru.dim_vessel_historical")
