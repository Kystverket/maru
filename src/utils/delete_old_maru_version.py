# Databricks notebook source
import os

# COMMAND ----------

version = dbutils.widgets.get("version")
print(version)

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

spark.sql(
    f"""
          DELETE FROM gold_{ENV}.maru.fact_emission
          WHERE version = "{version}"
          """
).show()

# COMMAND ----------

spark.sql(
    f"""
          DELETE FROM gold_{ENV}.maru.dm_maru_report
          WHERE version = "{version}"
          """
).show()
