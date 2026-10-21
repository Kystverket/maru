# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))
from utils.config import VERSION

# COMMAND ----------

version = dbutils.widgets.get("version")
print(version)

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV

# COMMAND ----------

spark.sql(
    f"""
          DELETE FROM gold_{ENV}.maru.fact_emission_{VERSION}
          WHERE version = "{version}"
          """
).show()

# COMMAND ----------

spark.sql(
    f"""
          DELETE FROM gold_{ENV}.maru.dm_maru_report_{VERSION}
          WHERE version = "{version}"
          """
).show()
