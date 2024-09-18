# Databricks notebook source
import os

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

spark.sql(f"OPTIMIZE gold_{ENV}.maru.maru_report")
