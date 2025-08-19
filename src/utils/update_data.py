# Databricks notebook source
import os

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

# MAGIC %md #Version 1.6.1

# COMMAND ----------

# MAGIC %md ## fact_emission
# MAGIC Remove phase 'p' and add column shore_power_is_connected as replacement

# COMMAND ----------

# spark.sql(f"""
# alter table gold_{ENV}.maru.fact_emission
# add columns shore_power_is_connected boolean
# """)

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.fact_emission
# set shore_power_is_connected = false
# """).show()

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.fact_emission
# set shore_power_is_connected = true where phase = 'p'
# """).show()

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.fact_emission
# set phase = 'n' where phase = 'p'
# """).show()

# COMMAND ----------

# MAGIC %md ##dm_maru_report
# MAGIC Remove phase 'p' and add column shore_power_is_connected as replacement

# COMMAND ----------

# spark.sql(f"""
# alter table gold_{ENV}.maru.dm_maru_report
# add columns shore_power_is_connected boolean
# """)

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.dm_maru_report
# set shore_power_is_connected = false
# """).show()

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.dm_maru_report
# set shore_power_is_connected = true where phase = 'p'
# """).show()

# COMMAND ----------

# spark.sql(f"""
# update gold_{ENV}.maru.dm_maru_report
# set phase = 'n' where phase = 'p'
# """).show()
