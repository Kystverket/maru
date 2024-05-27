# Databricks notebook source
import os

import dlt
from transformations import change_data_types

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")
BRONZE_TABLE = f"bronze_{ENV}.maru.emission_variables"
SILVER_TABLE = "emission_variables"
# COMMAND ----------


@dlt.view
def input_emission_variables_bronze_table():
    """
    Reads bronze table and add transformations. Return silver table.
    """
    df_in = spark.readStream.table(BRONZE_TABLE)
    df_data_types = change_data_types(df_in)
    return df_data_types


dlt.create_streaming_table(
    name=SILVER_TABLE,
    table_properties={"medallion": "silver"},
)
dlt.apply_changes(
    target=SILVER_TABLE,
    source="input_emission_variables_bronze_table",
    keys=["variable", "start_date"],
    sequence_by="bi_ingested_timestamp",
    except_column_list=["bi_ingested_timestamp"],
    stored_as_scd_type="2",
)
