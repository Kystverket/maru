# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

# COMMAND ----------

from datetime import datetime

import pytz

# pylint: disable=no-name-in-module
from area.areas import get_areas, join_ais_area, set_area_distance
from area.electric_shore_power_at_berth import (
    join_ais_vessel_port_electric,
    set_electric_shore_power_at_berth,
)
from consumption.kwh import (
    calculate_energy_consumption,
)
from emission.co2e import calculate_co2e
from emission.low_load import low_load
from emission.sox import set_sox_factor
from emission.total_emission import total_emission
from maru_functions import (
    calculate_emission,
    calculate_fuel,
    create_schema_and_table_if_not_exists,
    set_consumption_factors,
    set_emission_factors,
)
from pyspark.sql.functions import broadcast, coalesce, col, current_date, lit, to_date
from utilities.job_stats.upsert_job_stats import (
    get_notebook_name,
    upsert_ais_job_stats,
)
from utilities.tools import StopWatch, create_date_periods
from utilities.transformers.common import add_processing_timestamp
from utilities.transformers.h3 import CLOSE_TO_POWER_CELL
from utilities.transformers.silver import (
    select_expression_from_table_and_prefix_columns,
)
from utils.config import DATE_VARIABLES, VERSION
from utils.read_data import (
    create_variables_dictionary,
    read_ais_voyages,
    read_feature_values,
    read_vessel_data,
)
from utils.select_columns import select_maru_output_columns, select_vessel_columns

# COMMAND ----------

norway_timezone = pytz.timezone("Europe/Oslo")
notebook_name = get_notebook_name()

PROCESS_ALL_DATA = dbutils.widgets.get("process_all_data")
DEFAULT_START_DATE = dbutils.widgets.get("default_start_date")

# COMMAND ----------

# MAGIC %md #Variables

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

SOURCE_TABLE_NAME_AIS = f"gold_{ENV}.martraf.voyages"
SOURCE_TABLE_NAME_VARIABLES = f"gold_{ENV}.maru.emission_variables"
SOURCE_TABLE_NAME_VESSEL = f"gold_{ENV}.maru.dim_vessel_historical"
SOURCE_TABLE_NAME_VESSEL_PORT_ELECTRIC = f"gold_{ENV}.maru.vessel_port_electric"
SOURCE_TABLE_NAME_VESSEL_TYPE_PORT_ELECTRIC = (
    f"gold_{ENV}.maru.vessel_type_port_electric"
)

TARGET_TABLE_NAME_MARU = f"gold_{ENV}.maru.fact_emission"

AREAS = ["global", "eca"]
ENGINES = ["main_engine", "aux", "boiler"]
ENGINES_AUX_BOILER = ["aux", "boiler"]
EMISSIONS = ["bc", "ch4", "co", "co2", "n2o", "nmvoc", "nox", "pm10", "sox"]

AREA_TABLES = get_areas()

# COMMAND ----------

create_schema_and_table_if_not_exists(ENV, TARGET_TABLE_NAME_MARU)

# COMMAND ----------

# MAGIC %md #Read vessel data

# COMMAND ----------

# Read feature variable values to dataframe:
df_variables_in = read_feature_values(DATE_VARIABLES, SOURCE_TABLE_NAME_VARIABLES)

# Create variables dictionary from dataframe:
var = create_variables_dictionary(df_variables_in)

# COMMAND ----------

# Read vessel data
df_vessel_in = read_vessel_data(SOURCE_TABLE_NAME_VESSEL)

# Filter so only vessels with MarU vessel type is present
df_vessel_filtered = df_vessel_in.where(
    (df_vessel_in["vessel_type_maru"].isNotNull())
    & (df_vessel_in["vessel_type_maru"] != "")
    & (df_vessel_in["vessel_type_maru"] != "Unknown")
)

# Vessel port electric
df_vessel_port_electric = spark.table(
    SOURCE_TABLE_NAME_VESSEL_PORT_ELECTRIC
).withColumn("end_date", coalesce(col("end_date"), current_date()).alias("end_date"))

# Vessel type port electric
df_vessel_type_port_electric = spark.table(
    SOURCE_TABLE_NAME_VESSEL_TYPE_PORT_ELECTRIC
).withColumn(
    "end_date",
    coalesce(col("end_date"), current_date()).alias("end_date"),
)

# COMMAND ----------

df_vessel = set_emission_factors(df_vessel_filtered, var, ENGINES_AUX_BOILER)
df_vessel = set_consumption_factors(df_vessel, var)
df_vessel = set_sox_factor(df_vessel, var, ENGINES, AREAS)


# COMMAND ----------

df_vessel = select_vessel_columns(df_vessel)

# COMMAND ----------

df_vessel.cache().count()


# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

periods = create_date_periods(
    env=ENV,
    process_all_data=PROCESS_ALL_DATA,
    source_table="ais_global_voyages_gold",
    destination_table="maru",
    start_date=DEFAULT_START_DATE,
    days_to_process=90,
)


for list_of_dates in periods:
    start_date = list_of_dates[0]
    end_date = list_of_dates[1]

    current_time = datetime.now(norway_timezone)
    current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"{current_time}: Processing ", start_date, " to ", end_date)

    stopwatch = StopWatch(start_date, end_date)
    stopwatch.start()

    # Read AIS data
    df_ais_in = read_ais_voyages(SOURCE_TABLE_NAME_AIS, start_date, end_date)

    # Merge AIS and vessel data
    df_merged = df_ais_in.join(
        broadcast(df_vessel),
        on=(
            (df_ais_in.vessel_id == df_vessel.vessel_id)
            & (to_date(df_ais_in.date_time_utc) >= df_vessel.start_date)
            & (to_date(df_ais_in.date_time_utc) <= df_vessel.end_date)
        ),
        how="inner",
    ).drop(df_vessel.vessel_id)

    # Join Area datasets:
    for area in AREA_TABLES:
        # Get configs:
        area_name = area.get("area_name")
        table = area.get("table_name", area_name)
        k_ring = area.get("k_ring")
        h3 = area.get("hex_resolution")
        h3_krings = area.get("hex_krings") if k_ring else None
        filter_expr = area.get("filter_sql_expression")

        # Read area data:
        df_area = select_expression_from_table_and_prefix_columns(
            table_name=f"gold_{ENV}.area.{table}",
            select_expr=area.get("select_expression"),
            column_prefix=area_name,
        )

        if filter_expr:
            df_area = df_area.where(filter_expr)

        # Join Area and AIS data:
        df_merged = join_ais_area(
            ais_df=df_merged,
            area_df=df_area,
            name=area_name,
            window_id=area.get("window_id"),
            window_orderby_key=area.get("window_orderby_key"),
            h3_resolution=h3,
            k_rings=h3_krings,
        )

    # Set area distance
    df_merged_area_distance = set_area_distance(df_merged, CLOSE_TO_POWER_CELL)

    # Calculate kwh consumption
    df_kwh_consumption = calculate_energy_consumption(df_merged_area_distance)

    # Set electric shore power at berth
    df_vessel_shore_power = join_ais_vessel_port_electric(
        df_kwh_consumption, df_vessel_port_electric, df_vessel_type_port_electric
    )
    df_electric_shore_power = set_electric_shore_power_at_berth(df_vessel_shore_power)

    # Calculate fuel consumption
    df_fuel_consumption = calculate_fuel(df_electric_shore_power, var, ENGINES)

    # Calculate emissions
    df_emission = calculate_emission(
        df_fuel_consumption, var, ENGINES, ENGINES_AUX_BOILER, AREAS
    )

    # Set low load
    df_low_load = low_load(df_emission, var)

    # Sum total emissions
    df_total_emission = total_emission(df_low_load, ENGINES, EMISSIONS)

    # Calculate CO2 equivalent
    df_co2e = calculate_co2e(df_total_emission, var)

    # Add processing time
    df_processingtime = add_processing_timestamp(df_co2e)

    # Add version number and select columns
    df_version = df_processingtime.withColumn("version", lit(VERSION))
    df_selected = select_maru_output_columns(df_version)

    # Write table
    df_selected.write.mode("append").saveAsTable(TARGET_TABLE_NAME_MARU)

    print("Successfully wrote data until ", end_date)
    stopwatch.stop()

    # Write AIS job statistics:
    df_dates_written = df_selected.select(
        to_date("date_time_utc").alias("date_utc"), "bi_processing_time"
    ).dropDuplicates(subset=["date_utc"])

    upsert_ais_job_stats(
        dataframe_updates=df_dates_written,
        target_col_notebook=notebook_name,
        updates_col_timestamp="bi_processing_time",
    )

print("Finished.")
