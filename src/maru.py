# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

# COMMAND ----------

import time
from datetime import datetime

import pytz

# pylint: disable=no-name-in-module
from area.areas import get_areas, join_ais_area, set_area_distance
from area.electric_shore_power_at_berth import (
    set_electric_shore_power_at_berth,
    set_phase_p_when_electric_shore_power_at_berth,
)
from consumption.fuel import calculate_fuel_consumption, calculate_fuel_equivalent
from consumption.kwh import (
    calculate_aux_boiler_kwh,
    calculate_main_engine_kwh,
    calculate_main_engine_load_factor,
)
from consumption.sfc import (
    calculate_sfc_load,
    set_sfc_aux_baseline,
    set_sfc_boiler_baseline,
    set_sfc_main_engine_baseline,
    set_sfc_pilot_fuel,
)
from emission.bc import (
    calculate_bc_aux_boiler,
    calculate_bc_main_engine,
    set_bc_aux_boiler_factor,
    set_main_engine_bc_factor,
)
from emission.ch4 import (
    calculate_ch4,
    set_ch4_aux_boiler_factor,
    set_ch4_main_engine_factor,
)
from emission.co import (
    calculate_co,
    set_co_aux_boiler_factor,
    set_co_main_engine_factor,
)
from emission.co2 import calculate_co2, set_cf
from emission.co2e import calculate_co2e
from emission.fsf import set_fuel_sulfur_fraction_eca, set_fuel_sulfur_fraction_global
from emission.low_load import low_load
from emission.n2o import (
    calculate_n2o,
    set_n2o_aux_boiler_factor,
    set_n2o_main_engine_factor,
)
from emission.nmvoc import (
    calculate_nmvoc,
    set_nmvoc_aux_factor,
    set_nmvoc_boiler_factor,
    set_nmvoc_main_engine_factor,
)
from emission.nox import (
    calculate_nox,
    set_nox_aux_boiler_factor,
    set_nox_main_engine_eca_factor,
    set_nox_main_engine_factor,
)
from emission.pm import calculate_pm_2_5, calculate_pm_10, set_pm_factor
from emission.sox import calculate_sox, set_sox_factor
from emission.total_emission import total_emission
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, lit, to_date
from utilities.job_stats.upsert_job_stats import (
    create_date_ranges,
    get_notebook_name,
    upsert_ais_job_stats,
)
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
    read_vessel_data_silver,
)

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

CATALOG_MARU = f"gold_{ENV}"
SCHEMA_MARU = "maru"

TABLE_NAME_VARIABLES = f"silver_{ENV}.maru.emission_variables"
TABLE_NAME_VESSEL = f"gold_{ENV}.maru.vessel"
TABLE_NAME_AIS = f"gold_{ENV}.ais.ais_voyages"
TABLE_NAME_MARU = "maru_raw"

MARU_FULL_TABLE_NAME = f"{CATALOG_MARU}.{SCHEMA_MARU}.{TABLE_NAME_MARU}"

ENGINES = ["main_engine", "aux", "boiler"]
ENGINES_AUX_BOILER = ["aux", "boiler"]
EMISSIONS = ["bc", "ch4", "co", "co2", "n2o", "nmvoc", "nox", "pm10", "sox"]
AREAS = ["global", "eca"]

AREA_TABLES = get_areas()

# COMMAND ----------

norway_timezone = pytz.timezone("Europe/Oslo")
notebook_name = get_notebook_name()

# COMMAND ----------

spark.sql(
    f"""
          CREATE SCHEMA IF NOT EXISTS {CATALOG_MARU}.{SCHEMA_MARU}
          """
)

# COMMAND ----------

# MAGIC %md #Read vessel data

# COMMAND ----------

# Read feature variable values to dataframe:
df_variables_in = read_feature_values(DATE_VARIABLES, TABLE_NAME_VARIABLES)

# Create variables dictionary from dataframe:
var = create_variables_dictionary(df_variables_in)

# COMMAND ----------

# Read vessel data
df_vessel_in = read_vessel_data_silver(TABLE_NAME_VESSEL)

# Filter so only vessels with MarU vessel type is present
df_vessel_filtered = df_vessel_in.filter(
    (df_vessel_in["vessel_type_maru"].isNotNull())
    & (df_vessel_in["vessel_type_maru"] != "")
    & (df_vessel_in["vessel_type_maru"] != "Unknown")
)

# COMMAND ----------

# MAGIC %md #Set emission factors

# COMMAND ----------

# Black Carbon (bc)
df_vessel = set_bc_aux_boiler_factor(df_vessel_filtered, var, ENGINES_AUX_BOILER)

# Carbon conversion factors (cf)
df_vessel = set_cf(df_vessel, var)

# ch4
df_vessel = set_ch4_main_engine_factor(df_vessel, var)
df_vessel = set_ch4_aux_boiler_factor(df_vessel, var, ENGINES_AUX_BOILER)

# co
df_vessel = set_co_main_engine_factor(df_vessel, var)
df_vessel = set_co_aux_boiler_factor(df_vessel, var)

# Fuel sulfur fraction (fsf)
df_vessel = set_fuel_sulfur_fraction_global(df_vessel, var)
df_vessel = set_fuel_sulfur_fraction_eca(df_vessel, var)

# n2o
df_vessel = set_n2o_main_engine_factor(df_vessel, var)
df_vessel = set_n2o_aux_boiler_factor(df_vessel, var)

# nmvoc
df_vessel = set_nmvoc_main_engine_factor(df_vessel, var)
df_vessel = set_nmvoc_aux_factor(df_vessel, var)
df_vessel = set_nmvoc_boiler_factor(df_vessel, var)

# nox
df_vessel = set_nox_main_engine_factor(df_vessel, var)
df_vessel = set_nox_main_engine_eca_factor(df_vessel, var)
df_vessel = set_nox_aux_boiler_factor(df_vessel, var, ENGINES_AUX_BOILER)

# Specific Fuel Consumption (sfc)
df_vessel = set_sfc_main_engine_baseline(df_vessel, var)
df_vessel = set_sfc_pilot_fuel(df_vessel, var)
df_vessel = set_sfc_aux_baseline(df_vessel, var)
df_vessel = set_sfc_boiler_baseline(df_vessel, var)

# sox
df_vessel = set_sox_factor(df_vessel, var, ENGINES, AREAS)

# COMMAND ----------

# MAGIC %md # Select vessel columns

# COMMAND ----------

df_vessel = df_vessel.select(
    "vessel_id",
    "vessel_type_maru",
    # "vessel_type_id",
    "main_engine_fueltype",
    "main_engine_enginetype",
    "year_of_build",
    "main_engine_rpm",
    "speed_service",
    "stroketype",
    "degree_of_electrification",
    "main_engine_kw",
    "aux_berth_kw",
    "aux_anchor_kw",
    "aux_maneuver_kw",
    "aux_cruise_kw",
    "boiler_berth_kw",
    "boiler_anchor_kw",
    "boiler_maneuver_kw",
    "boiler_cruise_kw",
    "cf_eca",
    "cf_global",
    "sfc_main_engine_baseline",
    "sfc_aux_baseline",
    "sfc_boiler_baseline",
    "sfc_pilot_fuel",
    "fuel_sulfur_fraction_global",
    "fuel_sulfur_fraction_eca",
    "bc_aux_factor_global",
    "bc_aux_factor_eca",
    "bc_boiler_factor_global",
    "bc_boiler_factor_eca",
    "ch4_main_engine_factor",
    "ch4_aux_factor",
    "ch4_boiler_factor",
    "co_main_engine_factor_global",
    "co_main_engine_factor_eca",
    "co_aux_factor_global",
    "co_aux_factor_eca",
    "co_boiler_factor_global",
    "co_boiler_factor_eca",
    "n2o_main_engine_factor_global",
    "n2o_main_engine_factor_eca",
    "n2o_aux_factor_global",
    "n2o_aux_factor_eca",
    "n2o_boiler_factor_global",
    "n2o_boiler_factor_eca",
    "nmvoc_main_engine_factor",
    "nmvoc_aux_factor",
    "nmvoc_boiler_factor",
    "nox_main_engine_factor_global",
    "nox_main_engine_factor_eca",
    "nox_aux_factor_global",
    "nox_aux_factor_eca",
    "nox_boiler_factor_global",
    "nox_boiler_factor_eca",
    "sox_main_engine_global_factor",
    "sox_aux_global_factor",
    "sox_boiler_global_factor",
    "sox_main_engine_eca_factor",
    "sox_aux_eca_factor",
    "sox_boiler_eca_factor",
)

# COMMAND ----------

df_vessel.cache().count()

# COMMAND ----------

# MAGIC %md #Calculate consumption

# COMMAND ----------


def calculate_energy_consumption(df: DataFrame) -> DataFrame:
    """
    Calculate energy consumption in kWh.

    Parameters:
    -----------
    df: A PySpark DataFrame.

    Returns:
    --------
    pyspark.sql.DataFrame

    """

    # Main engine load
    df = calculate_main_engine_load_factor(df)

    # Main engine kWh
    df = calculate_main_engine_kwh(df)

    # Aux and boiler kWh
    df = calculate_aux_boiler_kwh(df)

    return df


# COMMAND ----------


def calculate_fuel(df: DataFrame, variables: dict, engines: list) -> DataFrame:
    """
    Calculates fuel consumption i tonnes.

    Parameters:
    -----------
    df: A PySpark DataFrame.

    variables: dict
        A dictionary containing input variables for different
        fuel equivalent factors.

    engines: List[str]
        A list of engine types, ie. me, aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame

    """

    # Spesific fuel consumption
    df = calculate_sfc_load(df)

    # Fuel consumption
    df = calculate_fuel_consumption(df, engines)

    # Fuel MDO equivalents
    df = calculate_fuel_equivalent(df, variables, engines)

    return df


# COMMAND ----------

# MAGIC %md #Calculate emission

# COMMAND ----------


def calculate_emission(
    df: DataFrame,
    emission_factors: dict,
    engines: list,
    engines_aux_bc: list,
    areas: list,
) -> DataFrame:
    """
    Calculates all emissions.

    Parameters:
    -----------
    df: A PySpark DataFrame.

    emission_factors: Dictionary
        A dictionary containing input variables for different emission factors

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    engines: List[str]
        A list of aux engine and boiler types, ie. aux, boiler

    areas: List[str]
        A list of areas, ie. eca and global

    Returns:
    --------
    pyspark.sql.DataFrame

    """
    # bc
    df = set_main_engine_bc_factor(df, emission_factors)
    df = calculate_bc_main_engine(df)
    df = calculate_bc_aux_boiler(df, engines_aux_bc)

    # ch4
    df = calculate_ch4(df, engines)

    # co2
    df = calculate_co2(df, engines)

    # co
    df = calculate_co(df, engines)

    # n2o
    df = calculate_n2o(df, engines)

    # nmvoc
    df = calculate_nmvoc(df, engines)

    # nox
    df = calculate_nox(df, engines)

    # pm
    df = set_pm_factor(df, emission_factors, engines, areas)
    df = calculate_pm_10(df, engines)
    df = calculate_pm_2_5(df, emission_factors)

    # sox
    df = calculate_sox(df, engines)

    return df


# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------


def generate_list_of_periods_to_process(years):
    """Generate a list of periods to process based on a list of years."""
    dates = []
    for year in years:
        dates.append([f"{year}-01-01", f"{year}-03-31"])
        dates.append([f"{year}-04-01", f"{year}-06-30"])
        dates.append([f"{year}-07-01", f"{year}-09-30"])
        dates.append([f"{year}-10-01", f"{year}-12-31"])
    return dates


# COMMAND ----------

# Get dates to process
df_dates = (
    spark.table(f"gold_{ENV}.ais.ais_job_stats")
    .select("date_utc")
    .where("ais_voyages IS NOT NULL AND maru IS NULL")
)

dates_list = [row[0] for row in df_dates.collect()]

if not dates_list:
    dbutils.notebook.exit("No periods to process.")

# COMMAND ----------

periods = create_date_ranges(list_of_dates=dates_list, max_dates_per_range=90)
print(periods)

# periods = generate_list_of_periods_to_process([])

# if not periods:
#     raise ValueError("No periods to process.")

# COMMAND ----------

for list_of_dates in periods:
    start_date = list_of_dates[0]
    end_date = list_of_dates[1]

    current_time = datetime.now(norway_timezone)
    current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"{current_time}: Processing ", start_date, " to ", end_date)

    start_time = time.time()

    # Read AIS data
    df_ais_in = read_ais_voyages(TABLE_NAME_AIS, start_date, end_date)

    # Merge AIS and vessel data
    df_merged = df_ais_in.join(broadcast(df_vessel), on="vessel_id", how="inner")

    # Join Area datasets:
    for area in AREA_TABLES:
        # Get configs:
        area_name = area.get("area_name")
        table = area.get("table_name", area_name)
        k_ring = area.get("k_ring")
        h3 = area.get("hex_resolution")
        h3_krings = area.get("hex_krings") if k_ring else None

        # Read area data:
        df_area = select_expression_from_table_and_prefix_columns(
            table_name=f"gold_{ENV}.area.{table}",
            select_expr=area.get("select_expression"),
            column_prefix=area_name,
        )

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

    df_merged = df_merged.drop("municipality_year")

    # Set area distance
    df_merged_area_distance = set_area_distance(df_merged, CLOSE_TO_POWER_CELL)

    # Calculate kwh consumption
    df_kwh_consumption = calculate_energy_consumption(df_merged_area_distance)

    # Set electric shore power at berth
    df_electric_shore_power = set_electric_shore_power_at_berth(df_kwh_consumption)
    df_shore_power_phase = set_phase_p_when_electric_shore_power_at_berth(
        df_electric_shore_power
    )

    # Calculate fuel consumption
    df_fuel_consumption = calculate_fuel(df_shore_power_phase, var, ENGINES)

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
    df_procsessingtime = add_processing_timestamp(df_co2e)

    # Add version number
    df_final = df_procsessingtime.withColumn("version", lit(VERSION))

    df_final.write.mode("append").saveAsTable(MARU_FULL_TABLE_NAME)

    print("Successfully wrote data until ", end_date)
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"Total time: {minutes} minutes and {seconds} seconds")

    # Write AIS job statistics:
    df_dates_written = df_final.select(
        to_date("date_time_utc").alias("date_utc"), "bi_processing_time"
    ).dropDuplicates(subset=["date_utc"])

    upsert_ais_job_stats(
        dataframe_updates=df_dates_written,
        target_col_notebook=notebook_name,
        updates_col_timestamp="bi_processing_time",
    )


print("Finished.")
