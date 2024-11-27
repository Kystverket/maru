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
    join_ais_vessel_port_electric,
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
from pyspark.sql.functions import broadcast, coalesce, col, current_date, lit, to_date
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
from utils.select_columns import select_maru_output_columns, select_vessel_columns

# COMMAND ----------

norway_timezone = pytz.timezone("Europe/Oslo")
notebook_name = get_notebook_name()

# COMMAND ----------

# MAGIC %md #Variables

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

TABLE_NAME_AIS = f"gold_{ENV}.ais.ais_voyages"
TABLE_NAME_MARU = f"gold_{ENV}.maru.fact_emission"
TABLE_NAME_VARIABLES = f"silver_{ENV}.maru.emission_variables"
TABLE_NAME_VESSEL = f"gold_{ENV}.maru.dim_vessel_historical"
TABLE_NAME_VESSEL_PORT_ELECTRIC = f"gold_{ENV}.maru.vessel_port_electric"
TABLE_NAME_VESSEL_TYPE_PORT_ELECTRIC = f"gold_{ENV}.maru.vessel_type_port_electric"

AREAS = ["global", "eca"]
ENGINES = ["main_engine", "aux", "boiler"]
ENGINES_AUX_BOILER = ["aux", "boiler"]
EMISSIONS = ["bc", "ch4", "co", "co2", "n2o", "nmvoc", "nox", "pm10", "sox"]

AREA_TABLES = get_areas()

# COMMAND ----------

# MAGIC %md #Create schema and table

# COMMAND ----------

spark.sql(
    f"""
          CREATE SCHEMA IF NOT EXISTS gold_{ENV}.maru
          """
)

# COMMAND ----------

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_MARU} (
            id STRING COMMENT 'Unique identifier',
            date_time_utc TIMESTAMP COMMENT 'Date and time in UTC',
            mmsi BIGINT COMMENT 'Maritime Mobile Service Identity',
            vessel_id INT COMMENT 'Identifier for the vessel',
            phase STRING COMMENT 'Current phase of the vessel',
            sail_id STRING COMMENT 'Sail identifier',
            geometry_wkt STRING COMMENT 'Geometry in Well-Known Text format',
            hex_14 BIGINT COMMENT 'Hexadecimal representation',
            emission_control_area_id STRING COMMENT 'Emission control area identifier',
            management_plan_marine_areas_area_id INT COMMENT 'Management plan marine areas area identifier',
            maritime_borders_norwegian_economic_zone_id STRING COMMENT 'Norwegian economic zone identifier',
            maritime_borders_norwegian_continental_shelf_id STRING COMMENT 'Norwegian continental shelf identifier',
            shore_power_id STRING COMMENT 'Shore power identifier',
            municipality_id STRING COMMENT 'Municipality identifier',
            in_coast_and_sea_area BOOLEAN COMMENT 'Indicator if in coast and sea area',
            electric_shore_power_at_berth_reduction_factor DOUBLE COMMENT 'Electric shore power at berth reduction factor',
            main_engine_load_factor DOUBLE COMMENT 'Main engine load factor',
            main_engine_kwh DOUBLE COMMENT 'Main engine kWh',
            aux_kwh DOUBLE COMMENT 'Auxiliary engine kWh',
            boiler_kwh DOUBLE COMMENT 'Boiler kWh',
            fuel_tonnes DOUBLE COMMENT 'Fuel in tonnes',
            fuel_mdo_equivalent_tonnes DOUBLE COMMENT 'Fuel MDO (Marine diesel oil) equivalent in tonnes',
            bc_tonnes DOUBLE COMMENT 'Black carbon in tonnes',
            ch4_tonnes DOUBLE COMMENT 'Methane in tonnes',
            co_tonnes DOUBLE COMMENT 'Carbon monoxide in tonnes',
            co2_tonnes DOUBLE COMMENT 'Carbon dioxide in tonnes',
            n2o_tonnes DOUBLE COMMENT 'Nitrous oxide in tonnes',
            nmvoc_tonnes DOUBLE COMMENT 'Non-methane volatile organic compounds in tonnes',
            nox_tonnes DOUBLE COMMENT 'Nitrogen oxides in tonnes',
            pm10_tonnes DOUBLE COMMENT 'Particulate matter (with a diameter of 10 microns or less) in tonnes',
            pm2_5_tonnes DOUBLE COMMENT 'Particulate matter (with a diameter of 2.5 microns or less) in tonnes',
            sox_tonnes DOUBLE COMMENT 'Sulfur oxides in tonnes',
            co2e_tonnes DOUBLE COMMENT 'CO2 equivalent in tonnes',
            delta_previous_point_seconds BIGINT COMMENT 'Seconds from previous AIS point',
            distance_previous_point_meters DOUBLE COMMENT 'Distance in meters from previous AIS point',
            bi_processing_time TIMESTAMP COMMENT 'Last processing time',
            version STRING COMMENT 'MarU version number'
        )
        CLUSTER BY(mmsi, date_time_utc)
        COMMENT 'MarU raw data'
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

# Vessel port electric
df_vessel_port_electric = spark.table(TABLE_NAME_VESSEL_PORT_ELECTRIC).withColumn(
    "end_date", coalesce(col("end_date"), current_date()).alias("end_date")
)

# Vessel type port electric
df_vessel_type_port_electric = spark.table(
    TABLE_NAME_VESSEL_TYPE_PORT_ELECTRIC
).withColumn(
    "end_date",
    coalesce(col("end_date"), current_date()).alias("end_date"),
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

df_vessel = select_vessel_columns(df_vessel)

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
    df_processingtime = add_processing_timestamp(df_co2e)

    # Add version number and select columns
    df_version = df_processingtime.withColumn("version", lit(VERSION))
    df_selected = select_maru_output_columns(df_version)

    # Write table
    df_selected.write.mode("append").saveAsTable(TABLE_NAME_MARU)

    print("Successfully wrote data until ", end_date)
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"Total time: {minutes} minutes and {seconds} seconds")

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
