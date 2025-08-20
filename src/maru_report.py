# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

import time
from datetime import datetime

import pyspark.sql.functions as F
import pytz
from maru_report_functions import (
    calculate_days_in_month,
    calculate_municipality_emission_type,
    calculate_municipality_voyage_route,
    check_complete_months,
    create_table_if_not_exists,
    generate_aggregated_report,
    generate_list_of_periods_to_process,
    get_processed_dates,
    merge_datasets,
    read_ais_voyages_data,
    read_maru_raw_data,
    set_kwh_battery,
    set_svalbard_as_county,
)
from utilities.job_stats.upsert_job_stats import create_date_ranges
from utils.config import MUNICIPALITY_CURRENT_YEAR, VERSION

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

MARU_RAW_VERSION = VERSION

ENV: str = os.getenv("ENVIRONMENT")

SOURCE_TABLE_NAME_SOURCE_MARU_RAW = f"gold_{ENV}.maru.fact_emission"
SOURCE_TABLE_NAME_SOURCE_AIS_VOYAGES = f"gold_{ENV}.martraf.voyages"
SOURCE_TABLE_NAME_SOURCE_MARU_VESSEL = f"gold_{ENV}.maru.dim_vessel_historical"
SOURCE_TABLE_NAME_SOURCE_SHIPDATA_COMBINED = f"gold_{ENV}.shipdata.combined"
SOURCE_TABLE_NAME_AIS_JOBS = f"gold_{ENV}.ais.ais_job_stats"
SOURCE_TABLE_NAME_MUNICIPALITY = f"gold_{ENV}.area.municipality_historical"
SOURCE_TABLE_NAME_COUNTY = f"silver_{ENV}.administrative_enheter.fylke"
SOURCE_TABLE_NAME_MARITIME_BORDERS_NOR_EZ = (
    f"gold_{ENV}.area.maritime_borders_norwegian_economic_zone"
)
SOURCE_TABLE_NAME_MANAGEMENT_PLAN_MARINE_AREAS = (
    f"gold_{ENV}.area.management_plan_marine_areas"
)

TARGET_TABLE_NAME = f"gold_{ENV}.maru.dm_maru_report"

# COMMAND ----------

norway_timezone = pytz.timezone("Europe/Oslo")

# COMMAND ----------

create_table_if_not_exists(TARGET_TABLE_NAME)

# COMMAND ----------

df_vessel_in = spark.table(SOURCE_TABLE_NAME_SOURCE_MARU_VESSEL).select(
    "vessel_id",
    "statcode5",
    "gt_group",
    "gt",
    "fp_lengthbetweenperpendicularslbp",
    "fp_displacement",
    "fp_breadthextreme",
    "main_engine_fueltype",
    "vessel_type_maru",
    "start_date",
    "end_date",
)

df_vessel_combined_in = spark.table(SOURCE_TABLE_NAME_SOURCE_SHIPDATA_COMBINED).select(
    "csid", "shiptype_level5"
)

# COMMAND ----------

df_municipality = (
    spark.table(SOURCE_TABLE_NAME_MUNICIPALITY)
    .select("municipality_no", F.col("municipality_name_no").alias("municipality_name"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

df_county = (
    spark.table(SOURCE_TABLE_NAME_COUNTY)
    .select("fylkesnummer", F.col("navn_norsk").alias("navn"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

# COMMAND ----------

df_maritime_borders_norwegian_economic_zone = (
    spark.table(SOURCE_TABLE_NAME_MARITIME_BORDERS_NOR_EZ)
    .select("id", "area_name")
    .distinct()
)

df_management_plan_marine_areas = (
    spark.table(SOURCE_TABLE_NAME_MANAGEMENT_PLAN_MARINE_AREAS)
    .select("area_id", "area_name_norwegian")
    .distinct()
)

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

df_maru_report_max_date = spark.table(TARGET_TABLE_NAME).select(
    F.max("year_month").alias("max_year_month")
)

if df_maru_report_max_date.head(1):
    df_dates = get_processed_dates(SOURCE_TABLE_NAME_AIS_JOBS, df_maru_report_max_date)
    df_days_in_month = calculate_days_in_month(df_dates)
    df_complete_dates = check_complete_months(df_days_in_month)
    dates_list = [row[0] for row in df_complete_dates.collect()]

    if not dates_list:
        dbutils.notebook.exit("No periods to process.")

    periods = create_date_ranges(list_of_dates=dates_list, max_dates_per_range=90)

else:
    print(f"{TARGET_TABLE_NAME} is empty. Generating date ranges.")
    periods = generate_list_of_periods_to_process()

print(periods)

# COMMAND ----------

for list_of_dates in periods:
    start_date = list_of_dates[0]
    end_date = list_of_dates[1]

    current_time = datetime.now(norway_timezone)
    current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"{current_time}: Processing ", start_date, " to ", end_date)

    start_time = time.time()

    # Read MarU raw data
    df_maru_in = read_maru_raw_data(
        SOURCE_TABLE_NAME_SOURCE_MARU_RAW, start_date, end_date, MARU_RAW_VERSION
    )

    # Read AIS voyages data
    df_voyages_in = read_ais_voyages_data(
        SOURCE_TABLE_NAME_SOURCE_AIS_VOYAGES, start_date, end_date
    )

    df_maru_voyages = df_maru_in.join(
        df_voyages_in,
        on=(df_maru_in.mmsi == df_voyages_in.mmsi)
        & (df_maru_in.date_time_utc == df_voyages_in.date_time_utc),
        how="inner",
    ).drop(df_voyages_in.mmsi, df_voyages_in.date_time_utc)

    df_joined = merge_datasets(
        df_maru_voyages,
        df_vessel_in,
        df_vessel_combined_in,
        df_municipality,
        df_county,
        df_maritime_borders_norwegian_economic_zone,
        df_management_plan_marine_areas,
    )

    # Transform
    df_mun_emi = calculate_municipality_emission_type(df_joined)
    df_mun_voy = calculate_municipality_voyage_route(df_mun_emi)

    df_svalbard = set_svalbard_as_county(df_mun_voy)
    df_kwh_battery = set_kwh_battery(df_svalbard)
    df_report = generate_aggregated_report(df_kwh_battery)

    # Write
    df_report.write.mode("append").saveAsTable(TARGET_TABLE_NAME)

    print("Successfully wrote data until ", end_date)
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"Total time: {minutes} minutes and {seconds} seconds")

print("Finished.")
