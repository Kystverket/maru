# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))
sys.path.append(os.path.abspath("../../"))

from zoneinfo import ZoneInfo

import pyspark.sql.functions as F
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
from utilities.tools import StopWatch, assert_name_contains
from utils.config import (
    MARTRAF_TABLE_VERSION,
    MUNICIPALITY_CURRENT_YEAR,
    VERSION,
)

# COMMAND ----------

dbutils.widgets.text("run_as_test_from_maru_test_data", "")
dbutils.widgets.text("run_as_test_from_maru_data", "")
dbutils.widgets.text("process_all_data", "")
RUN_AS_TEST_FROM_MARU_TEST_DATA = dbutils.widgets.get("run_as_test_from_maru_test_data")
RUN_AS_TEST_FROM_MARU_DATA = dbutils.widgets.get("run_as_test_from_maru_data")
PROCESS_ALL_DATA = dbutils.widgets.get("process_all_data").lower() == "true"

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_MARU_RAW_TABLE_FULL_PATH = f"gold_{ENV}.maru.fact_emission_{VERSION}"
SOURCE_AIS_VOYAGES_TABLE_FULL_PATH = (
    f"gold_prod.martraf.martraf_{MARTRAF_TABLE_VERSION}"
)
SOURCE_MARU_VESSEL_TABLE_FULL_PATH = f"gold_{ENV}.maru.dim_vessel_historical_{VERSION}"
SOURCE_SHIPDATA_COMBINED_TABLE_FULL_PATH = "gold_prod.ais.qsr_shipdata_combined_scd2_v3"
SOURCE_AIS_JOBS_TABLE_FULL_PATH = f"gold_{ENV}.ais.ais_job_stats"
SOURCE_MUNICIPALITY_TABLE_FULL_PATH = "gold_prod.area.municipality_historical"
SOURCE_COUNTY_TABLE_FULL_PATH = "silver_prod.administrative_enheter.fylke"
SOURCE_MARITIME_BORDERS_NOR_EZ_TABLE_FULL_PATH = (
    "gold_prod.area.maritime_borders_norwegian_economic_zone"
)
SOURCE_MANAGEMENT_PLAN_MARINE_AREAS_TABLE_FULL_PATH = (
    "gold_prod.area.management_plan_marine_areas"
)

TARGET_TABLE_FULL_PATH = f"gold_{ENV}.maru.dm_maru_report_{VERSION}"

# COMMAND ----------

RUN_AS_TEST_FROM_MARU_TEST_DATA = RUN_AS_TEST_FROM_MARU_TEST_DATA.lower() == "true"
RUN_AS_TEST_FROM_MARU_DATA = RUN_AS_TEST_FROM_MARU_DATA.lower() == "true"

if RUN_AS_TEST_FROM_MARU_DATA and RUN_AS_TEST_FROM_MARU_TEST_DATA:
    value_error_message = "Both 'run_as_test_from_maru_data' and 'run_as_test_from_maru_test_data' cannot be true at the same time."
    raise ValueError(value_error_message)

if RUN_AS_TEST_FROM_MARU_TEST_DATA:
    SOURCE_MARU_RAW_TABLE_FULL_PATH += "_test"

if RUN_AS_TEST_FROM_MARU_TEST_DATA or RUN_AS_TEST_FROM_MARU_DATA:
    print("Running in TEST mode.")
    TARGET_TABLE_FULL_PATH += "_test"
    assert_name_contains(name=TARGET_TABLE_FULL_PATH, required_substring="_test")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_FULL_PATH}")

# COMMAND ----------

NORWAY_TIMEZONE = ZoneInfo("Europe/Oslo")

# COMMAND ----------

create_table_if_not_exists(TARGET_TABLE_FULL_PATH)

# COMMAND ----------

df_vessel_in = spark.table(SOURCE_MARU_VESSEL_TABLE_FULL_PATH).select(
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

df_vessel_combined_in = spark.table(SOURCE_SHIPDATA_COMBINED_TABLE_FULL_PATH).select(
    F.col("vessel_id_hash").alias("vessel_id"),
    "shiptype_level5",
    "__START_AT",
    "__END_AT",
)

# COMMAND ----------

df_municipality = (
    spark.table(SOURCE_MUNICIPALITY_TABLE_FULL_PATH)
    .select("municipality_no", F.col("municipality_name_no").alias("municipality_name"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

df_county = (
    spark.table(SOURCE_COUNTY_TABLE_FULL_PATH)
    .select("fylkesnummer", F.col("navn_norsk").alias("navn"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

# COMMAND ----------

df_maritime_borders_norwegian_economic_zone = (
    spark.table(SOURCE_MARITIME_BORDERS_NOR_EZ_TABLE_FULL_PATH)
    .select("id", "area_name")
    .distinct()
)

df_management_plan_marine_areas = (
    spark.table(SOURCE_MANAGEMENT_PLAN_MARINE_AREAS_TABLE_FULL_PATH)
    .select("area_id", "area_name_norwegian")
    .distinct()
)

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

if RUN_AS_TEST_FROM_MARU_TEST_DATA or RUN_AS_TEST_FROM_MARU_DATA:
    periods = [
        ["2024-01-01", "2024-03-31"],
        # ["2024-04-01", "2024-06-30"],
    ]
elif PROCESS_ALL_DATA:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_FULL_PATH}")
    create_table_if_not_exists(TARGET_TABLE_FULL_PATH)
    periods = generate_list_of_periods_to_process()

else:
    df_maru_report_max_date = spark.table(TARGET_TABLE_FULL_PATH).select(
        F.max("year_month").alias("max_year_month")
    )

    row = df_maru_report_max_date.first()
    max_year_month = row["max_year_month"] if row else None

    if max_year_month is not None:
        df_dates = get_processed_dates(
            SOURCE_AIS_JOBS_TABLE_FULL_PATH, max_year_month, f"maru_{VERSION}"
        )
        df_days_in_month = calculate_days_in_month(df_dates)
        df_complete_dates = check_complete_months(df_days_in_month)
        dates_list = [row[0] for row in df_complete_dates.collect()]

        if not dates_list:
            dbutils.notebook.exit("No periods to process.")

        periods = create_date_ranges(list_of_dates=dates_list, max_dates_per_range=90)

    else:
        print(f"{TARGET_TABLE_FULL_PATH} is empty. Generating date ranges.")
        periods = generate_list_of_periods_to_process()

print(periods)

# COMMAND ----------

for list_of_dates in periods:
    start_date = list_of_dates[0]
    end_date = list_of_dates[1]

    stopwatch = StopWatch(start_date, end_date)
    stopwatch.start()

    # Read MarU raw data
    df_maru_in = read_maru_raw_data(
        SOURCE_MARU_RAW_TABLE_FULL_PATH, start_date, end_date
    )

    # Read AIS voyages data
    df_voyages_in = read_ais_voyages_data(
        SOURCE_AIS_VOYAGES_TABLE_FULL_PATH, start_date, end_date
    )

    df_maru_voyages = df_maru_in.join(
        df_voyages_in,
        on=(df_maru_in.vessel_id == df_voyages_in.vessel_id)
        & (df_maru_in.date_time_utc == df_voyages_in.date_time_utc),
        how="inner",
    ).drop(df_voyages_in.vessel_id, df_voyages_in.mmsi, df_voyages_in.date_time_utc)

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
    df_report.write.mode("append").saveAsTable(TARGET_TABLE_FULL_PATH)

    print("Successfully wrote data until ", end_date)
    stopwatch.stop()

print("Finished.")

# COMMAND ----------

spark.sql(f"OPTIMIZE {TARGET_TABLE_FULL_PATH}")
