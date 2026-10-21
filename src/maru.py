# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath("../../"))

# COMMAND ----------

from datetime import date, datetime
from zoneinfo import ZoneInfo

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
from pyspark.sql import functions as F
from utilities.job_stats.upsert_job_stats import upsert_ais_job_stats
from utilities.tools import (
    StopWatch,
    assert_name_contains,
    create_date_periods,
    prepare_job_stats_table_and_get_start_date,
    read_last_processed_date,
)
from utilities.transformers.common import add_processing_timestamp
from utilities.transformers.h3 import CLOSE_TO_POWER_CELL
from utilities.transformers.silver import (
    select_expression_from_table_and_prefix_columns,
)
from utils.config import DATE_VARIABLES, MARTRAF_TABLE_VERSION, VERSION
from utils.read_data import (
    create_variables_dictionary,
    read_ais_voyages,
    read_feature_values,
    read_vessel_data,
)
from utils.select_columns import select_maru_output_columns, select_vessel_columns

# COMMAND ----------

NORWAY_TIMEZONE = ZoneInfo("Europe/Oslo")

PROCESS_ALL_DATA = dbutils.widgets.get("process_all_data").lower() == "true"
RUN_AS_TEST_FROM_MARTRAF_TEST_DATA = dbutils.widgets.get(
    "run_as_test_from_martraf_test_data"
)
RUN_AS_TEST_FROM_MARTRAF_DATA = dbutils.widgets.get("run_as_test_from_martraf_data")
DEFAULT_START_DATE = dbutils.widgets.get("default_start_date")

# Period processed in test mode (run_as_test_from_martraf_*). Pass other dates
# as notebook parameters to test a specific window, e.g. a NOx area start date.
dbutils.widgets.text("test_start_date", "2024-01-01")
dbutils.widgets.text("test_end_date", "2024-03-31")
TEST_START_DATE = dbutils.widgets.get("test_start_date")
TEST_END_DATE = dbutils.widgets.get("test_end_date")

# COMMAND ----------

# MAGIC %md #Variables

# COMMAND ----------

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_MARTRAF_TABLE_FULL_PATH = f"gold_prod.martraf.martraf_{MARTRAF_TABLE_VERSION}"
SOURCE_VARIABLES_TABLE_FULL_PATH = f"gold_{ENV}.maru.emission_variables_{VERSION}"
SOURCE_VESSEL_TABLE_FULL_PATH = f"gold_{ENV}.maru.dim_vessel_historical_{VERSION}"
SOURCE_VESSEL_PORT_ELECTRIC_TABLE_FULL_PATH = (
    f"gold_{ENV}.maru.vessel_port_electric_{VERSION}"
)

SOURCE_VESSEL_TYPE_PORT_ELECTRIC_TABLE_FULL_PATH = (
    f"gold_{ENV}.maru.vessel_type_port_electric_{VERSION}"
)
TARGET_TABLE = f"fact_emission_{VERSION}"
TARGET_MARU_TABLE_FULL_PATH = f"gold_{ENV}.maru.{TARGET_TABLE}"
JOB_STATS_MARU_COLUMN = f"maru_{VERSION}"

AREAS = ["global", "eca"]
ENGINES = ["main_engine", "aux", "boiler"]
ENGINES_AUX_BOILER = ["aux", "boiler"]
EMISSIONS = ["bc", "ch4", "co", "co2", "n2o", "nmvoc", "nox", "pm10", "sox"]

AREA_TABLES = get_areas()

# COMMAND ----------

RUN_AS_TEST_FROM_MARTRAF_TEST_DATA = (
    RUN_AS_TEST_FROM_MARTRAF_TEST_DATA.lower() == "true"
)
RUN_AS_TEST_FROM_MARTRAF_DATA = RUN_AS_TEST_FROM_MARTRAF_DATA.lower() == "true"

if RUN_AS_TEST_FROM_MARTRAF_DATA and RUN_AS_TEST_FROM_MARTRAF_TEST_DATA:
    value_error_message = "Both 'run_as_test_from_martraf_data' and 'run_as_test_from_martraf_test_data' cannot be true at the same time."
    raise ValueError(value_error_message)


if RUN_AS_TEST_FROM_MARTRAF_TEST_DATA:
    SOURCE_MARTRAF_TABLE_FULL_PATH += "_test"


if RUN_AS_TEST_FROM_MARTRAF_TEST_DATA or RUN_AS_TEST_FROM_MARTRAF_DATA:
    print(f"Running in TEST mode for {TEST_START_DATE} to {TEST_END_DATE}.")
    if date.fromisoformat(TEST_START_DATE) > date.fromisoformat(TEST_END_DATE):
        value_error_message = (
            f"test_start_date {TEST_START_DATE} is after test_end_date {TEST_END_DATE}."
        )
        raise ValueError(value_error_message)
    VERSION = VERSION.replace(".", "_")
    TARGET_MARU_TABLE_FULL_PATH += "_test"

    assert_name_contains(name=TARGET_MARU_TABLE_FULL_PATH, required_substring="_test")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_MARU_TABLE_FULL_PATH}")

create_schema_and_table_if_not_exists(ENV, TARGET_MARU_TABLE_FULL_PATH)

# COMMAND ----------

# MAGIC %md #Read vessel data

# COMMAND ----------

# Read feature variable values to dataframe:
df_variables_in = read_feature_values(
    spark, DATE_VARIABLES, SOURCE_VARIABLES_TABLE_FULL_PATH
)

# Create variables dictionary from dataframe:
var = create_variables_dictionary(df_variables_in)

# COMMAND ----------

# Read vessel data
df_vessel_in = read_vessel_data(spark, SOURCE_VESSEL_TABLE_FULL_PATH)

# Filter so only vessels with MarU vessel type is present
df_vessel_filtered = df_vessel_in.where(
    (df_vessel_in["vessel_type_maru"].isNotNull())
    & (df_vessel_in["vessel_type_maru"] != "")
    & (df_vessel_in["vessel_type_maru"] != "Unknown")
)

# Vessel port electric
df_vessel_port_electric = spark.table(
    SOURCE_VESSEL_PORT_ELECTRIC_TABLE_FULL_PATH
).withColumn(
    "end_date", F.coalesce(F.col("end_date"), F.current_date()).alias("end_date")
)

# Vessel type port electric
df_vessel_type_port_electric = spark.table(
    SOURCE_VESSEL_TYPE_PORT_ELECTRIC_TABLE_FULL_PATH
)

# COMMAND ----------

df_vessel = set_emission_factors(df_vessel_filtered, var, ENGINES_AUX_BOILER)
df_vessel = set_consumption_factors(df_vessel, var)
df_vessel = set_sox_factor(df_vessel, var, ENGINES, AREAS)


# COMMAND ----------

df_vessel = select_vessel_columns(df_vessel)

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

if RUN_AS_TEST_FROM_MARTRAF_TEST_DATA or RUN_AS_TEST_FROM_MARTRAF_DATA:
    periods = [[TEST_START_DATE, TEST_END_DATE]]
else:
    earliest_allowed_processing_date = prepare_job_stats_table_and_get_start_date(
        env=ENV,
        source_table=f"martraf_{MARTRAF_TABLE_VERSION}",
        destination_table=JOB_STATS_MARU_COLUMN,
        earliest_allowed_processing_date=DEFAULT_START_DATE,
        process_all_data=PROCESS_ALL_DATA,
        destination_full_path=TARGET_MARU_TABLE_FULL_PATH,
    )

    if earliest_allowed_processing_date is None:
        print("No dates to process.")
        dbutils.notebook.exit("No dates to process")

    periods = create_date_periods(
        earliest_allowed_processing_date=earliest_allowed_processing_date,
        days_to_process=180,
        last_available_date=read_last_processed_date(
            env=ENV, source_table=f"martraf_{MARTRAF_TABLE_VERSION}"
        ),
    )


for list_of_dates in periods:
    start_date = list_of_dates[0]
    end_date = list_of_dates[1]

    current_time = datetime.now(NORWAY_TIMEZONE)
    current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"{current_time}: Processing ", start_date, " to ", end_date)

    stopwatch = StopWatch(start_date, end_date)
    stopwatch.start()

    # Read AIS data
    df_ais_in = read_ais_voyages(
        spark, SOURCE_MARTRAF_TABLE_FULL_PATH, start_date, end_date
    )

    # Merge AIS and vessel data
    df_merged = df_ais_in.join(
        F.broadcast(df_vessel),
        on=(
            (df_ais_in.vessel_id == df_vessel.vessel_id)
            # end_date is the next version's start_date, so the interval is
            # half-open. With <= a point on a version-change day matched both
            # versions, and the area join's row_number kept one of them by the
            # area ordering, not by the vessel version.
            & (F.to_date(df_ais_in.date_time_utc) >= df_vessel.start_date)
            & (F.to_date(df_ais_in.date_time_utc) < df_vessel.end_date)
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
        effective_from_column = area.get("effective_from_column")

        # Read area data:
        df_area = select_expression_from_table_and_prefix_columns(
            table_name=f"gold_prod.area.{table}",
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
            effective_from_column=effective_from_column,
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
    df_version = df_processingtime.withColumn("version", F.lit(VERSION))
    df_selected = select_maru_output_columns(df_version)

    # Write table
    df_selected.write.mode("append").saveAsTable(TARGET_MARU_TABLE_FULL_PATH)

    print("Successfully wrote data until ", end_date)
    stopwatch.stop()

    # Write AIS job statistics:

    df_dates_written = (
        spark.table(TARGET_MARU_TABLE_FULL_PATH)
        .where(F.to_date(F.col("date_time_utc")).between(start_date, end_date))
        .select(F.to_date("date_time_utc").alias("date_utc"), "bi_processing_time")
        .dropDuplicates(subset=["date_utc"])
    )

    if not (RUN_AS_TEST_FROM_MARTRAF_TEST_DATA or RUN_AS_TEST_FROM_MARTRAF_DATA):
        upsert_ais_job_stats(
            dataframe_updates=df_dates_written,
            target_column=JOB_STATS_MARU_COLUMN,
            updates_col_timestamp="bi_processing_time",
        )

print("Finished.")

# COMMAND ----------

spark.sql(f"OPTIMIZE {TARGET_MARU_TABLE_FULL_PATH}")
spark.sql(f"OPTIMIZE gold_{ENV}.ais.ais_job_stats")
spark.sql(f"OPTIMIZE gold_{ENV}.maru.dim_vessel_current_{VERSION}")
spark.sql(f"OPTIMIZE gold_{ENV}.maru.dim_vessel_historical_{VERSION}")
