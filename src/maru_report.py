# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

import time
from datetime import datetime

import pyspark.sql.functions as F
import pytz
from pyspark.sql import DataFrame, Window
from utilities.job_stats.upsert_job_stats import create_date_ranges
from utilities.transformers.common import add_processing_timestamp
from utils.config import MUNICIPALITY_CURRENT_YEAR, VERSION

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

MARU_RAW_VERSION = VERSION

ENV: str = os.getenv("ENVIRONMENT")

TABLE_NAME_SOURCE_MARU_RAW = f"gold_{ENV}.maru.fact_emission"
TABLE_NAME_SOURCE_AIS_VOYAGES = f"gold_{ENV}.ais.ais_voyages"
TABLE_NAME_SOURCE_MARU_VESSEL = f"gold_{ENV}.maru.dim_vessel_historical"
TABLE_NAME_SOURCE_SHIPDATA_COMBINED = (
    f"silver_{ENV}.shipdata_combined.combined_imputated"
)
TABLE_NAME_DESTINATION = f"gold_{ENV}.maru.dm_maru_report"
TABLE_NAME_AIS_JOBS = f"gold_{ENV}.ais.ais_job_stats"
TABLE_NAME_MUNICIPALITY = f"gold_{ENV}.area.municipality_historical"
TABLE_NAME_COUNTY = f"silver_{ENV}.administrative_enheter.fylke"
TABLE_NAME_MARITIME_BORDERS_NOR_EZ = (
    f"gold_{ENV}.area.maritime_borders_norwegian_economic_zone"
)
TABLE_NAME_MANAGEMENT_PLAN_MARINE_AREAS = (
    f"gold_{ENV}.area.management_plan_marine_areas"
)

# COMMAND ----------

norway_timezone = pytz.timezone("Europe/Oslo")

# COMMAND ----------

# MAGIC %md #Create table

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DESTINATION} (
  mmsi BIGINT COMMENT 'Maritime Mobile Service Identity',
  imo DOUBLE COMMENT 'International Maritime Organization number',
  vessel_id INT COMMENT 'Identifier for the vessel',
  shipname STRING COMMENT 'Name of the ship',
  statcode5 STRING COMMENT 'Statcode5 code',
  shiptypelevel5 STRING COMMENT 'Ship type level 5 classification',
  year STRING COMMENT 'Year of the report',
  month STRING COMMENT 'Month of the report',
  year_month STRING COMMENT 'Year and month of the report',
  gt_group STRING COMMENT 'Grouping by gross tonnage',
  gt DOUBLE COMMENT 'Gross tonnage',
  vessel_type STRING COMMENT 'Type of the vessel',
  degree_of_electrification DOUBLE COMMENT 'Degree of electrification',
  main_engine_fueltype STRING COMMENT 'Main engine fuel type',
  phase STRING COMMENT 'Phase of the voyage',
  voyage_type STRING COMMENT 'Type of the voyage',
  maritime_borders_norwegian_economic_zone_id STRING COMMENT 'Norwegian economic zone ID',
  maritime_borders_norwegian_economic_zone_area_name STRING COMMENT 'Norwegian economic zone area name',
  management_plan_marine_areas_area_id INT COMMENT 'Marine area management plan area ID',
  management_plan_marine_areas_area_name_norwegian STRING COMMENT 'Marine area management plan area name in Norwegian',
  municipality_id STRING COMMENT 'Municipality ID',
  municipality_name STRING COMMENT 'Municipality name',
  county_id STRING COMMENT 'County ID',
  county_name STRING COMMENT 'County name',
  municipality_voyage_type STRING COMMENT 'Municipality voyage type',
  in_coast_and_sea_area BOOLEAN COMMENT 'Indicator if in coast and sea area',
  in_norwegian_continental_shelf BOOLEAN COMMENT 'Indicator if in Norwegian continental shelf',
  sail_id STRING COMMENT 'Sail ID',
  municipality_voyage_route STRING COMMENT 'Municipality voyage route',
  version STRING COMMENT 'Version of the report',
  sum_seconds BIGINT COMMENT 'Sum of seconds',
  sum_kwh DOUBLE COMMENT 'Sum of kWh',
  sum_aux_kwh_shore_power DOUBLE COMMENT 'kWh og aux when consuming shore power',
  sum_boiler_kwh_shore_power DOUBLE COMMENT 'kWh of boiler when consuming shore power',
  sum_fuel DOUBLE COMMENT 'Sum of fuel',
  sum_co2 DOUBLE COMMENT 'Sum of CO2 emissions',
  sum_nmvoc DOUBLE COMMENT 'Sum of NMVOC emissions',
  sum_co DOUBLE COMMENT 'Sum of CO emissions',
  sum_ch4 DOUBLE COMMENT 'Sum of CH4 emissions',
  sum_n2o DOUBLE COMMENT 'Sum of N2O emissions',
  sum_sox DOUBLE COMMENT 'Sum of SOx emissions',
  sum_pm10 DOUBLE COMMENT 'Sum of PM10 emissions',
  sum_pm2_5 DOUBLE COMMENT 'Sum of PM2.5 emissions',
  sum_nox DOUBLE COMMENT 'Sum of NOx emissions',
  sum_bc DOUBLE COMMENT 'Sum of BC emissions',
  sum_co2e DOUBLE COMMENT 'Sum of CO2e emissions',
  distance_kilometers DOUBLE COMMENT 'Distance in kilometers',
  bi_processing_time TIMESTAMP COMMENT 'Last processing time'
)
COMMENT 'MarU report with aggregated data'

"""
)

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------


def read_maru_raw_data(
    table_name: str, from_date: str, to_date: str, maru_raw_version: str
) -> DataFrame:
    """
    Retrieve MARU raw data from a specified table within a date range and filter by version.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object.
    table_name : str
        The name of the source table.
    from_date : str
        The start date for filtering the date_time_utc column (inclusive).
    to_date : str
        The end date for filtering the date_time_utc column (inclusive).
    maru_raw_version : str
        The version number to filter the data.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing the filtered MARU raw data.
    """
    df = (
        spark.table(table_name)
        .select(
            "date_time_utc",
            "mmsi",
            "vessel_id",
            "sail_id",
            "phase",
            "maritime_borders_norwegian_economic_zone_id",
            "management_plan_marine_areas_area_id",
            "maritime_borders_norwegian_continental_shelf_id",
            "municipality_id",
            "in_coast_and_sea_area",
            "electric_shore_power_at_berth_reduction_factor",
            "main_engine_kwh",
            "aux_kwh",
            "boiler_kwh",
            "fuel_tonnes",
            "co2_tonnes",
            "nmvoc_tonnes",
            "co_tonnes",
            "ch4_tonnes",
            "n2o_tonnes",
            "sox_tonnes",
            "pm10_tonnes",
            "pm2_5_tonnes",
            "nox_tonnes",
            "bc_tonnes",
            "co2e_tonnes",
            "version",
        )
        .filter(
            (F.to_date(F.col("date_time_utc")) >= from_date)
            & (F.to_date(F.col("date_time_utc")) <= to_date)
        )
        .filter(F.col("version") == maru_raw_version)
        .withColumn("year", F.date_format("date_time_utc", "yyyy"))
        .withColumn(
            "county_id", F.left(F.col("municipality_id"), F.lit(2)).cast("string")
        )
        .withColumn(
            "aux_kwh_shore_power",
            F.col("aux_kwh")
            * (1 - F.col("electric_shore_power_at_berth_reduction_factor")),
        )
        .withColumn(
            "boiler_kwh_shore_power",
            F.col("boiler_kwh")
            * (1 - F.col("electric_shore_power_at_berth_reduction_factor")),
        )
        .withColumn(
            "in_norwegian_continental_shelf",
            F.when(
                F.col("maritime_borders_norwegian_continental_shelf_id").isNotNull(),
                True,
            ).otherwise(False),
        )
    )

    return df


# COMMAND ----------


def read_ais_voyages_data(table_name: str, from_date: str, to_date: str) -> DataFrame:
    """
    Retrieve MARU raw data from a specified table within a date range and filter by version.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object.
    table_name : str
        The name of the source table.
    from_date : str
        The start date for filtering the date_time_utc column (inclusive).
    to_date : str
        The end date for filtering the date_time_utc column (inclusive).

    Returns
    -------
    DataFrame
        A Spark DataFrame containing the filtered MARU raw data.
    """
    df = (
        spark.table(table_name)
        .select(
            "date_time_utc",
            "mmsi",
            "unlocode_country_code",
            "unlocode_location_code",
            "voyage_type",
            "delta_previous_point_seconds",
            "distance_previous_point_meters",
            "ship_is_stopped",
        )
        .filter(
            (F.to_date(F.col("date_time_utc")) >= from_date)
            & (F.to_date(F.col("date_time_utc")) <= to_date)
        )
    )

    return df


# COMMAND ----------

df_vessel_in = spark.table(TABLE_NAME_SOURCE_MARU_VESSEL).select(
    "vessel_id",
    "imo",
    "statcode5",
    "gt_group",
    "gt",
    "main_engine_fueltype",
    "degree_of_electrification",
    "vessel_type_maru",
    "start_date",
    "end_date",
)

df_vessel_combined_in = spark.table(TABLE_NAME_SOURCE_SHIPDATA_COMBINED).select(
    "csid", "shipname", "shiptypelevel5"
)

# COMMAND ----------

df_municipality = (
    spark.table(TABLE_NAME_MUNICIPALITY)
    .select("municipality_no", F.col("municipality_name_no").alias("municipality_name"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

df_county = (
    spark.table(TABLE_NAME_COUNTY)
    .select("fylkesnummer", F.col("navn_norsk").alias("navn"))
    .where(f"year == {MUNICIPALITY_CURRENT_YEAR}")
)

# COMMAND ----------

df_maritime_borders_norwegian_economic_zone = (
    spark.table(TABLE_NAME_MARITIME_BORDERS_NOR_EZ).select("id", "area_name").distinct()
)

df_management_plan_marine_areas = (
    spark.table(TABLE_NAME_MANAGEMENT_PLAN_MARINE_AREAS)
    .select("area_id", "area_name_norwegian")
    .distinct()
)

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------


def calculate_municipality_emission_type(df: DataFrame) -> DataFrame:
    """
    Calculate the municipality emission type based on the input DataFrame.

    Parameters:
    df (DataFrame): Input DataFrame containing the necessary columns.

    Returns:
    DataFrame: Modified DataFrame with the municipality emission type column added.
    """

    # Municipality emission type

    window_partitionby_sailid_orderby_datetime = (
        Window.partitionBy("sail_id")
        .orderBy("date_time_utc")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    window_partitionby_sailid_municipalityid = Window.partitionBy(
        "sail_id", "municipality_id"
    )

    window_partitionby_sailid = Window.partitionBy("sail_id")

    df = (
        # Generate columns for calculation
        df.withColumn(
            "municipality_id_start",
            F.first("municipality_id").over(window_partitionby_sailid_orderby_datetime),
        )
        .withColumn(
            "municipality_id_end",
            F.last("municipality_id").over(window_partitionby_sailid_orderby_datetime),
        )
        .withColumn("sail_id_count", F.count("*").over(window_partitionby_sailid))
        .withColumn(
            "sail_id_municipality_count",
            F.count("*").over(window_partitionby_sailid_municipalityid),
        )
        # Set municipality emission type
        .withColumn(
            "municipality_voyage_type",
            F.when(F.col("ship_is_stopped"), "Berthed")
            .when(
                F.col("sail_id_municipality_count") / F.col("sail_id_count") == 1,
                "Local",
            )
            .when(
                (F.col("municipality_id_start") == F.col("municipality_id"))
                | (F.col("municipality_id_end") == F.col("municipality_id")),
                "Departure or destination",
            )
            .otherwise(F.lit("Transit")),
        )
        .drop(
            "sail_id_count",
            "sail_id_municipality_count",
        )
    )

    return df


# COMMAND ----------


def calculate_municipality_voyage_route(df: DataFrame) -> DataFrame:
    """
    Calculates the municipality voyage route based on the first and last municipality for each Sail ID.
    The names are sorted alphabetically before they are concatenated by a dash (-) and stored as the municiapality_voyage_route column. Note, due to the alphabetically order, the start municiapality may be both on the left and right side of the dash.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing columns for sail ID, municipality names, and municipality IDs.

    Returns
    -------
    DataFrame
        Modified DataFrame with added columns for start and end municipality names and the municipality voyage route.
    """
    window_partitionby_sailid_orderby_datetime = (
        Window.partitionBy("sail_id")
        .orderBy("date_time_utc")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = (
        df.withColumn(
            "municipality_name_start",
            F.first("municipality_name").over(
                window_partitionby_sailid_orderby_datetime
            ),
        )
        .withColumn(
            "municipality_name_end",
            F.last("municipality_name").over(
                window_partitionby_sailid_orderby_datetime
            ),
        )
        .withColumn(
            "municipality_voyage_route",
            F.when(
                (F.col("municipality_id_start").isNotNull())
                & (F.col("municipality_id_end").isNotNull())
                & (F.col("municipality_id_start") != F.col("municipality_id_end")),
                F.concat_ws(
                    " - ",
                    F.array_sort(
                        F.array(
                            F.col("municipality_name_start"),
                            F.col("municipality_name_end"),
                        )
                    ),
                ),
            ),
        )
    )

    return df


# COMMAND ----------


def merge_datasets(df_maru: DataFrame) -> DataFrame:
    """
    Merge multiple datasets into a single DataFrame.

    Parameters
    ----------
    df_maru : DataFrame
        The MarU raw DataFrame.

    Returns
    -------
    DataFrame
        A merged DataFrame containing data from all input DataFrames.
    """

    df = (
        df_maru.join(
            F.broadcast(df_vessel_in),
            on=(
                (df_maru.vessel_id == df_vessel_in.vessel_id)
                & (F.to_date(df_maru.date_time_utc) >= df_vessel_in.start_date)
                & (F.to_date(df_maru.date_time_utc) <= df_vessel_in.end_date)
            ),
            how="left",
        )
        .join(
            F.broadcast(df_vessel_combined_in),
            on=(df_vessel_combined_in.csid == df_maru.vessel_id),
            how="left",
        )
        .join(
            F.broadcast(df_municipality),
            on=(df_maru.municipality_id == df_municipality.municipality_no),
            how="left",
        )
        .join(
            F.broadcast(df_county),
            on=(df_maru.county_id == df_county.fylkesnummer),
            how="left",
        )
        .join(
            df_maritime_borders_norwegian_economic_zone,
            on=df_maru.maritime_borders_norwegian_economic_zone_id
            == df_maritime_borders_norwegian_economic_zone.id,
            how="left",
        )
        .join(
            df_management_plan_marine_areas,
            on=df_maru.management_plan_marine_areas_area_id
            == df_management_plan_marine_areas.area_id,
            how="left",
        )
        .select(
            df_maru["*"],
            df_vessel_in["*"],
            df_vessel_combined_in["shipname"],
            df_vessel_combined_in["shiptypelevel5"],
            df_municipality["municipality_name"],
            df_county["navn"].alias("county_name"),
            df_maritime_borders_norwegian_economic_zone["area_name"].alias(
                "maritime_borders_norwegian_economic_zone_area_name"
            ),
            df_management_plan_marine_areas["area_name_norwegian"].alias(
                "management_plan_marine_areas_area_name_norwegian"
            ),
        )
        .drop(df_vessel_in["vessel_id"])
    )

    return df


# COMMAND ----------


def set_svalbard_as_county(df: DataFrame) -> DataFrame:
    """
    Set the county and municipality to "Svalbard" for specific maritime border IDs (territorial area around Svalbard).

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with maritime border IDs and county names.

    Returns
    -------
    DataFrame
        The DataFrame with updated county names where applicable.
    """
    df = df.withColumn(
        "county_name",
        F.when(
            F.col("maritime_borders_norwegian_economic_zone_id").isin(
                "431ed9a6-bfcb-41b2-99fe-ef9ac0ce49df",
                "b5336246-a11e-4311-85b4-37da96598bbe",
                "5b58a87b-58f2-4bd7-954f-46741c066405",
                "5d48c1fa-2178-46ab-97af-6ba2e6e54048",
            ),
            "Svalbard",
        ).otherwise(F.col("county_name")),
    )

    df = df.withColumn(
        "county_id",
        F.when(F.col("county_name") == "Svalbard", "-99").otherwise(F.col("county_id")),
    )

    df = df.withColumn(
        "municipality_name",
        F.when(F.col("county_name") == "Svalbard", "Svalbard").otherwise(
            F.col("municipality_name")
        ),
    )

    df = df.withColumn(
        "municipality_id",
        F.when(F.col("county_name") == "Svalbard", "-9999").otherwise(
            F.col("municipality_id")
        ),
    )

    return df


# COMMAND ----------

# MAGIC %md ## Aggregations

# COMMAND ----------


def generate_aggregated_report(df: DataFrame) -> DataFrame:
    """
    Generate an aggregated report from the joined dataset.

    Parameters
    ----------
    df : DataFrame
        The joined DataFrame containing vessel and maritime data.

    Returns
    -------
    DataFrame
        An aggregated DataFrame with calculated metrics and processing timestamp.
    """

    df = df.groupBy(
        "mmsi",
        "imo",
        "vessel_id",
        "shipname",
        "statcode5",
        "shiptypelevel5",
        "year",
        F.date_format("date_time_utc", "MM").alias("month"),
        F.date_format("date_time_utc", "yyyy-MM").alias("year_month"),
        "gt_group",
        "gt",
        F.col("vessel_type_maru").alias("vessel_type"),
        "degree_of_electrification",
        "main_engine_fueltype",
        "sail_id",
        "phase",
        "voyage_type",
        "maritime_borders_norwegian_economic_zone_id",
        "maritime_borders_norwegian_economic_zone_area_name",
        "management_plan_marine_areas_area_id",
        "management_plan_marine_areas_area_name_norwegian",
        "municipality_id",
        "municipality_name",
        "county_id",
        "county_name",
        "municipality_voyage_type",
        "in_coast_and_sea_area",
        "in_norwegian_continental_shelf",
        "municipality_voyage_route",
        "version",
    ).agg(
        F.round(F.sum("delta_previous_point_seconds"), 2).alias("sum_seconds"),
        F.round(
            F.sum("main_engine_kwh") + F.sum("aux_kwh") + F.sum("boiler_kwh"), 4
        ).alias("sum_kwh"),
        F.round(F.sum("aux_kwh_shore_power"), 4).alias("sum_aux_kwh_shore_power"),
        F.round(F.sum("boiler_kwh_shore_power"), 4).alias("sum_boiler_kwh_shore_power"),
        F.sum("fuel_tonnes").alias("sum_fuel"),
        F.sum("co2_tonnes").alias("sum_co2"),
        F.sum("nmvoc_tonnes").alias("sum_nmvoc"),
        F.sum("co_tonnes").alias("sum_co"),
        F.sum("ch4_tonnes").alias("sum_ch4"),
        F.sum("n2o_tonnes").alias("sum_n2o"),
        F.sum("sox_tonnes").alias("sum_sox"),
        F.sum("pm10_tonnes").alias("sum_pm10"),
        F.sum("pm2_5_tonnes").alias("sum_pm2_5"),
        F.sum("nox_tonnes").alias("sum_nox"),
        F.sum("bc_tonnes").alias("sum_bc"),
        F.sum("co2e_tonnes").alias("sum_co2e"),
        F.round((F.sum("distance_previous_point_meters") / 1000), 2).alias(
            "distance_kilometers"
        ),
    )

    # Add processing time
    df = add_processing_timestamp(df)

    return df


# COMMAND ----------

# MAGIC %md #Get dates to process

# COMMAND ----------


def get_processed_dates(table_name: str, max_year_month_df: DataFrame) -> DataFrame:
    """
    Get processed MarU raw dates from ais job stats newer than the max year month.

    Parameters
    ----------
    table_name : str
        The name of the table to query.
    max_year_month_df : DataFrame
        A DataFrame containing the maximum year_month.

    Returns
    -------
    DataFrame
        A DataFrame containing the processed dates.
    """
    return (
        spark.table(table_name)
        .filter(F.col("maru").isNotNull())
        .join(
            max_year_month_df,
            on=(F.date_format(F.col("date_utc"), "yyyy-MM") > F.col("max_year_month")),
            how="inner",
        )
        .select(
            "date_utc",
            F.month("date_utc").alias("month"),
            F.year("date_utc").alias("year"),
        )
    )


def calculate_days_in_month(df: DataFrame) -> DataFrame:
    """
    Calculate the number of days in each month and the start and end dates.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with date information.

    Returns
    -------
    DataFrame
        A DataFrame with the number of days in each month and the start and end dates.
    """
    return df.groupBy("year", "month").agg(
        F.countDistinct(F.dayofmonth("date_utc")).alias("days_count"),
        F.min(F.col("date_utc")).alias("start_date"),
        F.max(F.col("date_utc")).alias("end_date"),
    )


def check_complete_months(df: DataFrame) -> DataFrame:
    """
    Check if the dates form a complete month, filter to only keep completed months and explode the date range.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with date information.

    Returns
    -------
    DataFrame
        A DataFrame with individual dates for complete months.
    """
    return (
        df.withColumn(
            "expected_days",
            F.when(F.col("month").isin([1, 3, 5, 7, 8, 10, 12]), 31)
            .when(F.col("month").isin([4, 6, 9, 11]), 30)
            .when(
                (F.col("month") == 2)
                & (
                    (F.col("year") % 4 == 0)
                    & ((F.col("year") % 100 != 0) | (F.col("year") % 400 == 0))
                ),
                29,
            )
            .otherwise(28),
        )
        .withColumn("is_complete_month", F.col("days_count") >= F.col("expected_days"))
        .filter(F.col("is_complete_month") == True)
        .withColumn(
            "date_range", F.expr("sequence(start_date, end_date, interval 1 day)")
        )
        .select("date_range", F.explode("date_range").alias("date_utc"))
        .drop("date_range")
    )


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

# MAGIC %md #Write data

# COMMAND ----------

df_maru_report_max_date = spark.table(TABLE_NAME_DESTINATION).select(
    F.max("year_month").alias("max_year_month")
)
df_dates = get_processed_dates(TABLE_NAME_AIS_JOBS, df_maru_report_max_date)
df_days_in_month = calculate_days_in_month(df_dates)
df_complete_dates = check_complete_months(df_days_in_month)
dates_list = [row[0] for row in df_complete_dates.collect()]

if not dates_list:
    dbutils.notebook.exit("No periods to process.")

periods = create_date_ranges(list_of_dates=dates_list, max_dates_per_range=90)
print(periods)

# Can be used when processing several years:
# periods = generate_list_of_periods_to_process([])

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
        TABLE_NAME_SOURCE_MARU_RAW, start_date, end_date, MARU_RAW_VERSION
    )

    # Read AIS voyages data
    df_voyages_in = read_ais_voyages_data(
        TABLE_NAME_SOURCE_AIS_VOYAGES, start_date, end_date
    )

    df_maru_voyages = df_maru_in.join(
        df_voyages_in,
        on=(df_maru_in.mmsi == df_voyages_in.mmsi)
        & (df_maru_in.date_time_utc == df_voyages_in.date_time_utc),
        how="inner",
    ).drop(df_voyages_in.mmsi, df_voyages_in.date_time_utc)

    df_joined = merge_datasets(df_maru_voyages)

    # Transform
    df_mun_emi = calculate_municipality_emission_type(df_joined)
    df_mun_voy = calculate_municipality_voyage_route(df_mun_emi)

    df_svalbard = set_svalbard_as_county(df_mun_voy)
    df_report = generate_aggregated_report(df_svalbard)

    # Write
    df_report.write.mode("append").saveAsTable(TABLE_NAME_DESTINATION)

    print("Successfully wrote data until ", end_date)
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"Total time: {minutes} minutes and {seconds} seconds")

print("Finished.")
