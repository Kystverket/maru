# Databricks notebook source
import os
import sys

sys.path.append("/Workspace/")

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utilities.transformers.common import add_processing_timestamp

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

MARU_RAW_VERSION = "v1.1.0"

ENV: str = os.getenv("ENVIRONMENT")

TABLE_NAME_SOURCE_MARU_RAW = f"gold_{ENV}.maru.maru_raw"
TABLE_NAME_SOURCE_MARU_VESSEL = f"gold_{ENV}.maru.vessel"
TABLE_NAME_SOURCE_SHIPDATA_COMBINED = (
    f"silver_{ENV}.shipdata_combined.combined_imputated"
)
TABLE_NAME_DESTINATION = f"gold_{ENV}.maru.maru_report"

# To be changed (DATA-335)
PATH_SOURCE_COUNTY = f"abfss://raw@kyvdatalakehouse{ENV}.dfs.core.windows.net/maru/county/maru_county.csv"

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

df_maru_raw_in = (
    spark.table(TABLE_NAME_SOURCE_MARU_RAW)
    .select(
        "date_time_utc",
        "mmsi",
        "vessel_id",
        "sail_id",
        "phase",
        "municipality_name",
        "maritime_borders_norwegian_economic_zone_id",
        "maritime_borders_norwegian_economic_zone_area_name",
        "management_plan_marine_areas_area_id",
        "management_plan_marine_areas_area_name_norwegian",
        "municipality_id",
        "municipality_name",
        F.left(F.col("municipality_id"), F.lit(2)).alias("county_id").cast("String"),
        "unlocode_country_code",
        "unlocode_location_code",
        "voyage_type",
        "in_coast_and_sea_area",
        "in_norwegian_continental_shelf",
        "delta_previous_point_seconds",
        "main_engine_kwh",
        "aux_kwh",
        "boiler_kwh",
        "fuel_ton",
        "co2_ton",
        "nmvoc_ton",
        "co_ton",
        "ch4_ton",
        "n2o_ton",
        "sox_ton",
        "pm10_ton",
        "pm2_5_ton",
        "nox_ton",
        "bc_ton",
        "co2e_ton",
        "distance_previous_point_meters",
        "ship_is_stopped",
        "version",
    )
    .filter(F.col("version") == MARU_RAW_VERSION)
)

df_vessel_in = spark.table(TABLE_NAME_SOURCE_MARU_VESSEL).select(
    "vessel_id",
    "imo",
    "statcode5",
    "gt_group",
    "gt",
    "main_engine_fueltype",
    "degree_of_electrification",
    "vessel_type_maru",
)

df_vessel_combined_in = spark.table(TABLE_NAME_SOURCE_SHIPDATA_COMBINED).select(
    "csid", "shipname", "shiptypelevel5"
)

# COMMAND ----------

# To be changed (DATA-335)
df_county = spark.read.csv(
    PATH_SOURCE_COUNTY,
    sep=";",
    header="true",
)

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------

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

df_maru = (
    # Generate columns for calculation
    df_maru_raw_in.withColumn(
        "municipality_start",
        F.first("municipality_id").over(window_partitionby_sailid_orderby_datetime),
    )
    .withColumn(
        "municipality_end",
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
            (F.col("municipality_start") == F.col("municipality_id"))
            | (F.col("municipality_end") == F.col("municipality_id")),
            "Departure or destination",
        )
        .otherwise(F.lit("Transit")),
    )
    .drop(
        "municipality_start",
        "municipality_end",
        "sail_id_count",
        "sail_id_municipality_count",
    )
)

# COMMAND ----------

# Merge all datasets
df_joined = (
    df_maru.join(df_vessel_in, on="vessel_id", how="left")
    .join(
        df_vessel_combined_in,
        on=(df_vessel_combined_in.csid == df_maru.vessel_id),
        how="left",
    )
    .join(df_county, on=(df_maru.county_id == df_county.fylkesnummer), how="left")
    .select(
        df_maru["*"],
        df_vessel_in["*"],
        df_vessel_combined_in["shipname"],
        df_vessel_combined_in["shiptypelevel5"],
        df_county["navn"].alias("county_name"),
    )
    .drop(df_vessel_in["vessel_id"])
)

# COMMAND ----------

# MAGIC %md ## Aggregations

# COMMAND ----------

# Report:

df_aggregated = df_joined.groupBy(
    "mmsi",
    "imo",
    "vessel_id",
    "shipname",
    "statcode5",
    "shiptypelevel5",
    F.date_format("date_time_utc", "yyyy").alias("year"),
    F.date_format("date_time_utc", "MM").alias("month"),
    F.date_format("date_time_utc", "yyyy-MM").alias("year_month"),
    "gt_group",
    "gt",
    F.col("vessel_type_maru").alias("vessel_type"),
    "degree_of_electrification",
    "main_engine_fueltype",
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
    "unlocode_country_code",
    "unlocode_location_code",
    "in_coast_and_sea_area",
    "in_norwegian_continental_shelf",
    "version",
).agg(
    F.round(F.sum("delta_previous_point_seconds"), 2).alias("sum_seconds"),
    F.round(F.sum("main_engine_kwh") + F.sum("aux_kwh") + F.sum("boiler_kwh"), 4).alias(
        "sum_kwh"
    ),
    F.round(F.sum("fuel_ton"), 4).alias("sum_fuel"),
    F.round(F.sum("co2_ton"), 4).alias("sum_co2"),
    F.round(F.sum("nmvoc_ton"), 4).alias("sum_nmvoc"),
    F.round(F.sum("co_ton"), 4).alias("sum_co"),
    F.round(F.sum("ch4_ton"), 4).alias("sum_ch4"),
    F.round(F.sum("n2o_ton"), 4).alias("sum_n2o"),
    F.round(F.sum("sox_ton"), 4).alias("sum_sox"),
    F.round(F.sum("pm10_ton"), 4).alias("sum_pm10"),
    F.round(F.sum("pm2_5_ton"), 4).alias("sum_pm2_5"),
    F.round(F.sum("nox_ton"), 4).alias("sum_nox"),
    F.round(F.sum("bc_ton"), 4).alias("sum_bc"),
    F.round(F.sum("co2e_ton"), 4).alias("sum_co2e"),
    F.round((F.sum("distance_previous_point_meters") / 1000), 2).alias(
        "distance_kilometers"
    ),
)

# Add processing time
df_report = add_processing_timestamp(df_aggregated)

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

df_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    TABLE_NAME_DESTINATION
)
