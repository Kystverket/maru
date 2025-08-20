from datetime import date

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


def read_feature_values(date_variables: date, table_name: str) -> DataFrame:
    """Read feature variables to a dataframe.

    Filter the dataframe for date between df.start_date and df_end_date, so only active variables to be shown.
    The dataframe is also filtered on unique values, to account for duplicated data.

    Parameters
    ----------
    date_variables: date
        Cut-off date for variables. Each variable has a start_date and end_date.

    table_name: str
        Full table name

    Returns:
    -------
    pyspark.sql.DataFrame
        A DataFrame containing variables and value.

    """
    df = spark.table(table_name)

    # Filter records for input date
    df = df.where(
        (df["start_date"] <= date_variables)
        & ((df["end_date"] >= date_variables) | (df["end_date"].isNull()))
    )

    # Keep only one record per variable:
    w = Window().partitionBy("variable").orderBy(desc("start_date"))
    df = df.withColumn("row_number", row_number().over(w))
    df = df.where(df["row_number"] == 1).drop(df["row_number"])

    return df


def create_variables_dictionary(df: DataFrame) -> DataFrame:
    """Create a dictionary of variables and their corresponding values from a DataFrame.

    Parameters
    ----------
    df: pyspark.sql.dataframe.DataFrame
        A DataFrame containing 'variable' and 'value' columns.

    Returns:
    -------
    var: dict
        A dictionary where keys are variable names and values are their corresponding values.

    """
    var = {}
    for row in df.collect():
        var.update({row["variable"]: row["value"]})

    return var


def read_vessel_data(table_name: str) -> DataFrame:
    """Read vessel data and extracts relevant columns.

    Parameters
    ----------
    table_name: str
        Full table name (catalog.schema.table)

    Returns:
    -------
    pyspark.sql.dataframe.DataFrame
        A DataFrame with the relevant vessel data.

    """
    df = spark.read.table(table_name)

    columns = [
        "vessel_id",
        "mmsi",
        "vessel_type_maru",
        "main_engine_fueltype",
        "main_engine_enginetype",
        "year_of_build",
        "tier_group_nox",
        "main_engine_rpm",
        "speed_service",
        "stroketype",
        "main_engine_kw",
        "gt",
        "dwt",
        "teu",
        "cbm",
        "degree_of_electrification",
        "aux_berth_kw",
        "aux_anchor_kw",
        "aux_maneuver_kw",
        "aux_cruise_kw",
        "boiler_berth_kw",
        "boiler_anchor_kw",
        "boiler_maneuver_kw",
        "boiler_cruise_kw",
        "start_date",
        "end_date",
    ]

    df = df.select(columns)

    return df


def read_ais_voyages(
    table_name: str, ais_from_date: date, ais_to_date: date
) -> DataFrame:
    """Read AIS data from a Delta file.

    Parameters
    ----------
    table_name: str
        Full table name (catalog.schema.table)

    ais_from_date: date
        The starting date/time for the filter to be applied on date_time_utc in the format 'YYYY-MM-DD HH:mm:ss'.

    ais_to_date: date
        The ending date/time for the filter to be applied on date_time_utc in the format 'YYYY-MM-DD HH:mm:ss'.

    Returns:
    -------
    pyspark.sql.DataFrame
         A DataFrame with the selected AIS data.

    """
    df = (
        spark.read.table(table_name)
        .where((col("date_utc") >= ais_from_date) & (col("date_utc") <= ais_to_date))
        .select(
            "vessel_id",
            "mmsi",
            "imo",
            "ship_name",
            "call_sign",
            "date_time_utc",
            "date_utc",
            "hex_14",
            "sail_id",
            "geometry_wkt",
            "speed_over_ground",
            "phase",
            "sail_time_remaining_seconds",
            col("delta_previous_point_seconds")
            .cast("bigint")
            .alias("delta_previous_point_seconds"),
            "distance_previous_point_meters",
            "in_coast_and_sea_area",
            "ais_public_data",
            "ship_is_stopped",
            "msg_type",
        )
        .withColumn("hex_7", dbf.h3_toparent("hex_14", 7))
        .withColumn("hex_9", dbf.h3_toparent("hex_14", 9))
    )

    return df
