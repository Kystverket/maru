from collections.abc import Sequence
from datetime import date

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.functions import min as spark_min
from pyspark.sql.window import Window


def _resolve_column_name(columns: Sequence[str], candidates: Sequence[str]) -> str:
    """Return the first available column name from a list of candidates."""
    for candidate in candidates:
        if candidate in columns:
            return candidate

    candidate_list = ", ".join(candidates)
    message = f"Could not find any of the expected columns: {candidate_list}"
    raise ValueError(message)


def read_dnv_engine_nox_rating(spark: SparkSession, env: str) -> DataFrame:
    """Read the lowest valid DNV engine NOx rating per IMO.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
    env : str
        Environment identifier (dev, test, prod).

    Returns:
    -------
    pyspark.sql.DataFrame
        DataFrame with columns ``dnv_imo`` (int) and
        ``dnv_engine_nox_rating`` (double).

    """
    dnv_table = f"silver_{env}.dnv_shipdata_epi_portal.vessel_data"
    df_dnv = spark.read.table(dnv_table)

    imo_col = _resolve_column_name(df_dnv.columns, ("imo_nr", "IMO_NR", "imo", "IMO"))
    rating_col = _resolve_column_name(
        df_dnv.columns,
        ("engine_nox_rating", "EngineNoxRating"),
    )

    return (
        df_dnv.withColumn(
            "_engine_nox_rating",
            col(rating_col).cast("double"),
        )
        .where(col(imo_col).isNotNull())
        .where(
            (col("_engine_nox_rating") > 0) & (col("_engine_nox_rating") < 60),
        )
        .groupBy(col(imo_col).cast("int").alias("dnv_imo"))
        .agg(spark_min("_engine_nox_rating").alias("dnv_engine_nox_rating"))
    )


def read_feature_values(
    spark: SparkSession, date_variables: date, table_name: str
) -> DataFrame:
    """Read feature variables to a dataframe.

    Filter the dataframe for date between df.start_date and df_end_date, so only active variables to be shown.
    The dataframe is also filtered on unique values, to account for duplicated data.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
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


def read_vessel_data(spark: SparkSession, table_name: str) -> DataFrame:
    """Read vessel data and extracts relevant columns.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
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
        "engine_nox_rating",
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
        "msg_type",
        "start_date",
        "end_date",
    ]

    df = df.select(columns)

    return df


def read_ais_voyages(
    spark: SparkSession, table_name: str, ais_from_date: date, ais_to_date: date
) -> DataFrame:
    """Read AIS data from a Delta file.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
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
        .where(
            (col("date_utc") >= ais_from_date)
            & (col("date_utc") <= ais_to_date)
            & (~col("flag_speed_spike_in_sail_id"))
        )
        .select(
            col("vessel_id_hash").alias("vessel_id"),
            "mmsi",
            "imo",
            "ship_name",
            "call_sign",
            "date_time_utc",
            "date_utc",
            "hex_14",
            "sail_id",
            "geometry",
            "speed_over_ground",
            "phase",
            "sail_time_remaining_seconds",
            "delta_previous_point_seconds",
            "distance_previous_point_meters",
            "in_coast_and_sea_area",
            "ais_public_data",
            "ship_is_stopped",
            "flag_speed_spike_in_sail_id",
            "msg_type",
        )
        .withColumns(
            {
                "hex_7": dbf.h3_toparent("hex_14", 7),
                "hex_9": dbf.h3_toparent("hex_14", 9),
            }
        )
    )

    return df
