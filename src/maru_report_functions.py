from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from utilities.transformers.common import add_processing_timestamp

spark = SparkSession.getActiveSession()


def create_table_if_not_exists(target_table: str) -> None:
    """Creates the table in Spark if it does not already exist.

    Args:
        target_table (str): Fully qualified table name (including schema) to be created.

    Returns:
        None
    """
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
    year SMALLINT COMMENT 'AIS timestamp casted to year',
    month TINYINT COMMENT 'AIS timestamp casted to month',
    year_month STRING COMMENT 'AIS timestamp casted to year-month',
    vessel_id INT COMMENT 'Identifier for the vessel',
    mmsi INT COMMENT 'Maritime Mobile Service Identity',
    imo INT COMMENT 'International Maritime Organization number - a unique ship identifier',
    ship_name STRING COMMENT 'Name of the ship',
    call_sign STRING COMMENT 'Call sign of the ship',
    statcode5 STRING COMMENT 'Statcode5 code',
    shiptype_level5 STRING COMMENT 'Ship type level 5 classification',
    gt_group STRING COMMENT 'Grouping by gross tonnage',
    gt FLOAT COMMENT 'Gross tonnage',
    vessel_type STRING COMMENT 'Type of the vessel',
    usage_percentage_vessel_port_electric DOUBLE COMMENT 'Percentage of shore power usage',
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
    shore_power_is_connected BOOLEAN COMMENT 'Indicator if the vessel is connected and utilizing shore power',
    sail_id BIGINT COMMENT 'Sail ID',
    municipality_voyage_route STRING COMMENT 'Municipality voyage route',
    version STRING COMMENT 'Version of the report',
    sum_seconds BIGINT COMMENT 'Sum of seconds',
    sum_kwh DOUBLE COMMENT 'Sum of kWh',
    sum_kwh_battery DOUBLE COMMENT 'Sum of kWh from battery',
    sum_aux_kwh_shore_power DOUBLE COMMENT 'kWh og aux when consuming shore power',
    sum_boiler_kwh_shore_power DOUBLE COMMENT 'kWh of boiler when consuming shore power',
    sum_fuel_mdo_equivalent_tonnes DOUBLE COMMENT 'Sum of fuel MDO equivalent in tonnes',
    sum_fuel_main_aux_boiler_engines_tonnes DOUBLE COMMENT 'Sum of fuel from main engine, aux and boiler in tonnes',
    sum_co2_tonnes DOUBLE COMMENT 'Sum of CO2 emissions',
    sum_nmvoc_tonnes DOUBLE COMMENT 'Sum of NMVOC emissions',
    sum_co_tonnes DOUBLE COMMENT 'Sum of CO emissions',
    sum_ch4_tonnes DOUBLE COMMENT 'Sum of CH4 emissions',
    sum_n2o_tonnes DOUBLE COMMENT 'Sum of N2O emissions',
    sum_sox_tonnes DOUBLE COMMENT 'Sum of SOx emissions',
    sum_pm10_tonnes DOUBLE COMMENT 'Sum of PM10 emissions',
    sum_pm2_5_tonnes DOUBLE COMMENT 'Sum of PM2.5 emissions',
    sum_nox_tonnes DOUBLE COMMENT 'Sum of NOx emissions',
    sum_bc_tonnes DOUBLE COMMENT 'Sum of BC emissions',
    sum_co2e_tonnes DOUBLE COMMENT 'Sum of CO2e emissions',
    distance_kilometers DOUBLE COMMENT 'Distance in kilometers',
    bi_processing_time TIMESTAMP COMMENT 'Last processing time'
    )
    CLUSTER BY(year, mmsi, municipality_id, vessel_type)
    COMMENT 'MarU report with aggregated data'

    """
    )


def read_maru_raw_data(
    table_name: str, from_date: str, to_date: str, maru_raw_version: str
) -> DataFrame:
    """Retrieve MARU raw data from a specified table within a date range and filter by version.

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

    Returns:
    -------
    DataFrame
        A Spark DataFrame containing the filtered MARU raw data.

    """
    df = (
        spark.table(table_name)
        .select(
            "date_time_utc",
            "mmsi",
            "imo",
            "vessel_id",
            "ship_name",
            "call_sign",
            "sail_id",
            "phase",
            "maritime_borders_norwegian_economic_zone_id",
            "management_plan_marine_areas_area_id",
            "maritime_borders_norwegian_continental_shelf_id",
            "municipality_id",
            "in_coast_and_sea_area",
            "degree_of_electrification",
            "usage_percentage_vessel_port_electric",
            "shore_power_is_connected",
            "electric_shore_power_at_berth_reduction_factor",
            "main_engine_kwh",
            "aux_kwh",
            "boiler_kwh",
            "fuel_mdo_equivalent_tonnes",
            "fuel_main_aux_boiler_engines_tonnes",
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
        .where(
            (F.to_date(F.col("date_time_utc")) >= from_date)
            & (F.to_date(F.col("date_time_utc")) <= to_date)
        )
        .where(F.col("version") == maru_raw_version)
        .withColumn("year", F.date_format("date_time_utc", "yyyy").cast("smallint"))
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


def read_ais_voyages_data(table_name: str, from_date: str, to_date: str) -> DataFrame:
    """Retrieve MARU raw data from a specified table within a date range and filter by version.

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

    Returns:
    -------
    DataFrame
        A Spark DataFrame containing the filtered MARU raw data.

    """
    df = (
        spark.table(table_name)
        .select(
            "date_time_utc",
            "mmsi",
            "voyage_type",
            "delta_previous_point_seconds",
            "distance_previous_point_meters",
            "ship_is_stopped",
        )
        .where(
            (F.to_date(F.col("date_time_utc")) >= from_date)
            & (F.to_date(F.col("date_time_utc")) <= to_date)
        )
    )

    return df


def calculate_municipality_emission_type(df: DataFrame) -> DataFrame:
    """Calculate the municipality emission type based on the input DataFrame.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame containing the necessary columns.

    Returns:
    -------
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


def calculate_municipality_voyage_route(df: DataFrame) -> DataFrame:
    """Calculate the municipality voyage route based on the first and last municipality for each Sail ID.

    The names are sorted alphabetically before they are concatenated by a dash (-) and stored as the municiapality_voyage_route column. Note, due to the alphabetically order, the start municiapality may be both on the left and right side of the dash.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing columns for sail ID, municipality names, and municipality IDs.

    Returns:
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


def merge_datasets(
    df_maru_voyages: DataFrame,
    df_vessel_in: DataFrame,
    df_vessel_combined_in: DataFrame,
    df_municipality: DataFrame,
    df_county: DataFrame,
    df_maritime_borders_norwegian_economic_zone: DataFrame,
    df_management_plan_marine_areas: DataFrame,
) -> DataFrame:
    """Merge multiple datasets into a single DataFrame.

    Parameters
    ----------
    df_maru : DataFrame
        The MarU raw DataFrame.

    Returns:
    -------
    DataFrame
        A merged DataFrame containing data from all input DataFrames.

    """
    df = (
        df_maru_voyages.join(
            F.broadcast(df_vessel_in),
            on=(
                (df_maru_voyages.vessel_id == df_vessel_in.vessel_id)
                & (F.to_date(df_maru_voyages.date_time_utc) >= df_vessel_in.start_date)
                & (F.to_date(df_maru_voyages.date_time_utc) <= df_vessel_in.end_date)
            ),
            how="left",
        )
        .join(
            F.broadcast(df_vessel_combined_in),
            on=(df_vessel_combined_in.csid == df_maru_voyages.vessel_id),
            how="left",
        )
        .join(
            F.broadcast(df_municipality),
            on=(df_maru_voyages.municipality_id == df_municipality.municipality_no),
            how="left",
        )
        .join(
            F.broadcast(df_county),
            on=(df_maru_voyages.county_id == df_county.fylkesnummer),
            how="left",
        )
        .join(
            df_maritime_borders_norwegian_economic_zone,
            on=df_maru_voyages.maritime_borders_norwegian_economic_zone_id
            == df_maritime_borders_norwegian_economic_zone.id,
            how="left",
        )
        .join(
            df_management_plan_marine_areas,
            on=df_maru_voyages.management_plan_marine_areas_area_id
            == df_management_plan_marine_areas.area_id,
            how="left",
        )
        .select(
            df_maru_voyages["*"],
            df_vessel_in["*"],
            df_vessel_combined_in["shiptype_level5"],
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


def set_svalbard_as_county(df: DataFrame) -> DataFrame:
    """Set the county and municipality to "Svalbard" for specific maritime border IDs (territorial area around Svalbard).

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with maritime border IDs and county names.

    Returns:
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


def set_kwh_battery(df: DataFrame) -> DataFrame:
    """Set the kWh battery column based on degree of electrification.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing vessel data.

    Returns:
    -------
    DataFrame
        The DataFrame with the kWh battery column set.
    """
    df = df.withColumn(
        "kwh_battery",
        F.when(F.col("shore_power_is_connected"), F.lit(0)).otherwise(
            F.col("degree_of_electrification")
            * (F.col("main_engine_kwh") + F.col("aux_kwh") + F.col("boiler_kwh"))
        ),
    )

    return df


def generate_aggregated_report(df: DataFrame) -> DataFrame:
    """Generate an aggregated report from the joined dataset.

    Parameters
    ----------
    df : DataFrame
        The joined DataFrame containing vessel and maritime data.

    Returns:
    -------
    DataFrame
        An aggregated DataFrame with calculated metrics and processing timestamp.

    """
    df = df.groupBy(
        "year",
        F.date_format("date_time_utc", "MM").cast("tinyint").alias("month"),
        F.date_format("date_time_utc", "yyyy-MM").alias("year_month"),
        "vessel_id",
        "mmsi",
        "imo",
        "ship_name",
        "call_sign",
        "statcode5",
        "shiptype_level5",
        "gt_group",
        "gt",
        F.col("vessel_type_maru").alias("vessel_type"),
        "usage_percentage_vessel_port_electric",
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
        "shore_power_is_connected",
        "municipality_voyage_route",
        "version",
    ).agg(
        F.round(F.sum("delta_previous_point_seconds"), 2).alias("sum_seconds"),
        F.round(
            F.sum("main_engine_kwh") + F.sum("aux_kwh") + F.sum("boiler_kwh"), 4
        ).alias("sum_kwh"),
        F.round(F.sum("kwh_battery"), 4).alias("sum_kwh_battery"),
        F.round(F.sum("aux_kwh_shore_power"), 4).alias("sum_aux_kwh_shore_power"),
        F.round(F.sum("boiler_kwh_shore_power"), 4).alias("sum_boiler_kwh_shore_power"),
        F.sum("fuel_mdo_equivalent_tonnes").alias("sum_fuel_mdo_equivalent_tonnes"),
        F.sum("fuel_main_aux_boiler_engines_tonnes").alias(
            "sum_fuel_main_aux_boiler_engines_tonnes"
        ),
        F.sum("co2_tonnes").alias("sum_co2_tonnes"),
        F.sum("nmvoc_tonnes").alias("sum_nmvoc_tonnes"),
        F.sum("co_tonnes").alias("sum_co_tonnes"),
        F.sum("ch4_tonnes").alias("sum_ch4_tonnes"),
        F.sum("n2o_tonnes").alias("sum_n2o_tonnes"),
        F.sum("sox_tonnes").alias("sum_sox_tonnes"),
        F.sum("pm10_tonnes").alias("sum_pm10_tonnes"),
        F.sum("pm2_5_tonnes").alias("sum_pm2_5_tonnes"),
        F.sum("nox_tonnes").alias("sum_nox_tonnes"),
        F.sum("bc_tonnes").alias("sum_bc_tonnes"),
        F.sum("co2e_tonnes").alias("sum_co2e_tonnes"),
        F.round((F.sum("distance_previous_point_meters") / 1000), 2).alias(
            "distance_kilometers"
        ),
    )

    # Add processing time
    df = add_processing_timestamp(df)

    return df


def get_processed_dates(table_name: str, max_year_month_df: DataFrame) -> DataFrame:
    """Get processed MarU raw dates from ais job stats newer than the max year month.

    Parameters
    ----------
    table_name : str
        The name of the table to query.
    max_year_month_df : DataFrame
        A DataFrame containing the maximum year_month.

    Returns:
    -------
    DataFrame
        A DataFrame containing the processed dates.

    """
    return (
        spark.table(table_name)
        .where(F.col("maru").isNotNull())
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
    """Calculate the number of days in each month and the start and end dates.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with date information.

    Returns:
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
    """Check if the dates form a complete month, filter to only keep completed months and explode the date range.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame with date information.

    Returns:
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
        .where(F.col("is_complete_month") == True)
        .withColumn(
            "date_range", F.expr("sequence(start_date, end_date, interval 1 day)")
        )
        .select("date_range", F.explode("date_range").alias("date_utc"))
        .drop("date_range")
    )


def generate_list_of_periods_to_process(start_year: int = 2016) -> list:
    """Returns list with quarterly date ranges from start_year up to last full month.

    Args:
        start_year (int): The first year to include in the output. Defaults to 2016.

    Returns:
        list: A list of lists, where each sublist contains two ISO date strings
              representing the start and end of a quarterly date range.
    """
    today = datetime.now()  # noqa: DTZ005
    today_month_first_date = today.replace(day=1)
    last_month_last_day = today_month_first_date - timedelta(days=1)

    periods = []
    for year in range(start_year, last_month_last_day.year + 1):
        q1 = [f"{year}-01-01", f"{year}-03-31"]
        q2 = [f"{year}-04-01", f"{year}-06-30"]
        q3 = [f"{year}-07-01", f"{year}-09-30"]
        q4 = [f"{year}-10-01", f"{year}-12-31"]
        for q in [q1, q2, q3, q4]:
            if datetime.fromisoformat(q[1]) <= last_month_last_day:
                periods.append(q)  # noqa: PERF401
    return periods
