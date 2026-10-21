from ais.shipdata_qsr.utils.engine_enrichments import set_gt_group
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, element_at, lit, when


def create_vessel_historical_table(spark: SparkSession, table_path: str) -> None:
    """Create the dim_vessel_historical table if it does not already exist.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
    table_path : str
        Fully qualified table path (e.g. ``gold_dev.maru.dim_vessel_historical_v1_10``).

    """
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {table_path} (
    vessel_id BIGINT COMMENT 'Identifier for the vessel (hash of mmsi, imo and call sign)',
    start_date DATE COMMENT 'Start date of the entry',
    end_date DATE COMMENT 'End date of the entry',
    mmsi ARRAY<INT> COMMENT 'Maritime Mobile Service Identity array',
    imo ARRAY<INT> COMMENT 'International Maritime Organization number',
    call_sign ARRAY<STRING> COMMENT 'Vessel call sign',
    ship_name STRING COMMENT 'Name of the ship',
    flag_code STRING COMMENT 'Flag country code',
    vessel_type_imo_ghg STRING COMMENT 'Vessel type according to IMO GHG',
    vessel_type_maru STRING COMMENT 'Vessel type according to Maru classification',
    statcode5 STRING COMMENT 'Statcode5 code',
    year_of_build INT COMMENT 'Year the vessel was built',
    gt DOUBLE COMMENT 'Gross tonnage',
    dwt FLOAT COMMENT 'Deadweight tonnage',
    teu FLOAT COMMENT 'Twenty-foot Equivalent Unit',
    cbm FLOAT COMMENT 'Cubic meter',
    length_overall FLOAT COMMENT 'Overall length of the vessel',
    breadth_moulded FLOAT COMMENT 'Breadth of the vessel moulded',
    draught FLOAT COMMENT 'Draught of the vessel',
    ship_type STRING COMMENT 'Type of the ship',
    main_engine_rpm FLOAT COMMENT 'RPM (revolutions per minute) of the main engine',
    main_engine_kw FLOAT COMMENT 'KW of the main engine',
    stroketype INT COMMENT 'Type of engine stroke',
    speed_service FLOAT COMMENT 'Service speed of the vessel',
    speed_max FLOAT COMMENT 'Maximum speed of the vessel',
    power_kw_max FLOAT COMMENT 'Maximum power in KW',
    fp_fueltype1code STRING COMMENT 'Primary fuel type code',
    fp_fueltype2code STRING COMMENT 'Secondary fuel type code',
    fp_me_enginetype STRING COMMENT 'Main engine type',
    fp_propulsiontype STRING COMMENT 'Type of propulsion',
    fp_lengthbetweenperpendicularslbp FLOAT COMMENT 'Length between the forward and aft perpendiculars',
    fp_displacement INT COMMENT 'The total weight of the ship (tonnes) based on the volume of water it pushes aside when fully loaded',
    fp_breadthextreme FLOAT COMMENT 'The maximum width of the hull of the ship, measured at the widest point',
    engine_builder STRING COMMENT 'Engine builder',
    engine_model STRING COMMENT 'Engine model',
    degree_of_electrification DOUBLE COMMENT 'Degree of electrification',
    battery_pack_kwh INT COMMENT 'Battery pack capacity in kWh',
    aux_berth_kw INT COMMENT 'Auxiliary power at berth in kW',
    aux_anchor_kw INT COMMENT 'Auxiliary power at anchor in kW',
    aux_maneuver_kw INT COMMENT 'Auxiliary power during maneuvering in kW',
    aux_cruise_kw INT COMMENT 'Auxiliary power during cruising in kW',
    boiler_berth_kw INT COMMENT 'Boiler power at berth in kW',
    boiler_anchor_kw INT COMMENT 'Boiler power at anchor in kW',
    boiler_maneuver_kw INT COMMENT 'Boiler power during maneuvering in kW',
    boiler_cruise_kw INT COMMENT 'Boiler power during cruising in kW',
    length_group STRING COMMENT 'Grouping by length',
    tier_group INT COMMENT 'Tier group',
    tier_group_nox INT COMMENT 'NOx tier group',
    engine_nox_rating DOUBLE COMMENT 'Engine NOx rating from DNV EPI data when available',
    year_group STRING COMMENT 'Grouping by year',
    gt_group STRING COMMENT 'Grouping by gross tonnage',
    dwt_group STRING COMMENT 'Grouping by deadweight tonnage',
    fp_propulsiontypecode STRING COMMENT 'Propulsion type code',
    main_engine_fueltype STRING COMMENT 'Main engine fuel type',
    main_engine_fueltype_original STRING COMMENT 'Original main engine fuel type',
    main_engine_enginetype STRING COMMENT 'Main engine type',
    msg_type SMALLINT COMMENT 'AIS static message type (5=Class A, 24=Class B)'
)
CLUSTER BY (vessel_id, start_date, end_date)
COMMENT 'Ship register for the MarU model. This table contains historical data with columns `start_date` `end_date`'
"""
    )


def create_vessel_current_table(spark: SparkSession, table_path: str) -> None:
    """Create the dim_vessel_current table if it does not already exist.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
    table_path : str
        Fully qualified table path (e.g. ``gold_dev.maru.dim_vessel_current_v1_10``).

    """
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {table_path} (
    vessel_id BIGINT COMMENT 'Identifier for the vessel (hash of mmsi, imo and call sign)',
  mmsi ARRAY<INT> COMMENT 'Maritime Mobile Service Identity array',
  imo ARRAY<INT> COMMENT 'International Maritime Organization number',
  call_sign ARRAY<STRING> COMMENT 'Vessel call sign',
  ship_name STRING COMMENT 'Name of the ship',
  flag_code STRING COMMENT 'Flag country code',
  vessel_type_imo_ghg STRING COMMENT 'Vessel type according to IMO GHG',
  vessel_type_maru STRING COMMENT 'Vessel type according to Maru classification',
  statcode5 STRING COMMENT 'Statcode5 code',
  year_of_build INT COMMENT 'Year the vessel was built',
  gt DOUBLE COMMENT 'Gross tonnage',
  dwt FLOAT COMMENT 'Deadweight tonnage',
  teu FLOAT COMMENT 'Twenty-foot Equivalent Unit',
  cbm FLOAT COMMENT 'Cubic meter',
  length_overall FLOAT COMMENT 'Overall length of the vessel',
  breadth_moulded FLOAT COMMENT 'Breadth of the vessel moulded',
  draught FLOAT COMMENT 'Draught of the vessel',
  ship_type STRING COMMENT 'Type of the ship',
  main_engine_rpm FLOAT COMMENT 'RPM (revolutions per minute) of the main engine',
  main_engine_kw FLOAT COMMENT 'KW of the main engine',
  stroketype INT COMMENT 'Type of engine stroke',
  speed_service FLOAT COMMENT 'Service speed of the vessel',
  speed_max FLOAT COMMENT 'Maximum speed of the vessel',
  power_kw_max FLOAT COMMENT 'Maximum power in KW',
  fp_fueltype1code STRING COMMENT 'Primary fuel type code',
  fp_fueltype2code STRING COMMENT 'Secondary fuel type code',
  fp_me_enginetype STRING COMMENT 'Main engine type',
  fp_propulsiontype STRING COMMENT 'Type of propulsion',
  engine_builder STRING COMMENT 'Engine builder',
  engine_model STRING COMMENT 'Engine model',
  degree_of_electrification DOUBLE COMMENT 'Degree of electrification',
  battery_pack_kwh INT COMMENT 'Battery pack capacity in kWh',
  aux_berth_kw INT COMMENT 'Auxiliary power at berth in kW',
  aux_anchor_kw INT COMMENT 'Auxiliary power at anchor in kW',
  aux_maneuver_kw INT COMMENT 'Auxiliary power during maneuvering in kW',
  aux_cruise_kw INT COMMENT 'Auxiliary power during cruising in kW',
  boiler_berth_kw INT COMMENT 'Boiler power at berth in kW',
  boiler_anchor_kw INT COMMENT 'Boiler power at anchor in kW',
  boiler_maneuver_kw INT COMMENT 'Boiler power during maneuvering in kW',
  boiler_cruise_kw INT COMMENT 'Boiler power during cruising in kW',
  length_group STRING COMMENT 'Grouping by length',
  tier_group INT COMMENT 'Tier group',
  tier_group_nox INT COMMENT 'NOx tier group',
  engine_nox_rating DOUBLE COMMENT 'Engine NOx rating from DNV EPI data when available',
  year_group STRING COMMENT 'Grouping by year',
  gt_group STRING COMMENT 'Grouping by gross tonnage',
  dwt_group STRING COMMENT 'Grouping by deadweight tonnage',
  fp_propulsiontypecode STRING COMMENT 'Propulsion type code',
  fp_lengthbetweenperpendicularslbp FLOAT COMMENT 'Length between the forward and aft perpendiculars',
  fp_displacement INT COMMENT 'The total weight of the ship (tonnes) based on the volume of water it pushes aside when fully loaded',
  fp_breadthextreme FLOAT COMMENT 'The maximum width of the hull of the ship, measured at the widest point',
  main_engine_fueltype STRING COMMENT 'Main engine fuel type',
  main_engine_fueltype_original STRING COMMENT 'Original main engine fuel type',
  main_engine_enginetype STRING COMMENT 'Main engine type',
  msg_type SMALLINT COMMENT 'AIS static message type (5=Class A, 24=Class B)'

)
CLUSTER BY (vessel_id)
COMMENT 'Ship register for the MarU model. This table contains current data without history'
"""
    )


def set_year_tier(df: DataFrame) -> DataFrame:
    """Set tier group from year.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: tier_group.

    """
    df = df.withColumn(
        # Set year tier
        "tier_group",
        when((df["year_of_build"] >= 1900) & (df["year_of_build"] < 2000), lit(0))
        .when((df["year_of_build"] >= 2000) & (df["year_of_build"] < 2010), lit(1))
        .when((df["year_of_build"] >= 2010) & (df["year_of_build"] < 2020), lit(2))
        .when(df["year_of_build"] >= 2020, lit(3))
        .otherwise(lit(0)),
    )

    return df


def set_year_tier_nox(df: DataFrame) -> DataFrame:
    """Nox tier according to IMO GHG fourth study, table 11.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: tier_group.

    """
    df = df.withColumn(
        # Set year tier
        "tier_group_nox",
        when((df["year_of_build"] >= 1900) & (df["year_of_build"] < 2000), lit(0))
        .when((df["year_of_build"] >= 2000) & (df["year_of_build"] < 2011), lit(1))
        .when((df["year_of_build"] >= 2011) & (df["year_of_build"] < 2021), lit(2))
        .when(df["year_of_build"] >= 2021, lit(3))
        .otherwise(lit(-99)),
    )

    return df


def add_dnv_engine_nox_rating(
    df: DataFrame, df_dnv_engine_nox_rating: DataFrame
) -> DataFrame:
    """Attach the lowest valid DNV engine NOx rating to each vessel.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Vessel DataFrame with an ``imo`` ARRAY<INT> column.
    df_dnv_engine_nox_rating : pyspark.sql.DataFrame
        DNV NOx rating DataFrame with ``dnv_imo`` and
        ``dnv_engine_nox_rating`` columns.

    Returns:
    -------
    pyspark.sql.DataFrame
        DataFrame with an ``engine_nox_rating`` column when DNV data is
        available for the vessel.

    """
    df = df.join(
        df_dnv_engine_nox_rating,
        element_at(col("imo"), 1) == col("dnv_imo"),
        how="left",
    )

    return df.withColumn(
        "engine_nox_rating",
        col("dnv_engine_nox_rating"),
    ).drop("dnv_imo", "dnv_engine_nox_rating")


def set_year_group(df: DataFrame) -> DataFrame:
    """Set year group from year.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: year_group.

    """
    df = df.withColumn(
        "year_group",
        when((df["year_of_build"] > 0) & (df["year_of_build"] < 1960), lit("y1, <1959"))
        .when(
            (df["year_of_build"] >= 1960) & (df["year_of_build"] < 1980),
            lit("y2, 1960-1979"),
        )
        .when(
            (df["year_of_build"] >= 1980) & (df["year_of_build"] < 1990),
            lit("y3, 1980-1989"),
        )
        .when(
            (df["year_of_build"] >= 1990) & (df["year_of_build"] < 2000),
            lit("y4, 1990-1999"),
        )
        .when(
            (df["year_of_build"] >= 2000) & (df["year_of_build"] < 2005),
            lit("y5, 2000-2004"),
        )
        .when(
            (df["year_of_build"] >= 2005) & (df["year_of_build"] < 2010),
            lit("y6, 2005-2009"),
        )
        .when(
            (df["year_of_build"] >= 2010) & (df["year_of_build"] < 2015),
            lit("y7, 2010-2014"),
        )
        .when(
            (df["year_of_build"] >= 2015) & (df["year_of_build"] < 2020),
            lit("y8, 2015-2019"),
        )
        .when(df["year_of_build"] >= 2020, lit("y9, >=2020"))
        .otherwise(lit("Unknown")),
    )

    return df


def set_length_group(df: DataFrame) -> DataFrame:
    """Set length group from length.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: length_overall

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: length_group.

    """
    df = df.withColumn(
        "length_group",
        when((df["length_overall"] > 0) & (df["length_overall"] < 24), "l1, <24 m")
        .when((df["length_overall"] >= 24) & (df["length_overall"] < 50), "l2, 24-50 m")
        .when((df["length_overall"] >= 50) & (df["length_overall"] < 70), "l3, 50-70 m")
        .when(
            (df["length_overall"] >= 70) & (df["length_overall"] < 100), "l4, 70-100 m"
        )
        .when(
            (df["length_overall"] >= 100) & (df["length_overall"] < 150),
            "l5, 100-150 m",
        )
        .when(
            (df["length_overall"] >= 150) & (df["length_overall"] < 225),
            "l6, 150-225 m",
        )
        .when(df["length_overall"] >= 225, "l7, >=225 m")
        .otherwise("Unknown"),
    )

    return df


def set_dwt_group(df: DataFrame) -> DataFrame:
    """Set dwt group (deadweight tonnage group) from dwt (deadweight tonnage).

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: dwt

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: dwt_group.

    """
    df = df.withColumn(
        "dwt_group",
        when((df["dwt"] > 0) & (df["dwt"] < 5000), "dwt1, <5000")
        .when(
            (df["dwt"] >= 5000) & (df["dwt"] < 10000),
            "dwt2, 5 000-10 000",
        )
        .when(
            (df["dwt"] >= 10000) & (df["dwt"] < 25000),
            "dwt3, 10 000-25 000",
        )
        .when(
            (df["dwt"] >= 25000) & (df["dwt"] < 50000),
            "dwt4, 25 000-50 000",
        )
        .when(
            (df["dwt"] >= 50000) & (df["dwt"] < 75000),
            "dwt5, 50 000-75 000",
        )
        .when(
            (df["dwt"] >= 75000) & (df["dwt"] < 100000),
            "dwt6, 75 000-100 000",
        )
        .when(
            (df["dwt"] >= 100000) & (df["dwt"] < 150000),
            "dwt7, 100 000-150 000",
        )
        .when(
            (df["dwt"] >= 150000) & (df["dwt"] < 200000),
            "dwt8, 150 000-200 000",
        )
        .when(df["dwt"] >= 200000, "dwt9, >=200 000")
        .otherwise("Unknown"),
    )

    return df


def update_degree_of_electrification(df: DataFrame) -> DataFrame:
    """Set degree_of_electrification to 0.95 for vessels with fuel_type = Electric when degree_of_electrification is not set from vessel_battery table.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation: fuel_type and degree_of_electrification

    Returns:
    -------
    pyspark.sql.DataFrame
    Spark DataFrame with updated values.

    """
    df = df.withColumn(
        "degree_of_electrification",
        when(
            (df["main_engine_fueltype"] == "Electric")
            & (df["degree_of_electrification"].isNull()),
            lit("0.95").cast("double"),
        ).otherwise(coalesce(df["degree_of_electrification"], lit(0))),
    )

    return df


def update_main_engine_kw_with_speed_correction_factor(df: DataFrame) -> DataFrame:
    """Apply a power correction factor to the 'main_engine_kw' column based on the vessel type.

    For some ship types (large container ships and cruise ships) the speed given in the ship registers (e.g. S&P Markit)
    correspond to lower engine loading than maximum continuous rating (MCR). The model therefore overpredict power output.
    A power correction factor of 0.70 is used for Cruise.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame containing the necessary columns for the calculation: 'vessel_type_maru' and 'main_engine_kw'.

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with the 'main_engine_kw' column updated with the power correction factor.

    """
    df = df.withColumn(
        "main_engine_kw",
        when(
            df["vessel_type_maru"] == "Cruise",
            (df["main_engine_kw"] * 0.7).cast("float"),
        ).otherwise(df["main_engine_kw"]),
    )
    return df


def add_enriched_columns(df: DataFrame) -> DataFrame:
    """Select and add new columns to vessel dataset.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns (see specific columns in docstring for each function)

    Returns:
    -------
    pyspark.sql.DataFrame
        Spark DataFrame with new columns (see specific columns in docstring for each function).

    """
    df = set_length_group(df)
    df = set_year_tier(df)
    df = set_year_tier_nox(df)
    df = set_year_group(df)
    df = set_dwt_group(df)
    df = set_gt_group(df, "gt")
    df = update_degree_of_electrification(df)
    df = update_main_engine_kw_with_speed_correction_factor(df)

    return df
