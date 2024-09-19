from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when

REDUCE_FACT_TANKER = 1 / 3
REDUCE_FACT_OTHER = 3 / 4


def calculate_main_engine_load_factor(df: DataFrame) -> DataFrame:
    """
    Calculate main engine load factor.
    We estimate main engine load using service speed and phases.

    Following phases exist:
    - a: anchor
    - aq: aquacultur
    - c: cruise
    - dp-o: dynamic positioning offshore
    - f: fishing
    - m: maneuver
    - n: node (berth)

    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame containing the data to be analyzed.

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
        DataFrame with a new calculated "main_engine_load_factor" column added.

    """

    df = df.withColumn(
        "main_engine_load_factor",
        ((df["speed_over_ground"] / df["speed_service"]) ** 3) * 0.85,
    )

    # We assume engine load never to exceed 98 %:
    df = df.withColumn(
        "main_engine_load_factor",
        when(df["main_engine_load_factor"] < 0, 0)
        .when(df["main_engine_load_factor"] > 0.98, 0.98)
        .otherwise(df["main_engine_load_factor"]),
    )

    # Main engine load is set to zero in the following cases:

    # In node set main_engine_load_factor to zero
    df = df.withColumn(
        "main_engine_load_factor",
        when(df["phase"] == "n", 0).otherwise(df["main_engine_load_factor"]),
    )

    # In anchor set main_engine_load_factor to zero
    df = df.withColumn(
        "main_engine_load_factor",
        when(df["phase"] == "a", 0).otherwise(df["main_engine_load_factor"]),
    )

    # "Set some main_engine_load_factor on dp offshore."
    df = df.withColumn(
        "main_engine_load_factor",
        when(
            (df["phase"] == "dp-o") & (df["main_engine_load_factor"] < 0.1), 0.1
        ).otherwise(df["main_engine_load_factor"]),
    )

    return df


def calculate_main_engine_kwh(df: DataFrame) -> DataFrame:
    """
    Calculate energy consumption for main engine in kwh

    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame containing the data to be analyzed.

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
        DataFrame with a new calculated "main_engine_kw" column added.
    """

    df = df.withColumn(
        "main_engine_kwh",
        df["main_engine_load_factor"]
        * df["main_engine_kw"]
        * df["delta_previous_point_seconds"]
        / 3600,
    )

    return df


def calculate_aux_boiler_kwh(df: DataFrame) -> DataFrame:
    """
    Calculating aux engine kwh demand based on input table from IMO GHG based on phases.

    Following phases exist:
    - a: anchor
    - aq: aquacultur
    - c: cruise
    - dp-o: dynamic positioning offshore
    - f: fishing
    - m: maneuver
    - n: node (berth)

    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame containing the data to be analyzed.

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
        DataFrame with new calculated "aux_kwh" and "boiler_kwh" columns added.
    """

    # Set default values for kwh:
    df = df.withColumn("aux_kwh", lit(0))
    df = df.withColumn("boiler_kwh", lit(0))

    # When anchor:
    df = df.withColumn(
        "aux_kwh",
        when(
            df["phase"] == "a",
            df["aux_anchor_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["aux_kwh"]),
    )

    df = df.withColumn(
        "boiler_kwh",
        when(
            df["phase"] == "a",
            df["boiler_anchor_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["boiler_kwh"]),
    )

    # When dynamic positioning offshore (dp-o), fishing (f), aquacultur (aq) or maneuver (m) => maneuver:
    df = df.withColumn(
        "aux_kwh",
        when(
            df["phase"].isin(["aq", "dp-o", "f", "m"]),
            df["aux_maneuver_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["aux_kwh"]),
    )

    df = df.withColumn(
        "boiler_kwh",
        when(
            df["phase"].isin(["aq", "dp-o", "f", "m"]),
            df["boiler_maneuver_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["boiler_kwh"]),
    )

    # When cruise:
    df = df.withColumn(
        "aux_kwh",
        when(
            df["phase"] == "c",
            df["aux_cruise_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["aux_kwh"]),
    )

    df = df.withColumn(
        "boiler_kwh",
        when(
            df["phase"] == "c",
            df["boiler_cruise_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["boiler_kwh"]),
    )

    # When node:
    df = df.withColumn(
        "aux_kwh",
        when(
            df["phase"] == "n",
            df["aux_berth_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["aux_kwh"]),
    )

    df = df.withColumn(
        "boiler_kwh",
        when(
            df["phase"] == "n",
            df["boiler_berth_kw"] * df["delta_previous_point_seconds"] / 3600,
        ).otherwise(df["boiler_kwh"]),
    )

    # Reduce boiler / aux if in node for longer periods (high sail_time_remaining_seconds)

    # Aux:
    df = df.withColumn(
        "aux_kwh",
        when(
            (df["phase"] == "n"),
            when(
                df["vessel_type_maru"].isin("Gasstankskip", "Oljetanker")
                & (df["sail_time_remaining_seconds"] >= 3600 * 24 * 2),
                df["aux_kwh"] * REDUCE_FACT_TANKER,
            )
            .when(
                (~df["vessel_type_maru"].isin("Gasstankskip", "Oljetanker"))
                & (df["sail_time_remaining_seconds"] >= 3600 * 24 * 1),
                df["aux_kwh"] * REDUCE_FACT_OTHER,
            )
            .otherwise(df["aux_kwh"]),
        ).otherwise(df["aux_kwh"]),
    )
    # Boiler:
    df = df.withColumn(
        "boiler_kwh",
        when(
            (df["phase"] == "n"),
            when(
                df["vessel_type_maru"].isin("Gasstankskip", "Oljetanker")
                & (df["sail_time_remaining_seconds"] >= 3600 * 24 * 2),
                df["boiler_kwh"] * REDUCE_FACT_TANKER,
            )
            .when(
                (~df["vessel_type_maru"].isin("Gasstankskip", "Oljetanker"))
                & (df["sail_time_remaining_seconds"] >= 3600 * 24 * 1),
                df["boiler_kwh"] * REDUCE_FACT_OTHER,
            )
            .otherwise(df["boiler_kwh"]),
        ).otherwise(df["boiler_kwh"]),
    )

    # — when main engine power is between 0 and 150 kW then auxiliary engine and boiler are
    # set to zero;
    # — when main engine power is between 150 and 500 kW then the auxiliary engine is set to
    # 5% of the main engine installed power while the boiler power output is based on Table
    # 17;
    # — when the main engine power is larger than 500 kW then the auxiliary engine and boiler
    # values shown in Table 17 are used

    df = df.withColumn(
        "aux_kwh",
        when((df["main_engine_kw"] > 0) & (df["main_engine_kw"] <= 150), lit(0))
        .when(
            (df["main_engine_kw"] > 150) & (df["main_engine_kw"] <= 500),
            df["main_engine_kw"] * 0.05 * df["delta_previous_point_seconds"] / 3600,
        )
        .otherwise(df["aux_kwh"]),
    )

    df = df.withColumn(
        "boiler_kwh",
        when(
            (df["main_engine_kw"] > 0) & (df["main_engine_kw"] <= 150), lit(0)
        ).otherwise(df["boiler_kwh"]),
    )

    return df
