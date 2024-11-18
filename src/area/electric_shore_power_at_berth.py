from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, coalesce, col, lit
from pyspark.sql.functions import round as F_round
from pyspark.sql.functions import to_date, when


def join_ais_vessel_port_electric(
    df_ais: DataFrame,
    df_vessel_port_electric: DataFrame,
    df_vessel_type_port_electric: DataFrame,
) -> DataFrame:
    """
    Join shore power phase data with vessel port electric and vessel type port electric data.

    Parameters
    ----------
    df_ais : DataFrame
        DataFrame containing shore power phase data.
    df_vessel_port_electric : DataFrame
        DataFrame containing vessel port electric data.
    df_vessel_type_port_electric : DataFrame
        DataFrame containing vessel type port electric data.

    Returns
    -------
    DataFrame
        DataFrame with joined data and selected columns.
    """
    df_result = (
        df_ais.join(
            broadcast(df_vessel_port_electric),
            on=(
                (df_ais.vessel_id == df_vessel_port_electric.vessel_id)
                & (df_ais.municipality_id == df_vessel_port_electric.municipality_no)
                & (to_date(df_ais.date_time_utc) >= df_vessel_port_electric.start_date)
                & (to_date(df_ais.date_time_utc) <= df_vessel_port_electric.end_date)
            ),
            how="left",
        )
        .join(
            broadcast(df_vessel_type_port_electric),
            on=(
                (df_ais.vessel_type_maru == df_vessel_type_port_electric.vessel_type)
                & (
                    df_ais.municipality_id
                    == df_vessel_type_port_electric.municipality_no
                )
                & (
                    to_date(df_ais.date_time_utc)
                    >= df_vessel_type_port_electric.start_date
                )
                & (
                    to_date(df_ais.date_time_utc)
                    <= df_vessel_type_port_electric.end_date
                )
            ),
            how="left",
        )
        .select(
            df_ais["*"],
            df_vessel_port_electric["usage_percentage"].alias(
                "usage_percentage_vessel_port_electric"
            ),
            df_vessel_type_port_electric["usage_percentage"].alias(
                "usage_percentage_vessel_type_port_electric"
            ),
        )
    )
    return df_result


def set_electric_shore_power_at_berth(df: DataFrame) -> DataFrame:
    """
    Adds a boolean column to indicate if electric shore power is available at berth.

    This function evaluates certain conditions to determine if electric shore power
    can be used while a ship is at berth. The conditions include being close to power,
    being in a specific phase, and having a sail time remaining of more than 2 hours.
    A new column `electric_shore_power_at_berth` is added to the DataFrame indicating
    the availability of electric shore power (True or False).
    Additionally, it calculates a reduction factor based on whether electric shore power
    is used, using a variable from the input dictionary `var` for the shore power usage
    percentage. This reduction factor is stored in the column
    `electric_shore_power_at_berth_reduction_factor`.


    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the columns `close_to_power`, `phase`, and
        `sail_time_remaining_seconds`.

    Returns
    -------
    DataFrame
        The original DataFrame with an added boolean column `electric_shore_power_at_berth`.
    """

    df = df.withColumn(
        "electric_shore_power_at_berth",
        when(
            (df["close_to_power"])
            & (df["phase"] == "n")
            & (df["sail_time_remaining_seconds"] > 3600 * 2),
            True,
        ).otherwise(False),
    )

    df = df.withColumn(
        "shore_power_usage_percentage_total",
        when(
            df["electric_shore_power_at_berth"],
            coalesce(
                df["usage_percentage_vessel_port_electric"],
                df["usage_percentage_vessel_type_port_electric"],
                df["shore_power_usage_percentage"],
            ),
        ).otherwise(lit(0)),
    )

    df = df.withColumn(
        "electric_shore_power_at_berth_reduction_factor",
        F_round(1 - df["shore_power_usage_percentage_total"], 2),
    )

    return df


def set_phase_p_when_electric_shore_power_at_berth(df: DataFrame) -> DataFrame:
    """
    Sets the phase to "p" (power) when electric shore power is available at berth.
    """
    df = df.withColumn(
        "phase", when(df["electric_shore_power_at_berth"], "p").otherwise(col("phase"))
    )
    return df
