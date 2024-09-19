from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def set_electric_shore_power_at_berth(df: DataFrame) -> DataFrame:
    """
    Adds a boolean column to indicate if electric shore power is available at berth.

    This function evaluates certain conditions to determine if electric shore power
    can be used while a ship is at berth. The conditions include being close to power,
    being in a specific phase, and having a sail time remaining of more than 2 hours.
    A new column `electric_shore_power_at_berth` is added to the DataFrame indicating
    the availability of electric shore power (True or False).

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

    return df


def set_phase_p_when_electric_shore_power_at_berth(df: DataFrame) -> DataFrame:
    """
    Sets the phase to "p" (power) when electric shore power is available at berth.
    """
    df = df.withColumn(
        "phase", when(df["electric_shore_power_at_berth"], "p").otherwise(col("phase"))
    )
    return df
