from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when


def set_fuel_sulfur_fraction_global(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the global fuel sulfur fraction for each observation in a DataFrame.

    Parameters:
    -----------
    df: pyspark.sql.dataframe.DataFrame
        The input DataFrame with column main_engine_fueltype.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the added 'fuel_sulfur_fraction_global' column.
    """

    df = df.withColumn(
        "fuel_sulfur_fraction_global",
        when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            lit(var["fuel_sulfur_fraction_global_distillate_fuel"]),
        )
        .when(
            df["main_engine_fueltype"] == "Residual Fuel",
            lit(var["fuel_sulfur_fraction_global_residual_fuel"]),
        )
        .otherwise(lit(var["fuel_sulfur_fraction_global"])),
    )

    return df


def set_fuel_sulfur_fraction_eca(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the fuel sulfur fraction for each observation in a DataFrame if the area is
    an ECA (Emission Control Area). The fuel sulfur fraction is set to a fixed value
    defined in var["fuel_sulfur_fraction_eca"].

    Parameters:
    -----------
    df: pyspark.sql.dataframe.DataFrame
        The input DataFrame.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the added 'fuel_sulfur_fraction_eca' column.
    """

    df = df.withColumn("fuel_sulfur_fraction_eca", lit(var["fuel_sulfur_fraction_eca"]))

    return df
