from pyspark.sql import DataFrame


def calculate_co2e(df: DataFrame, var: dict) -> DataFrame:
    """
    CO2 equivalent (co2e) according to "Global warming potential in a 100-year perspective (GWP100, global)" from the Norwegian Environment Agency (Miljødirektoratet).

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns "main_engine_fueltype"

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A new dataframe with the additional column "co2e_factor"
    """

    df = df.withColumn(
        "co2e_ton",
        df["co2_ton"] * var["co2e_gwp100_co2"]
        + df["ch4_ton"] * var["co2e_gwp100_ch4"]
        + df["n2o_ton"] * var["co2e_gwp100_n2o"],
    )
    return df
