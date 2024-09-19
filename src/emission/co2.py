from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_cf(df: DataFrame, var: dict) -> DataFrame:
    """
    Carbon conversion factors for `co2` (Carbon Dioxide) in `gram co2 / gram fuel`, according to Fourth IMO GHG Study 2020. Full report, Table 21

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
        A new dataframe with the additional column "cf_eca" and "cf_global.
    """

    df = df.withColumn(
        "cf_global",
        when(df["main_engine_fueltype"] == "Residual Fuel", var["cf_residual_fuel"])
        .when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            var["cf_distillate_fuel"],
        )
        .when(df["main_engine_fueltype"] == "LNG", var["cf_lng"])
        .when(df["main_engine_fueltype"] == "Methanol", var["cf_methanol"])
        .otherwise(var["cf_other"]),
    )
    # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:
    df = df.withColumn(
        "cf_eca",
        when(
            df["main_engine_fueltype"] == "Residual Fuel", var["cf_distillate_fuel"]
        ).otherwise(df["cf_global"]),
    )
    return df


def calculate_co2(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `co2` (Carbon Dioxide) emissions in `tonnes` for a given dataset.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        The input dataset containing columns `main_engine_fuel_tonnes`, `aux_fuel_tonnes`, `boiler_fuel_tonnes`, `eca`, `cf_global` and `cf_eca`.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        The input dataframe with three new columns: `co2_main_engine_tonnes`, `co2_aux_tonnes`, and `co2_boiler_tonnes`.
    """
    for engine in engines:
        df = df.withColumn(
            f"co2_{engine}_tonnes",
            when(df["eca"], df["cf_eca"] * df[f"{engine}_fuel_tonnes"]).otherwise(
                df["cf_global"] * df[f"{engine}_fuel_tonnes"]
            ),
        )

    return df
