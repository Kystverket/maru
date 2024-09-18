from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, lit, when


def calculate_fuel_consumption(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the total fuel consumption for the main engine, auxiliary engine and boiler for each row in the given DataFrame.
    For a given hour, fuel consumption (g) is estimated by multiplying SFC (Specific Fuel Consumption) in g/kwh by the engine’s energy use (kWh).

    Parameters:
    -----------
    df: pyspark.sql.dataframe.DataFrame
        The input DataFrame containing columns `sfc_main_engine`, `main_engine_kwh`, `sfc_aux`, `aux_kwh`, `sfc_boiler` and `boiler_kwh`.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler.

    Returns:
    pyspark.sql.dataframe.DataFrame
        The output DataFrame with new columns: `{engine}_fuel_tonnes`, ie. main_engine_fuel_tonnes.
    """

    for engine in engines:
        df = df.withColumn(
            f"{engine}_fuel_tonnes",
            df[f"sfc_{engine}"]
            * df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            / 1_000_000,
        )

    # Set aux and boiler fuel to zero when electric shore power at berth:
    df = df.withColumn(
        "aux_fuel_tonnes",
        when((df["electric_shore_power_at_berth"]), 0).otherwise(df["aux_fuel_tonnes"]),
    ).withColumn(
        "boiler_fuel_tonnes",
        when((df["electric_shore_power_at_berth"]), 0).otherwise(
            df["boiler_fuel_tonnes"]
        ),
    )

    return df


def calculate_fuel_equivalent(df: DataFrame, var: dict, engines: list) -> DataFrame:
    """
    Calculate the fuel MDO equivalents

    Parameters:
    -----------
    df: pyspark.sql.DataFrame)
        A PySpark DataFrame object containing containing the necessary columns: 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        fuel equivalent factors.
    engines: List[str]
        A list of engine types, ie. aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the fuel equivalent factor values added as a new columns.
    """
    for engine in engines:
        df = df.withColumn(
            f"{engine}_fuel_mdo_equivalent_tonnes",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var["fuel_equivalent_hfo"] * df[f"{engine}_fuel_tonnes"],
            )
            .when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var["fuel_equivalent_mdo"] * df[f"{engine}_fuel_tonnes"],
            )
            .when(
                df["main_engine_fueltype"] == "LNG",
                var["fuel_equivalent_lng"] * df[f"{engine}_fuel_tonnes"],
            )
            .when(
                df["main_engine_fueltype"] == "Methanol",
                var["fuel_equivalent_methanol"] * df[f"{engine}_fuel_tonnes"],
            ),
        )

    df = df.withColumn(
        "fuel_mdo_equivalent_tonnes",
        sum(
            coalesce(df[f"{engine}_fuel_mdo_equivalent_tonnes"], lit(0))
            for engine in engines
        ),
    )

    return df
