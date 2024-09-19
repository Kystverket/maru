from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_sox_factor(df: DataFrame, var: dict, engines: list, areas: list) -> DataFrame:
    """
    Set `SOx` (Sulphur Dioxide) factor based on fuel type.

    Reference is the IMO GHG 4.study. Equation (15) p. 74 (102) is used to calculate SOx for residual and distillate fuel:

    SOx g/fuel = 2 * 0.97753 * S

    - S is the fuel sulphur content fraction given as g of SOx.
    - Assumed that 97.753% of the sulphur in the fuel is converted to SOx (the rest is converted to sulphate/sulfite aerosol and classified as a part of particulate matter)
    - The '2' reflects the ratio of the molecular weight of SO2 to sulphur because, for ship emissions, the vast majority of SOx is O2.

    SOx emissions from vessel consuming LNG or Methanol are obtained from table 48 p. 409 (437).


    Parameters:
    -----------
    df: PySpark DataFrame
        Input dataframe containing fuel type and fuel suplfur fraction.
    var: dict
        A dictionary containing input variables for different emission factors
    engines: List[str]
        A list of engine types to calculate SOx factor for.
    areas: List[str]
        A list of areas ('global' or 'eca') to calculate SOx factor for.


    Returns:
    --------
    PySpark DataFrame
        Output dataframe with SOx factor added as columns.
    """

    for engine in engines:
        for area in areas:
            df = df.withColumn(
                f"sox_{engine}_{area}_factor",
                # Oil fuel:
                when(
                    df["main_engine_fueltype"].isin(
                        ["Residual Fuel", "Distillate Fuel"]
                    ),
                    2 * 0.97753 * df[f"fuel_sulfur_fraction_{area}"],
                )
                # LNG:
                .when(df["main_engine_fueltype"] == "LNG", var[f"sox_{engine}_lng"])
                # Methanol:
                .when(
                    df["main_engine_fueltype"] == "Methanol",
                    var[f"sox_{engine}_methanol"],
                ),
            )

    return df


def calculate_sox(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculate `SOx` (Sulphur Dioxide) emissions by multiplying the SOx factor
    with the fuel consumption of the engines.

    Parameters:
    -----------
    df: PySpark DataFrame
        - eca (bool): Inside / outside Emission Control Area
        - sox factors (float): Factors set by function set_sox_factor
        - main_engine_fuel_tonnes / aux_fuel_tonnes / boiler_fuel_tonnes (float): Fuel consumption in tonnes

    engines: List[str]
        A list of engine types to calculate SOx factor for.

    Returns:
    --------
    PySpark DataFrame
        Output dataframe with SOx emissions calculated as columns.
    """

    for engine in engines:
        df = df.withColumn(
            f"sox_{engine}_tonnes",
            when(
                df["eca"],
                df[f"sox_{engine}_eca_factor"] * df[f"{engine}_fuel_tonnes"],
            ).otherwise(
                df[f"sox_{engine}_global_factor"] * df[f"{engine}_fuel_tonnes"]
            ),
        )

    return df
