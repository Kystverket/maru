from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_nmvoc_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `nmvoc` (Non-methane volatile organic compounds) factor in `g/kWh` for main engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary columns: 'main_engine_enginetype', 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the nmvoc factor values added as a new column.
    """

    df = df.withColumn(
        "nmvoc_main_engine_factor",
        # SSD:
        when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Methanol"),
            var["nmvoc_main_engine_ssd_methanol"],
        ).when(df["main_engine_enginetype"] == "SSD", var["nmvoc_main_engine_ssd"])
        # MSD:
        .when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Methanol"),
            var["nmvoc_main_engine_msd_methanol"],
        ).when(df["main_engine_enginetype"] == "MSD", var["nmvoc_main_engine_msd"])
        # HSD:
        .when(df["main_engine_enginetype"] == "HSD", var["nmvoc_main_engine_hsd"])
        # GT:
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nmvoc_main_engine_gt_lng"],
        ).when(df["main_engine_enginetype"] == "GT", var["nmvoc_main_engine_gt"])
        # ST:
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nmvoc_main_engine_st_lng"],
        ).when(df["main_engine_enginetype"] == "ST", var["nmvoc_main_engine_st"])
        # LNG:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["nmvoc_main_engine_lng_otto_ss"],
        )
        .when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            var["nmvoc_main_engine_lng_otto_ms"],
        )
        .when(df["main_engine_enginetype"] == "LBSI", var["nmvoc_main_engine_lng_lbsi"])
        .when(
            df["main_engine_enginetype"] == "LNG-Diesel",
            var["nmvoc_main_engine_lng_diesel"],
        ),
    )

    return df


def set_nmvoc_aux_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `nmvoc` (Non-methane volatile organic compounds) factor in `g/kWh` for aux engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary column 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the nmvoc factor values added as a new column.
    """

    df = df.withColumn(
        "nmvoc_aux_factor",
        when(
            df["main_engine_fueltype"] == "Residual Fuel",
            var["nmvoc_aux_residual_fuel"],
        )
        .when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            var["nmvoc_aux_distillate_fuel"],
        )
        .when(df["main_engine_fueltype"] == "LNG", var["nmvoc_aux_lng"]),
    )

    return df


def set_nmvoc_boiler_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `nmvoc` (Non-methane volatile organic compounds) factor in `g/kWh` for boiler.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary column 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the nmvoc factor values added as a new column.
    """

    df = df.withColumn(
        "nmvoc_boiler_factor",
        when(
            df["main_engine_fueltype"] == "Residual Fuel",
            var["nmvoc_boiler_residual_fuel"],
        )
        .when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            var["nmvoc_boiler_distillate_fuel"],
        )
        .when(df["main_engine_fueltype"] == "LNG", var["nmvoc_boiler_lng"]),
    )

    return df


def calculate_nmvoc(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `nmvoc` (Non-methane volatile organic compounds) emission in tonnes for each engine type 'main_engine', 'aux', 'boiler', based on their
    respective factors, i.e. 'nmvoc_main_engine_factor_kwh', 'nmvoc_aux_factor_kwh', 'nmvoc_boiler_factor_kwh', and the
    energy consumption of each engine in kWh, i.e. 'main_engine_kwh', 'aux_kwh', 'boiler_kwh',
    respectively. The calculation is based on the formula nmvoc_emission = factor * kWh_consumption / 1 000 000.
    (g / kwh => divide by 1 000 000 to get tonnes)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing the data to be processed.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the nmvoc emission values in tonnes.
    """
    for engine in engines:
        df = df.withColumn(
            f"nmvoc_{engine}_tonnes",
            df[f"nmvoc_{engine}_factor"]
            * df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000,
        )

    return df
