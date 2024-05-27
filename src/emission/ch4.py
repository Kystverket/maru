from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_ch4_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `ch4` (methane) factor (in g/kWh) for main engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame)
        A PySpark DataFrame object containing containing the necessary columns: 'main_engine_enginetype', 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the CH4 factor values added as a new column.
    """

    df = df.withColumn(
        "ch4_main_engine_factor",
        when(df["main_engine_fueltype"] == "Methanol", var["ch4_main_engine_methanol"])
        .when(df["main_engine_enginetype"] == "SSD", var["ch4_main_engine_ssd"])
        .when(df["main_engine_enginetype"] == "MSD", var["ch4_main_engine_msd"])
        .when(df["main_engine_enginetype"] == "HSD", var["ch4_main_engine_hsd"])
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["ch4_main_engine_lng_otto_ss"],
        )
        .when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            var["ch4_main_engine_lng_otto_ms"],
        )
        .when(
            df["main_engine_enginetype"] == "LNG-Diesel",
            var["ch4_main_engine_lng_diesel"],
        )
        .when(df["main_engine_enginetype"] == "LBSI", var["ch4_main_engine_lng_lbsi"])
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["ch4_main_engine_gt_lng"],
        )
        .when(df["main_engine_enginetype"] == "GT", var["ch4_main_engine_gt"])
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["ch4_main_engine_st_lng"],
        )
        .when(df["main_engine_enginetype"] == "ST", var["ch4_main_engine_st"]),
    )

    return df


def set_ch4_aux_boiler_factor(df: DataFrame, var: dict, engines: list) -> DataFrame:
    """
    Sets the `ch4` (methane) factor in `g/kWh` for aux and boiler.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary column  'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.
    engines: List[str]
        A list of engine types, ie. aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the CH4 factor values added as a new column.
    """
    for engine in engines:
        df = df.withColumn(
            f"ch4_{engine}_factor",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var[f"ch4_{engine}_distillate_fuel"],
            )
            .when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"ch4_{engine}_residual_fuel"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var[f"ch4_{engine}_lng"]),
        )

    return df


def calculate_ch4(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `ch4` (methane) emission in tonnes for each engine type 'main_engine', 'aux', 'boiler', based on their
    respective factors, i.e. 'ch4_main_engine_factor_kwh', 'ch4_aux_factor_kwh', 'ch4_boiler_factor_kwh', and the
    energy consumption of each engine in kWh, i.e. 'main_engine_kwh', 'aux_kwh', 'boiler_kwh',
    respectively. The calculation is based on the formula CH4_emission = factor * kWh_consumption / 1000.
    (g / kwh => divide by 1 000 000 to get ton)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing the data to be processed.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the CH4 emission values in ton.
    """
    for engine in engines:
        df = df.withColumn(
            f"ch4_{engine}_ton",
            df[f"ch4_{engine}_factor"]
            * df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000,
        )

    return df
