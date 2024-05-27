from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_co_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `co` (Carbon Monoxide) factor in `g/kWh` for main engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary columns: 'main_engine_enginetype', 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the co factor values added as a new column.
    """

    df = df.withColumn(
        "co_main_engine_factor_global",
        # Methanol:
        when(df["main_engine_fueltype"] == "Methanol", var["co_main_engine_methanol"])
        # SSD:
        .when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_ssd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["co_main_engine_ssd_distillate_fuel"],
        )
        # MSD:
        .when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_msd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["co_main_engine_msd_distillate_fuel"],
        )
        # HSD:
        .when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_hsd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["co_main_engine_hsd_distillate_fuel"],
        )
        # GT:
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["co_main_engine_gt_lng"],
        )
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["co_main_engine_gt_distillate_fuel"],
        )
        .when(df["main_engine_enginetype"] == "GT", var["co_main_engine_gt"])
        # ST:
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["co_main_engine_st_lng"],
        )
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["co_main_engine_st_distillate_fuel"],
        )
        .when(df["main_engine_enginetype"] == "ST", var["co_main_engine_st"])
        # LNG:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["co_main_engine_lng_otto_ss"],
        )
        .when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            var["co_main_engine_lng_otto_ms"],
        )
        .when(df["main_engine_enginetype"] == "LBSI", var["co_main_engine_lng_lbsi"])
        .when(
            df["main_engine_enginetype"] == "LNG-Diesel",
            var["co_main_engine_lng_diesel"],
        ),
    )

    # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:

    df = df.withColumn(
        "co_main_engine_factor_eca",
        # SSD:
        when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_ssd_distillate_fuel"],
        )
        # MSD:
        .when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_msd_distillate_fuel"],
        )
        # HSD:
        .when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["co_main_engine_hsd_distillate_fuel"],
        ).otherwise(df["co_main_engine_factor_global"]),
    )

    return df


def set_co_aux_boiler_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `co` (Carbon Monoxide)  factor in `g/kWh` for aux engine.

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
        A PySpark DataFrame object with the co factor values added as a new column.
    """
    aux_boiler_engines = ["aux", "boiler"]

    for engine in aux_boiler_engines:
        df = df.withColumn(
            f"co_{engine}_factor_global",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"co_{engine}_residual_fuel"],
            )
            .when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var[f"co_{engine}_distillate_fuel"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var[f"co_{engine}_lng"]),
        )

        # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:
        df = df.withColumn(
            f"co_{engine}_factor_eca",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"co_{engine}_distillate_fuel"],
            ).otherwise(df[f"co_{engine}_factor_global"]),
        )

    return df


def calculate_co(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `co` (Carbon Monoxide)  emission in `ton` for each engine type 'main_engine', 'aux', 'boiler', based on their
    respective factors, i.e. 'co_main_engine_factor_kwh', 'co_aux_factor_kwh', 'co_boiler_factor_kwh', and the
    energy consumption of each engine in kWh, i.e. 'main_engine_kwh', 'aux_kwh', 'boiler_kwh',
    respectively. The calculation is based on the formula co_emission = factor * kWh_consumption / 1 000 000.
    (g / kwh => divide by 1 000 000 to get ton)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing the data to be processed.

    engines: List[str]
        A list of engine types to calculate CO factor for.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the co emission values in ton.
    """
    for engine in engines:
        co_calulation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000
        )

        df = df.withColumn(
            f"co_{engine}_ton",
            when(
                df["eca"],
                df[f"co_{engine}_factor_eca"] * co_calulation,
            ).otherwise(df[f"co_{engine}_factor_global"] * co_calulation),
        )

    return df
