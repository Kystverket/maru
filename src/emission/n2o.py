from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_n2o_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `n2o` (Nitrous Oxide) factor in `g/kWh` for main engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary columns: 'main_engine_enginetype', 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    -----------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the n2o factor values added as a new column.
    """

    df = df.withColumn(
        "n2o_main_engine_factor_global",
        # Methanol:
        when(df["main_engine_fueltype"] == "Methanol", var["n2o_main_engine_methanol"])
        # SSD:
        .when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_ssd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["n2o_main_engine_ssd_distillate_fuel"],
        )
        # MSD:
        .when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_msd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["n2o_main_engine_msd_distillate_fuel"],
        )
        # HSD:
        .when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_hsd_residual_fuel"],
        ).when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["n2o_main_engine_hsd_distillate_fuel"],
        )
        # GT:
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["n2o_main_engine_gt_lng"],
        )
        .when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["n2o_main_engine_gt_distillate_fuel"],
        )
        .when(df["main_engine_enginetype"] == "GT", var["n2o_main_engine_gt"])
        # ST:
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["n2o_main_engine_st_lng"],
        )
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "Distillate Fuel"),
            var["n2o_main_engine_st_distillate_fuel"],
        )
        .when(df["main_engine_enginetype"] == "ST", var["n2o_main_engine_st"])
        # LNG:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["n2o_main_engine_lng_otto_ss"],
        )
        .when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            var["n2o_main_engine_lng_otto_ms"],
        )
        .when(df["main_engine_enginetype"] == "LBSI", var["n2o_main_engine_lng_lbsi"])
        .when(
            df["main_engine_enginetype"] == "LNG-Diesel",
            var["n2o_main_engine_lng_diesel"],
        ),
    )

    # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:

    df = df.withColumn(
        "n2o_main_engine_factor_eca",
        # SSD:
        when(
            (df["main_engine_enginetype"] == "SSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_ssd_distillate_fuel"],
        )
        # MSD:
        .when(
            (df["main_engine_enginetype"] == "MSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_msd_distillate_fuel"],
        )
        # HSD:
        .when(
            (df["main_engine_enginetype"] == "HSD")
            & (df["main_engine_fueltype"] == "Residual Fuel"),
            var["n2o_main_engine_hsd_distillate_fuel"],
        ).otherwise(df["n2o_main_engine_factor_global"]),
    )

    return df


def set_n2o_aux_boiler_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `n2o` (Nitrous Oxide) factor in `g/kWh` for aux engine.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing containing the necessary column: 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    -----------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the n2o factor values added as a new column.
    """
    aux_boiler_engines = ["aux", "boiler"]

    for engine in aux_boiler_engines:
        df = df.withColumn(
            f"n2o_{engine}_factor_global",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"n2o_{engine}_residual_fuel"],
            )
            .when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var[f"n2o_{engine}_distillate_fuel"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var[f"n2o_{engine}_lng"]),
        )

        # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:
        df = df.withColumn(
            f"n2o_{engine}_factor_eca",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"n2o_{engine}_distillate_fuel"],
            ).otherwise(df[f"n2o_{engine}_factor_global"]),
        )

    return df


def calculate_n2o(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `n2o` (Nitrous Oxide) emission in tonnes for each engine type 'main_engine', 'aux', 'boiler', based on their
    respective factors, i.e. 'n2o_main_engine_factor_kwh', 'n2o_aux_factor_kwh', 'n2o_boiler_factor_kwh', and the
    energy consumption of each engine in kWh, i.e. 'main_engine_kwh', 'aux_kwh', 'boiler_kwh',
    respectively. The calculation is based on the formula n2o_emission = factor * kWh_consumption / 1 000 000.
    (g / kwh => divide by 1 000 000 to get tonnes)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A PySpark DataFrame object containing the data to be processed.

    engines: List[str]
        A list of engine types to calculate N2O factor for.

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame object with the n2o emission values in tonnes.
    """

    for engine in engines:
        n2o_calulation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * df["electric_shore_power_at_berth_reduction_factor"]
            / 1_000_000
        )

        df = df.withColumn(
            f"n2o_{engine}_tonnes",
            when(
                df["eca"],
                df[f"n2o_{engine}_factor_eca"] * n2o_calulation,
            ).otherwise(df[f"n2o_{engine}_factor_global"] * n2o_calulation),
        )

    return df
