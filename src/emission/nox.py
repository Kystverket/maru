from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when


def set_nox_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Set `NOx` (Nitrogen Oxides) factor in gram based on fuel type, rpm, and year.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns "main_engine_fueltype",
        "main_engine_rpm", "year_of_build", and "main_engine_enginetype".

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A new dataframe with the additional column "nox_main_engine_factor".
    """

    df = df.withColumn(
        "nox_main_engine_factor_global",
        # Gas turbine (GT):
        when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nox_main_engine_gt_lng"],
        ).when(df["main_engine_enginetype"] == "GT", var["nox_main_engine_gt"])
        # Steam turbine (ST):
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nox_main_engine_st_lng"],
        ).when(df["main_engine_enginetype"] == "ST", var["nox_main_engine_st"])
        # LNG Otto:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["nox_main_engine_lng_otto_ss"],
        ).when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            var["nox_main_engine_lng_otto_ms"],
        )
        # LBSI:
        .when(
            df["main_engine_enginetype"] == "LBSI",
            var["nox_main_engine_lng_lbsi"],
        )
        # Electric
        .when(
            df["main_engine_fueltype"] == "Electric",
            lit(0),
        )
        # Tier group 0:
        .when(
            df["tier_group_nox"] == 0,
            when(
                df["main_engine_enginetype"] == "SSD", var["nox_main_engine_tier_0_ssd"]
            )
            .when(
                df["main_engine_enginetype"] == "MSD", var["nox_main_engine_tier_0_msd"]
            )
            .when(
                df["main_engine_enginetype"] == "HSD", var["nox_main_engine_tier_0_hsd"]
            )
            .when(
                df["main_engine_enginetype"] == "LNG-Diesel",
                var["nox_main_engine_tier_0_lng_diesel"],
            ),
        )
        # Tier group 1:
        .when(
            df["tier_group_nox"] == 1,
            when(
                df["main_engine_enginetype"] == "LNG-Diesel",
                var["nox_main_engine_tier_1_lng_diesel"],
            )
            .when(
                (df["main_engine_rpm"] > 0) & (df["main_engine_rpm"] < 130),
                var["nox_main_engine_tier_1_rpm_1_129"],
            )
            .when(
                (df["main_engine_rpm"] >= 130) & (df["main_engine_rpm"] < 2000),
                45 * df["main_engine_rpm"] ** (-0.2),
            )
            .when(
                df["main_engine_rpm"] >= 2000, var["nox_main_engine_tier_1_rpm_2000_"]
            )
            .when(
                df["main_engine_enginetype"] == "SSD", var["nox_main_engine_tier_1_ssd"]
            )
            .when(
                df["main_engine_enginetype"] == "MSD", var["nox_main_engine_tier_1_msd"]
            )
            .when(
                df["main_engine_enginetype"] == "HSD", var["nox_main_engine_tier_1_hsd"]
            ),
        )
        # Tier group 2 and 3 (tier group 3 only applies in ECA):
        .when(
            df["tier_group_nox"].isin(2, 3),
            when(
                df["main_engine_enginetype"] == "LNG-Diesel",
                var["nox_main_engine_tier_2_lng_diesel"],
            )
            .when(
                (df["main_engine_rpm"] > 0) & (df["main_engine_rpm"] < 130),
                var["nox_main_engine_tier_2_rpm_1_129"],
            )
            .when(
                (df["main_engine_rpm"] >= 130) & (df["main_engine_rpm"] < 2000),
                44 * df["main_engine_rpm"] ** (-0.23),
            )
            .when(
                df["main_engine_rpm"] >= 2000, var["nox_main_engine_tier_2_rpm_2000_"]
            )
            .when(
                df["main_engine_enginetype"] == "SSD", var["nox_main_engine_tier_2_ssd"]
            )
            .when(
                df["main_engine_enginetype"] == "MSD", var["nox_main_engine_tier_2_msd"]
            )
            .when(
                df["main_engine_enginetype"] == "HSD", var["nox_main_engine_tier_2_hsd"]
            ),
        ),
    )

    return df


def set_nox_main_engine_eca_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Set `NOx` (Nitrogen Oxides) factor in gram based on fuel type, rpm, and year for ECA (Emission Control Areas).

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns "main_engine_fueltype",
        "main_engine_rpm", "year_of_build", "main_engine_enginetype", and "tier_group_nox".

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        A new dataframe with the additional column "nox_main_engine_eca_factor".
    """

    df = df.withColumn(
        "nox_main_engine_factor_eca",
        # Tier group 3:
        when(
            df["tier_group_nox"] == 3,
            when(
                df["main_engine_enginetype"] == "LNG-Diesel",
                var["nox_main_engine_tier_3_lng_diesel"],
            )
            # Electric
            .when(
                df["main_engine_fueltype"] == "Electric",
                lit(0),
            )
            .when(
                (df["main_engine_rpm"] > 0) & (df["main_engine_rpm"] < 130),
                var["nox_main_engine_tier_3_rpm_1_129"],
            )
            .when(
                (df["main_engine_rpm"] >= 130) & (df["main_engine_rpm"] < 2000),
                9 * df["main_engine_rpm"] ** (-0.2),
            )
            .when(
                df["main_engine_rpm"] >= 2000, var["nox_main_engine_tier_3_rpm_2000_"]
            )
            .when(
                df["main_engine_enginetype"] == "SSD", var["nox_main_engine_tier_3_ssd"]
            )
            .when(
                df["main_engine_enginetype"] == "MSD", var["nox_main_engine_tier_3_msd"]
            )
            .when(
                df["main_engine_enginetype"] == "HSD", var["nox_main_engine_tier_3_hsd"]
            )
            .otherwise(df["nox_main_engine_factor_global"]),
        ).otherwise(df["nox_main_engine_factor_global"]),
    )

    return df


def set_nox_aux_boiler_factor(df: DataFrame, var: dict, engines: list) -> DataFrame:
    """
    Set `NOx` (Nitrogen Oxides) factor for auxiliary engines and boiler based on fuel type.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns fuel_type.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    engines: List[str]
        A list of engine types, ie. aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A new dataframe with the additional columns for eca and global factors for each engine.
    """

    for engine in engines:
        df = df.withColumn(
            f"nox_{engine}_factor_global",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"nox_{engine}_residual_fuel"],
            )
            .when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var[f"nox_{engine}_distillate_fuel"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var[f"nox_{engine}_lng"]),
        )
        # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:
        df = df.withColumn(
            f"nox_{engine}_factor_eca",
            when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var[f"nox_{engine}_distillate_fuel"],
            ).otherwise(df[f"nox_{engine}_factor_global"]),
        )

    return df


def calculate_nox(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculate `NOx` (Nitrogen Oxides) emissions in `tonnes` for different engine types and boilers.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns for each engine category
        and boiler.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        A new dataframe with the additional columns for NOx emissions
        for each engine category and boiler.
    """
    for engine in engines:
        nox_calculation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000
        )

        df = df.withColumn(
            f"nox_{engine}_tonnes",
            when(
                df["eca"],
                df[f"nox_{engine}_factor_eca"] * nox_calculation,
            ).otherwise(df[f"nox_{engine}_factor_global"] * nox_calculation),
        )

    return df
