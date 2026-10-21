from pyspark.sql import DataFrame
from pyspark.sql.functions import col, least, lit, when, year

LARGE_PASSENGER_GT_THRESHOLD = 10_000


def set_nox_main_engine_factor(df: DataFrame, var: dict) -> DataFrame:
    """Set `NOx` (Nitrogen Oxides) factor in gram based on fuel type, rpm, and year.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns "main_engine_fueltype",
        "main_engine_rpm", "year_of_build", and "main_engine_enginetype".

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    -------
    pyspark.sql.DataFrame
        A new dataframe with the additional column "nox_main_engine_factor".

    """
    default_nox_main_engine_factor_global = (
        # Gas turbine (GT):
        when(
            (df["main_engine_enginetype"] == "GT")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nox_main_engine_gt_lng"],
        )
        .when(df["main_engine_enginetype"] == "GT", var["nox_main_engine_gt"])
        # Steam turbine (ST):
        .when(
            (df["main_engine_enginetype"] == "ST")
            & (df["main_engine_fueltype"] == "LNG"),
            var["nox_main_engine_st_lng"],
        )
        .when(df["main_engine_enginetype"] == "ST", var["nox_main_engine_st"])
        # LNG Otto:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            var["nox_main_engine_lng_otto_ss"],
        )
        .when(
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
        )
    )

    if "engine_nox_rating" in df.columns:
        default_nox_main_engine_factor_global = when(
            (df["engine_nox_rating"] > 0) & (df["engine_nox_rating"] < 60),
            df["engine_nox_rating"],
        ).otherwise(default_nox_main_engine_factor_global)

    df = df.withColumn(
        "nox_main_engine_factor_global",
        default_nox_main_engine_factor_global,
    )

    return df


def set_nox_main_engine_eca_factor(df: DataFrame, var: dict) -> DataFrame:
    """Set `NOx` factors for rows inside the NOx emission control area.

    This runs on the vessel table, before the join with AIS positions, so it
    cannot see which NOx area a position is in. Whether Tier III applies in a
    given area is decided per position in `calculate_nox`.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns "main_engine_fueltype",
        "main_engine_rpm", "year_of_build", "main_engine_enginetype", and "tier_group_nox".

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    -------
    pyspark.sql.DataFrame
        A new dataframe with the additional column "nox_main_engine_eca_factor".

    """
    default_nox_main_engine_factor_eca = (
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
        ).otherwise(df["nox_main_engine_factor_global"])
    )

    df = df.withColumn(
        "nox_main_engine_factor_eca",
        default_nox_main_engine_factor_eca,
    )

    return df


def set_nox_aux_boiler_factor(df: DataFrame, var: dict, engines: list) -> DataFrame:
    """Set `NOx` (Nitrogen Oxides) factor for auxiliary engines and boiler based on fuel type.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns fuel_type.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    engines: List[str]
        A list of engine types, ie. aux, boiler

    Returns:
    -------
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

        # Override: For large passenger vessels (Cruise and Passasjer with
        # GT > 10 000), use the lower of the tier-differentiated main engine
        # NOx factor and the default aux factor. This reduces NOx for
        # Tier II/III vessels (where main engine factor < 11.2 g/kWh due to
        # SCR or equivalent abatement) while keeping Tier 0/I unchanged
        # (where the main engine factor > 11.2 g/kWh).
        if engine == "aux":
            is_large_passenger = df["vessel_type_maru"].isin("Cruise", "Passasjer") & (
                df["gt"] > LARGE_PASSENGER_GT_THRESHOLD
            )
            df = df.withColumn(
                f"nox_{engine}_factor_global",
                when(
                    is_large_passenger,
                    least(
                        df["nox_main_engine_factor_global"],
                        df[f"nox_{engine}_factor_global"],
                    ),
                ).otherwise(df[f"nox_{engine}_factor_global"]),
            )
            df = df.withColumn(
                f"nox_{engine}_factor_eca",
                when(
                    is_large_passenger,
                    least(
                        df["nox_main_engine_factor_eca"],
                        df[f"nox_{engine}_factor_eca"],
                    ),
                ).otherwise(df[f"nox_{engine}_factor_eca"]),
            )

    return df


def calculate_nox(df: DataFrame, engines: list) -> DataFrame:
    """Calculate `NOx` (Nitrogen Oxides) emissions in `tonnes` for different engine types and boilers.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Input dataframe containing columns for each engine category
        and boiler.

    engines: List[str]
        A list of engine types, ie. main_engine, aux, boiler

    Returns:
    -------
    pyspark.sql.DataFrame
        A new dataframe with the additional columns for NOx emissions
        for each engine category and boiler.

    """
    # Tier III applies in a NOx area only to ships built in or after the year
    # the area took effect: 2021 for the North Sea and Baltic, 2026 for the
    # Norwegian Sea and Canadian Arctic (MARPOL Annex VI reg. 13). Older tier
    # group 3 ships use the global (Tier II) factors there. For other tier
    # groups the ECA and global NOx factors are equal.
    built_before_area_start = (col("tier_group_nox") == 3) & (
        col("year_of_build") < year(col("nox_emission_control_area_start_date"))
    )
    use_eca_factor = col("in_nox_emission_control_area") & ~built_before_area_start

    for engine in engines:
        nox_calculation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * df["electric_shore_power_at_berth_reduction_factor"]
            / 1_000_000
        )

        df = df.withColumn(
            f"nox_{engine}_tonnes",
            when(
                use_eca_factor,
                df[f"nox_{engine}_factor_eca"] * nox_calculation,
            ).otherwise(df[f"nox_{engine}_factor_global"] * nox_calculation),
        )

    return df
