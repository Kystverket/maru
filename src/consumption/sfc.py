from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when


def set_sfc_main_engine_baseline(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the Specific Fuel Consumption (SFC) baseline for aux according to their fuel type.

    This function expects a Spark DataFrame `df` as input and returns a new DataFrame with an additional column named `sfc_aux_baseline`.

    The SFC baseline is set based on the value of `main_engine_fueltype` and `year`.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing a column named `main_engine_fueltype` and `year`.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns
    -------
    pyspark.sql.DataFrame
        New DataFrame with an additional column named `sfc_main_engine_baseline`.

    """

    df = df.withColumn(
        "sfc_main_engine_baseline",
        # Slow speed diesel (SSD):
        when(
            df["main_engine_enginetype"] == "SSD",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",  # MDO
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_distillate_fuel_ssd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_distillate_fuel_ssd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_distillate_fuel_ssd_year_2001_2999"],
                ),
            )
            .when(
                df["main_engine_fueltype"] == "Residual Fuel",  # HFO
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_residual_fuel_ssd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_residual_fuel_ssd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_residual_fuel_ssd_year_2001_2999"],
                ),
            )
            .when(
                df["main_engine_fueltype"] == "Methanol",
                var["sfc_main_engine_methanol_ssd"],
            ),
        )
        # Medium speed diesel (MSD):
        .when(
            df["main_engine_enginetype"] == "MSD",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_distillate_fuel_msd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_distillate_fuel_msd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_distillate_fuel_msd_year_2001_2999"],
                ),
            )
            .when(
                df["main_engine_fueltype"] == "Residual Fuel",
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_residual_fuel_msd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_residual_fuel_msd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_residual_fuel_msd_year_2001_2999"],
                ),
            )
            .when(
                df["main_engine_fueltype"] == "Methanol",
                var["sfc_main_engine_methanol_msd"],
            ),
        )
        # High speed diesel (HSD):
        .when(
            df["main_engine_enginetype"] == "HSD",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_distillate_fuel_hsd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_distillate_fuel_hsd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_distillate_fuel_hsd_year_2001_2999"],
                ),
            ).when(
                df["main_engine_fueltype"] == "Residual Fuel",
                when(
                    df["year_of_build"] <= 1983,
                    var["sfc_main_engine_residual_fuel_hsd_year_0_1983"],
                )
                .when(
                    (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                    var["sfc_main_engine_residual_fuel_hsd_year_1984_2000"],
                )
                .when(
                    df["year_of_build"] > 2000,
                    var["sfc_main_engine_residual_fuel_hsd_year_2001_2999"],
                ),
            ),
        )
        # LNG (Liquified natural gas) Diesel:
        .when(
            df["main_engine_enginetype"] == "LNG-Diesel",
            when(
                df["year_of_build"] >= 2001,
                var["sfc_main_engine_lng_diesel_year_2001_2999"],
            ),
        )
        # Slow speed LNG otto:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto SS",
            when(
                df["year_of_build"] >= 2001,
                var["sfc_main_engine_lng_otto_ss_year_2001_2999"],
            ),
        )
        # Medium speed LNG otto:
        .when(
            df["main_engine_enginetype"] == "LNG-Otto MS",
            when(
                df["year_of_build"] <= 2000,
                var["sfc_main_engine_lng_otto_ms_year_0_2000"],
            ).when(
                df["year_of_build"] >= 2001,
                var["sfc_main_engine_lng_otto_ms_year_2001_2999"],
            ),
        )
        # LBSI:
        .when(
            df["main_engine_enginetype"] == "LBSI",
            when(
                df["year_of_build"] <= 2000, var["sfc_main_engine_lng_lbsi_year_0_2000"]
            ).when(
                df["year_of_build"] >= 2001,
                var["sfc_main_engine_lng_lbsi_year_2001_2999"],
            ),
        )
        # ST:
        .when(
            df["main_engine_enginetype"] == "ST",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var["sfc_main_engine_distillate_fuel_st"],
            )
            .when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var["sfc_main_engine_residual_fuel_st"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var["sfc_main_engine_lng_st"]),
        )
        # GT:
        .when(
            df["main_engine_enginetype"] == "GT",
            when(
                df["main_engine_fueltype"] == "Distillate Fuel",
                var["sfc_main_engine_distillate_fuel_gt"],
            )
            .when(
                df["main_engine_fueltype"] == "Residual Fuel",
                var["sfc_main_engine_residual_fuel_gt"],
            )
            .when(df["main_engine_fueltype"] == "LNG", var["sfc_main_engine_lng_gt"]),
        ),
    )

    return df


def set_sfc_pilot_fuel(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the pilot fuel factor for Specific Fuel Consumption (SFC) baseline.

    Reference: IMO GHG 4.study page 70(98):
    'It is assumed that the dual-fuel LNG engines always operate on LNG as their primary fuel while the mass of pilot fuel injected remains constant across engine loads.'

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing a column named `main_engine_fueltype` and `year`.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns
    -------
    pyspark.sql.DataFrame
        New DataFrame with an additional column named `sfc_pilot_fuel`.

    """

    df = df.withColumn(
        "sfc_pilot_fuel",
        # LNG (Liquified natural gas) Diesel:
        when(
            (df["main_engine_enginetype"] == "LNG-Diesel")
            & (df["year_of_build"] >= 2001),
            var["sfc_main_engine_lng_diesel_year_2001_2999_pilot_fuel"],
        )
        # Slow speed LNG otto:
        .when(
            (df["main_engine_enginetype"] == "LNG-Otto SS")
            & (df["year_of_build"] >= 2001),
            var["sfc_main_engine_lng_otto_ss_year_2001_2999_pilot_fuel"],
        ).otherwise(lit(0)),
    )

    return df


def set_sfc_aux_baseline(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the Specific Fuel Consumption (SFC) baseline for aux according to their fuel type.

    This function expects a Spark DataFrame `df` as input and returns a new DataFrame with an additional column named `sfc_aux_baseline`.

    The SFC baseline is set based on the value of `main_engine_fueltype` and `year`.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing a column named `main_engine_fueltype` and `year`.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns
    -------
    pyspark.sql.DataFrame
        New DataFrame with an additional column named `sfc_aux_baseline`.

    """

    df = df.withColumn(
        "sfc_aux_baseline",
        when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            when(
                df["year_of_build"] <= 1983, var["sfc_aux_distillate_fuel_year_0_1983"]
            )
            .when(
                (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                var["sfc_aux_distillate_fuel_year_1984_2000"],
            )
            .when(
                df["year_of_build"] > 2000,
                var["sfc_aux_distillate_fuel_year_2001_2999"],
            ),
        )
        .when(
            df["main_engine_fueltype"] == "Residual Fuel",
            when(df["year_of_build"] <= 1983, var["sfc_aux_residual_fuel_year_0_1983"])
            .when(
                (df["year_of_build"] > 1983) & (df["year_of_build"] <= 2000),
                var["sfc_aux_residual_fuel_year_1984_2000"],
            )
            .when(
                df["year_of_build"] > 2000, var["sfc_aux_residual_fuel_year_2001_2999"]
            ),
        )
        .when(
            df["main_engine_fueltype"] == "LNG",
            when(
                df["year_of_build"] <= 2000,
                var["sfc_aux_lng_year_0_2000"],
            ).otherwise(var["sfc_aux_lng_year_2001_2999"]),
        ),
    )

    return df


def set_sfc_boiler_baseline(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the Specific Fuel Consumption (SFC) baseline for boilers according to their fuel type.

    This function expects a Spark DataFrame `df` as input and returns a new DataFrame with an additional column named `sfc_boiler_baseline`.

    The SFC baseline is set based on the value of `main_engine_fueltype`.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing a column named `main_engine_fueltype`.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns
    -------
    pyspark.sql.DataFrame
        New DataFrame with an additional column named `sfc_boiler_baseline`.

    """

    df = df.withColumn(
        "sfc_boiler_baseline",
        when(
            df["main_engine_fueltype"] == "Distillate Fuel",
            var["sfc_boiler_distillate_fuel"],
        )
        .when(
            df["main_engine_fueltype"] == "Residual Fuel",
            var["sfc_boiler_residual_fuel"],
        )
        .when(df["main_engine_fueltype"] == "LNG", var["sfc_boiler_lng"]),
    )

    return df


def calculate_sfc_load(df: DataFrame) -> DataFrame:
    """
    Calculates the SFC (Specific Fuel Consumption) load based on SFC baseline and Load factor.

    The calculation is based on Equation 10 from the Fourth IMO GHG Study 2020. Annexes B.2:

    SFOC_load = SFOC_baseline * (0.455 * load^2 - 0.71 * load + 1.28)

    'Where SFOC load is the specific fuel oil consumption (SFOC) at a given engine load, SFOC baseline is the lowest
    SFOC for a given engine. The SFOC curves for marine engines are u-shaped: SFOC is higher at lower loads,
    gets lower until it reaches a minimum, and begins to increase again at higher loads. The Third IMO GHG
    Study 2014 showed that this equation satisfactorily described how SFOC changes as a function of engine load
    when SFOCs are optimized (i.e., lowest) at 80% load (Figure 6).'

    'It is important to highlight that Equation (10) only applies to propulsion systems that use internal combustion
    engines, highlighted as engines one to eight in Table 10. Unlike for oil and LNG engines, SFCME values for
    gas and steam turbines (GT & ST) are assumed to be not dependent on the engine load and, hence the SFCME for these
    engine types are always assumed to be the SFC baseline'

    'It is assumed that the dual-fuel LNG engines always operate on LNG as their primary fuel while the mass of pilot fuel injected remains constant across engine loads.'

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing columns named `main_engine_load_factor`, as well as
        the SFC baselines calculated by the `set_sfc_main_engine_baseline`, `set_sfc_aux_baseline`, and `set_sfc_boiler_baseline`
        functions.

    Returns
    -------
    pyspark.sql.DataFrame
        New DataFrame with additional columns named `sfc_main_engine`, `sfc_aux`, and `sfc_boiler`.

    """

    df = (
        df.withColumn(
            "sfc_main_engine",
            when(
                df["main_engine_enginetype"].isin("GT", "ST"),
                df["sfc_main_engine_baseline"],
            ).otherwise(
                (
                    df["sfc_main_engine_baseline"]
                    * (
                        0.455 * df["main_engine_load_factor"] ** 2
                        - 0.71 * df["main_engine_load_factor"]
                        + 1.28
                    )
                )
                + df["sfc_pilot_fuel"]
            ),
        )
        .withColumn("sfc_aux", df["sfc_aux_baseline"])
        .withColumn("sfc_boiler", df["sfc_boiler_baseline"])
    )

    return df
