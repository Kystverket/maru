from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when


def set_main_engine_bc_factor(df: DataFrame, var: dict) -> DataFrame:
    """
    Sets the `bc` (Black Carbon) emission factor for the main engine based on the fuel type, stroke type, and load type.

    The reference is the IMO GHG 4. study page 77 (105):
    - Two-stroke engines operating on residual fuel (e.g., HFO).
        Equation 18: 1.500*10^-4 * (Load ^-0.359)
    - Two-stroke engines operating on distillate fuel (e.g., MDO or MGO).
        Equation 19: 3.110 * 10^-5 * (Load ^-0.397)
    - Four-stroke engines operating on residual fuel (e.g., HFO).
        Equation 20: 2.500 * 10^-4 * (Load ^-0.968)
    - Four-stroke engines operating on distillate fuel (e.g., MDO or MGO).
        Equation 21: 1.201 * 10^-4 * (Load ^-1.124)
    - For methanol-fueled engines, the fuel-based BC emission factor is assumed to be 90% less than the HFO fuel-based BC emission factor (IMO, 2017) and it is assumed that the same reduction would be seen in the energy-based emission factor.
    - LNG-fueled engines according to table 65, page 415 (443)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation: main_engine_fueltype, stroketype, main_engine_load_factor, main_engine_enginetype, and main_engine_fueltype.

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with an additional column "bc_main_engine_factor" containing the black carbon emission factor.

    """

    df = df.withColumn(
        "bc_main_engine_factor_global",
        when(
            (df["main_engine_enginetype"].isin("SSD", "MSD", "HSD")),
            when(
                df["main_engine_load_factor"] > 0,
                when(
                    (df["main_engine_fueltype"] == "Residual Fuel")
                    & (df["stroketype"] == 2),
                    1.500
                    * 10**-4
                    * (df["main_engine_load_factor"] ** -0.359),  # Equation 18
                )
                .when(
                    (df["main_engine_fueltype"] == "Residual Fuel")
                    & (df["stroketype"] == 4),
                    2.500
                    * 10**-4
                    * (df["main_engine_load_factor"] ** -0.968),  # Equation 20
                )
                .when(
                    (df["main_engine_fueltype"] == "Distillate Fuel")
                    & (df["stroketype"] == 2),
                    3.110
                    * 10**-5
                    * (df["main_engine_load_factor"] ** -0.397),  # Equation 19
                )
                .when(
                    (df["main_engine_fueltype"] == "Distillate Fuel")
                    & (df["stroketype"] == 4),
                    1.201
                    * 10**-4
                    * (df["main_engine_load_factor"] ** -1.124),  # Equation 21
                )
                .when(
                    (df["main_engine_fueltype"] == "Methanol")
                    & (df["stroketype"] == 2),
                    (1.500 * 10**-4 * (df["main_engine_load_factor"] ** -0.359)) * 0.1,
                )
                .when(
                    (df["main_engine_fueltype"] == "Methanol")
                    & (df["stroketype"] == 4),
                    (2.500 * 10**-4 * (df["main_engine_load_factor"] ** -0.968)) * 0.1,
                ),
            ).otherwise(lit(0)),
        )
        .when(
            (df["main_engine_fueltype"] == "LNG"),
            when(
                df["main_engine_enginetype"] == "LNG-Diesel",
                var["bc_main_engine_lng_diesel"],
            )
            .when(
                df["main_engine_enginetype"] == "LNG-Otto SS",
                var["bc_main_engine_lng_otto_ss"],
            )
            .when(
                df["main_engine_enginetype"] == "LNG-Otto MS",
                var["bc_main_engine_lng_otto_ms"],
            )
            .when(
                df["main_engine_enginetype"] == "LBSI",
                var["bc_main_engine_lng_lbsi"],
            )
            .when(
                df["main_engine_enginetype"] == "GT",
                var["bc_main_engine_lng_gt"],
            )
            .when(
                df["main_engine_enginetype"] == "ST",
                var["bc_main_engine_lng_st"],
            ),
        )
        .when(
            (df["main_engine_fueltype"] == "Residual Fuel")
            & (df["main_engine_enginetype"] == "GT"),
            var["bc_main_engine_residual_fuel_gt"],
        )
        .when(
            (df["main_engine_fueltype"] == "Residual Fuel")
            & (df["main_engine_enginetype"] == "ST"),
            var["bc_main_engine_residual_fuel_st"],
        )
        .when(
            (df["main_engine_fueltype"] == "Distillate Fuel")
            & (df["main_engine_enginetype"] == "GT"),
            var["bc_main_engine_residual_fuel_st"],
        )
        .when(
            (df["main_engine_fueltype"] == "Distillate Fuel")
            & (df["main_engine_enginetype"] == "ST"),
            var["bc_main_engine_residual_fuel_st"],
        ),
    )

    # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA. The equation :
    df = df.withColumn(
        "bc_main_engine_factor_eca",
        when(
            df["main_engine_load_factor"] > 0,
            # Equation 19:
            when(
                (df["main_engine_fueltype"] == "Residual Fuel")
                & (df["stroketype"] == 2),
                3.110 * 10**-5 * (df["main_engine_load_factor"] ** -0.397),
            )
            # Equation 21:
            .when(
                (df["main_engine_fueltype"] == "Residual Fuel")
                & (df["stroketype"] == 4),
                1.201 * 10**-4 * (df["main_engine_load_factor"] ** -1.124),
            ).otherwise(df["bc_main_engine_factor_global"]),
        ).otherwise(lit(0)),
    )

    return df


def set_bc_aux_boiler_factor(df: DataFrame, var: dict, engines: list) -> DataFrame:
    """
    Set the `bc` (Black Carbon) factor for auxiliary engine based on fuel type.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing the necessary column 'main_engine_fueltype'

    var: dict
        A dictionary containing input variables for different
        emission factors.

    engines: List[str]
        A list of engine types, ie. aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with columns 'bc_aux_factor_kw_global','bc_aux_factor_kw_eca', and 'bc_aux_factor_fuel'
    """
    for engine in engines:
        df = df.withColumn(
            f"bc_{engine}_factor_global",
            when(
                (df["main_engine_fueltype"] == "Residual Fuel"),
                var[f"bc_{engine}_residual_fuel"],
            )
            .when(
                (df["main_engine_fueltype"] == "Distillate Fuel"),
                var[f"bc_{engine}_distillate_fuel"],
            )
            .when((df["main_engine_fueltype"] == "LNG"), var[f"bc_{engine}_lng"]),
        )

        df = df.withColumn(
            f"bc_{engine}_factor_eca",
            # Change from HFO (Residual Fuel) to MDO (Distillate Fuel) in ECA:
            when(
                (df["main_engine_fueltype"] == "Residual Fuel"),
                var[f"bc_{engine}_distillate_fuel"],
            ).otherwise(df[f"bc_{engine}_factor_global"]),
        )

    return df


def calculate_bc_main_engine(df: DataFrame) -> DataFrame:
    """
    Calculate the `bc` (Black Carbon) emissions for the main engine based on the bc factor and kwh consumption or fuel consumption.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
         Spark DataFrame containing necessary columns to perform BC main engine calculations: 'main_engine_fueltype', 'bc_main_engine_factor', 'main_engine_kwh', and 'main_engine_fueltype'.

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column 'bc_main_engine_tonnes' containing the black carbon mass emissions in tonnes.
    """

    # Reduce kwh with degree of electrification and convert gram to ton:
    bc_kwh_calculation = (
        df["main_engine_kwh"] * (1 - df["degree_of_electrification"]) / 1_000_000
    )

    # Calculate column:
    df = df.withColumn(
        "bc_main_engine_tonnes",
        when(
            df["eca"],
            when(
                (df["main_engine_fueltype"] == "LNG")
                | (df["main_engine_enginetype"].isin("ST", "GT")),
                df["bc_main_engine_factor_eca"] * bc_kwh_calculation,
            ).otherwise(
                df["bc_main_engine_factor_eca"] * df["main_engine_fuel_tonnes"]
            ),
        ).otherwise(
            when(
                (df["main_engine_fueltype"] == "LNG")
                | (df["main_engine_enginetype"].isin("ST", "GT")),
                df["bc_main_engine_factor_global"] * bc_kwh_calculation,
            ).otherwise(
                df["bc_main_engine_factor_global"] * df["main_engine_fuel_tonnes"]
            ),
        ),
    )

    return df


def calculate_bc_aux_boiler(df: DataFrame, engines: list) -> DataFrame:
    """
    Calculates the `bc` (Black Carbon) emissions of the auxiliary engine and boiler based on the bc aux/boiler factor and kwh consumption.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing the necessary columns for BC auxiliary engine calculation: 'eca', 'degree_of_electrification', bc_{engine}_factor_eca', 'bc_{engine}_factor_global' and '{engine}_kwh'

    engines: list
        A list of engines, ie. aux, boiler

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column 'bc_aux' containing the black carbon mass emissions in tonnes
    """

    for engine in engines:
        # Reduce kwh with degree of electrification and convert gram to tonnes:
        bc_calculation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000
        )

        # Calculate column:
        df = df.withColumn(
            f"bc_{engine}_tonnes",
            when(
                df["eca"],
                df[f"bc_{engine}_factor_eca"] * bc_calculation,
            ).otherwise(df[f"bc_{engine}_factor_global"] * bc_calculation),
        )
    return df
