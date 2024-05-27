from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def set_pm_factor(df: DataFrame, var: dict, engines: list, areas: list) -> DataFrame:
    """
    Sets the PM (Particulate Matter) emission factors.

    The PM10’s EFe are a function of the fuel’s sulphur content and are therefore reduced when operating on lower
    sulphur fuels (e.g. when operating in ECAs). For engines being fueled by HFO (Heavy Fuel Oil / Residual fuel) and MDO/MGO (Marine Fuel Oil / Distillate Fuel), the fourth IMO GHG study page 75 (103) estimates PM10 EFe based on the sulphur content reported in Table 22 (Global average fuel sulphur content in percentage) and by using the following formulas:
    - HFO (eq. 16): 1.35 + SFC * 7 * 0.02247 * (FUEL_SULPHUR_FRACTION - 0.0246)
    - MDO/MGO (eq. 17):  0.23 + SFC * 7 * 0.02247 * (FUEL_SULPHUR_FRACTION - 0.024)

    The number 7 in Equations (16) and (17) comes from the molecular weight ratio between sulfate PM and
    Sulphur and 0.02247 reflects the proportion of the sulphur in the fuel that is converted to sulfate PM (US Office
    of Transportation Air Quality, 2020).

    Parameters:
    -----------
    df: A PySpark DataFrame containing the following columns: 'main_engine_fueltype', 'sfc_me', 'sfc_aux', 'sfc_boiler', 'main_engine_enginetype', 'fuel_sulfur_fraction_global', 'fuel_sulfur_fraction_eca'.

    var: Dictionary
        A dictionary containing input variables for different emission factors

    engines: List[str]
        A list of engine types, ie. me, aux, boiler

    areas: List[str]
        A list of areas, ie. eca and global

    Returns:
    --------
    pyspark.sql.DataFrame
        A PySpark DataFrame with additional columns representing the PM emission factor for each engine, as follows: pm_main_engine_global_factor, pm_main_engine_eca_factor, pm_aux_global_factor, pm_aux_eca_factor,
        pm_boiler_global_factor, pm_boiler_eca_factor: The PM emission factor in g/kWh for each engine.
    """

    for engine in engines:
        for area in areas:
            equation_residual_fuel = 1.35 + df[f"sfc_{engine}"] * 7 * 0.02247 * (
                df[f"fuel_sulfur_fraction_{area}"] - 0.0246
            )
            equation_distillate_fuel = 0.23 + df[f"sfc_{engine}"] * 7 * 0.02247 * (
                df[f"fuel_sulfur_fraction_{area}"] - 0.0024
            )

            df = df.withColumn(
                f"pm_{engine}_factor_{area}",
                # Residual fuel (HFO - Heavy Fuel Oil) from IMO GHG 4. study p. 75 (103) equation 17:
                when(
                    df["main_engine_fueltype"] == "Residual Fuel",
                    equation_residual_fuel,
                )
                # Distillate fuel (MFO - Marine Fuel Oil) from IMO GHG 4. study p. 75 (103) equation 16:
                .when(
                    df["main_engine_fueltype"] == "Distillate Fuel",
                    equation_distillate_fuel,
                )
                # Methanol (The PM10 emission factor for methanol is considered to be 10% of the SSD and MSD engines PM10 emission factor when consuming HFO):
                .when(
                    df["main_engine_fueltype"] == "Methanol",
                    equation_residual_fuel * 0.1,
                )
                # LNG:
                .when(
                    df["main_engine_fueltype"] == "LNG",
                    when(
                        df["main_engine_enginetype"] == "LNG-Otto SS",
                        var[f"pm_{engine}_lng_otto_ss"],
                    )
                    .when(
                        df["main_engine_enginetype"] == "LNG-Otto MS",
                        var[f"pm_{engine}_lng_otto_ms"],
                    )
                    .when(
                        df["main_engine_enginetype"] == "LBSI",
                        var[f"pm_{engine}_lng_lbsi"],
                    )
                    .when(
                        df["main_engine_enginetype"] == "GT",
                        var[f"pm_{engine}_lng_gt"],
                    )
                    .when(
                        df["main_engine_enginetype"] == "ST",
                        var[f"pm_{engine}_lng_st"],
                    )
                    .otherwise(var[f"pm_{engine}_lng_otto_ms"]),
                ),
            )

    return df


def calculate_pm_10(df, engines):
    """
    Calculates PM10 (Particulate Matter, particles with a diameter of 10 micrometres or less) emissions in `ton` for each engine type in the given DataFrame.

    Parameters:
    -----------
    df: A PySpark DataFrame containing the following columns: main_engine_kwh', 'aux_kwh', 'boiler_kwh', 'pm_main_engine_factor', 'pm_aux_factor', 'pm_boiler_factor'

    engines: List[str]
        A list of engine types, ie. me, aux, boiler

    Returns:
    --------
    A PySpark DataFrame with additional columns representing the PM10 emission (in ton) for each engine type,
             as follows: pm10_main_engine_ton, pm10_aux_ton, pm10_boiler_ton: The PM10 emission in ton for the main engine, auxiliary engine, and boiler, respectively, based on the PM factor and power output for each engine type.
    """
    for engine in engines:
        pm_calculation = (
            df[f"{engine}_kwh"]
            * (1 - df["degree_of_electrification"])
            * (~df["electric_shore_power_at_berth"]).cast("int")
            / 1_000_000
        )

        df = df.withColumn(
            f"pm10_{engine}_ton",
            when(
                df["eca"],
                df[f"pm_{engine}_factor_eca"] * pm_calculation,
            ).otherwise(df[f"pm_{engine}_factor_global"] * pm_calculation),
        )

    return df


def calculate_pm_2_5(df: DataFrame, var: dict) -> DataFrame:
    """
    Calculate PM 2.5 (Particle pollution from fine particulates, 2.5 micrometers or less) in ton based on PM10 (Particulate Matter, particles with a diameter of 10 micrometres or less)

    Parameters:
    -----------
    df: A PySpark DataFrame with added column for pm 2.5

    var: Dictionary
        A dictionary containing input variables for different emission factors

    Returns:
    --------
    A PySpark DataFrame with additional columns representing the PM2.5 emission (in ton) for each engine type
    """

    df = df.withColumn("pm2_5_ton", df["pm10_main_engine_ton"] * var["pm_2_5"])

    return df
