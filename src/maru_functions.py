from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

from consumption.fuel import calculate_fuel_consumption, calculate_fuel_equivalent
from consumption.sfc import (
    calculate_sfc_load,
    set_sfc_aux_baseline,
    set_sfc_boiler_baseline,
    set_sfc_main_engine_baseline,
    set_sfc_pilot_fuel,
)
from emission.bc import (
    calculate_bc_aux_boiler,
    calculate_bc_main_engine,
    set_bc_aux_boiler_factor,
    set_main_engine_bc_factor,
)
from emission.ch4 import (
    calculate_ch4,
    set_ch4_aux_boiler_factor,
    set_ch4_main_engine_factor,
)
from emission.co import (
    calculate_co,
    set_co_aux_boiler_factor,
    set_co_main_engine_factor,
)
from emission.co2 import calculate_co2, set_cf
from emission.fsf import set_fuel_sulfur_fraction_eca, set_fuel_sulfur_fraction_global
from emission.n2o import (
    calculate_n2o,
    set_n2o_aux_boiler_factor,
    set_n2o_main_engine_factor,
)
from emission.nmvoc import (
    calculate_nmvoc,
    set_nmvoc_aux_factor,
    set_nmvoc_boiler_factor,
    set_nmvoc_main_engine_factor,
)
from emission.nox import (
    calculate_nox,
    set_nox_aux_boiler_factor,
    set_nox_main_engine_eca_factor,
    set_nox_main_engine_factor,
)
from emission.pm import calculate_pm_2_5, calculate_pm_10, set_pm_factor
from emission.sox import calculate_sox
from pyspark.sql import DataFrame


def calculate_fuel(df: DataFrame, variables: dict, engines: list) -> DataFrame:
    """Calculates fuel consumption in tonnes based on engine types and fuel factors.

    Args:
        df (DataFrame): A PySpark DataFrame containing relevant input data.
        variables (dict): Dictionary with fuel equivalent factors for different engine types.
        engines (list): List of engine types to include in the calculation (e.g., 'me', 'aux', 'boiler').

    Returns:
        DataFrame: A PySpark DataFrame with calculated fuel consumption.
    """
    # Spesific fuel consumption
    df = calculate_sfc_load(df)

    # Fuel consumption
    df = calculate_fuel_consumption(df, engines)

    # Fuel MDO equivalents
    df = calculate_fuel_equivalent(df, variables, engines)

    return df


def calculate_emission(
    df: DataFrame,
    emission_factors: dict,
    engines: list,
    engines_aux_bc: list,
    areas: list,
) -> DataFrame:
    """Calculates emissions based on engine types, emission factors, and operational areas.

    Args:
        df (DataFrame): A PySpark DataFrame containing operational and fuel data.
        emission_factors (dict): Dictionary with emission factors for different pollutants and engine types.
        engines (list): List of engine types to include in the calculation (e.g., 'main_engine', 'aux', 'boiler').
        engines_aux_bc (list): List of auxiliary engine and boiler types relevant for black carbon calculations.
        areas (list): List of operational areas (e.g., 'eca', 'global').

    Returns:
        DataFrame: A PySpark DataFrame with calculated emissions.
    """
    # bc
    df = set_main_engine_bc_factor(df, emission_factors)
    df = calculate_bc_main_engine(df)
    df = calculate_bc_aux_boiler(df, engines_aux_bc)

    # ch4
    df = calculate_ch4(df, engines)

    # co2
    df = calculate_co2(df, engines)

    # co
    df = calculate_co(df, engines)

    # n2o
    df = calculate_n2o(df, engines)

    # nmvoc
    df = calculate_nmvoc(df, engines)

    # nox
    df = calculate_nox(df, engines)

    # pm
    df = set_pm_factor(df, emission_factors, engines, areas)
    df = calculate_pm_10(df, engines)
    df = calculate_pm_2_5(df, emission_factors)

    # sox
    df = calculate_sox(df, engines)

    return df


def set_emission_factors(
    df: DataFrame, dict_variables: dict, engines_aux_boiler: list
) -> DataFrame:
    """Applies a series of emission-related factors to a vessel DataFrame.

    This function updates the input DataFrame with emission factors for
    black carbon (BC), carbon conversion (CF), methane (CH4), carbon monoxide (CO),
    fuel sulfur fraction (FSF), nitrous oxide (N2O), non-methane VOCs (NMVOC),
    and nitrogen oxides (NOx) based on engine type and other parameters.

    Args:
        df (DataFrame): Filtered vessel data to which emission factors will be applied.
        dict_variables (dict): Dictionary containing necessary variables and parameters for the factor functions.
        engines_aux_boiler (list): List of engine types to treat as auxiliary or boiler (e.g., ["aux", "boiler"]).

    Returns:
        DataFrame: A PySpark DataFrame with set emission factors.
    """
    df = set_bc_aux_boiler_factor(df, dict_variables, engines_aux_boiler)

    # Carbon conversion factors (cf)
    df = set_cf(df, dict_variables)

    # ch4
    df = set_ch4_main_engine_factor(df, dict_variables)
    df = set_ch4_aux_boiler_factor(df, dict_variables, engines_aux_boiler)

    # co
    df = set_co_main_engine_factor(df, dict_variables)
    df = set_co_aux_boiler_factor(df, dict_variables)

    # Fuel sulfur fraction (fsf)
    df = set_fuel_sulfur_fraction_global(df, dict_variables)
    df = set_fuel_sulfur_fraction_eca(df, dict_variables)

    # n2o
    df = set_n2o_main_engine_factor(df, dict_variables)
    df = set_n2o_aux_boiler_factor(df, dict_variables)

    # nmvoc
    df = set_nmvoc_main_engine_factor(df, dict_variables)
    df = set_nmvoc_aux_factor(df, dict_variables)
    df = set_nmvoc_boiler_factor(df, dict_variables)

    # nox
    df = set_nox_main_engine_factor(df, dict_variables)
    df = set_nox_main_engine_eca_factor(df, dict_variables)
    df = set_nox_aux_boiler_factor(df, dict_variables, engines_aux_boiler)

    return df


def set_consumption_factors(df: DataFrame, dict_variables: dict) -> DataFrame:
    """Applies a series of fuel consumption (SFC) functions to a DataFrame.

    Args:
        df (pd.DataFrame): Vessel DataFrame to be updated with SFC factors.
        dict_variables (dict): Dictionary containing relevant variables and parameters for SFC calculations.

    Returns:
        DataFrame: A PySpark DataFrame with added SFC-related columns.
    """
    df = set_sfc_main_engine_baseline(df, dict_variables)
    df = set_sfc_pilot_fuel(df, dict_variables)
    df = set_sfc_aux_baseline(df, dict_variables)
    df = set_sfc_boiler_baseline(df, dict_variables)
    return df


def create_schema_and_table_if_not_exists(env: str, target_table: str) -> None:
    """Creates the target schema and table in Spark if they do not already exist.

    Args:
        env (str): Environment name (e.g., "dev", "prod") used to construct the schema name.
        target_table (str): Fully qualified table name (including schema) to be created.

    Returns:
        None
    """
    spark.sql(
        f"""
          CREATE SCHEMA IF NOT EXISTS gold_{env}.maru
          """
    )
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                vessel_id INT COMMENT 'Identifier for the vessel',
                mmsi INT COMMENT 'Maritime Mobile Service Identity',
                imo INT COMMENT 'International Maritime Organization number - a unique ship identifier',
                ship_name STRING COMMENT 'Name of the ship',
                call_sign STRING COMMENT 'Call sign of the ship',
                date_time_utc TIMESTAMP COMMENT 'Date and time in UTC',
                date_utc DATE COMMENT 'Date in UTC',
                phase STRING COMMENT 'Current phase of the vessel',
                sail_id BIGINT COMMENT 'Sail ID',
                geometry_wkt STRING COMMENT 'Geometry in Well-Known Text format',
                hex_14 BIGINT COMMENT 'Hexadecimal representation',
                degree_of_electrification DOUBLE COMMENT 'Degree of electrification',
                usage_percentage_vessel_port_electric DOUBLE COMMENT 'Percentage of shore power usage',
                emission_control_area_id STRING COMMENT 'Emission control area identifier',
                management_plan_marine_areas_area_id INT COMMENT 'Management plan marine areas area identifier',
                maritime_borders_norwegian_economic_zone_id STRING COMMENT 'Norwegian economic zone identifier',
                maritime_borders_norwegian_continental_shelf_id STRING COMMENT 'Norwegian continental shelf identifier',
                municipality_id STRING COMMENT 'Municipality identifier',
                in_coast_and_sea_area BOOLEAN COMMENT 'Indicator if in coast and sea area',
                shore_power_is_connected BOOLEAN COMMENT 'Indicator if the vessel is connected and utilizing shore power',
                shore_power_id STRING COMMENT 'Shore power identifier',
                electric_shore_power_at_berth_reduction_factor DOUBLE COMMENT 'Shore power reduction factor',
                main_engine_load_factor DOUBLE COMMENT 'Main engine load factor',
                main_engine_kwh DOUBLE COMMENT 'Main engine kWh',
                aux_kwh DOUBLE COMMENT 'Auxiliary engine kWh',
                boiler_kwh DOUBLE COMMENT 'Boiler kWh',
                fuel_main_aux_boiler_engines_tonnes DOUBLE COMMENT 'Fuel from main engine, aux and boiler in tonnes',
                fuel_mdo_equivalent_tonnes DOUBLE COMMENT 'Fuel MDO (Marine diesel oil) equivalent in tonnes',
                bc_tonnes DOUBLE COMMENT 'Black carbon in tonnes',
                ch4_tonnes DOUBLE COMMENT 'Methane in tonnes',
                co_tonnes DOUBLE COMMENT 'Carbon monoxide in tonnes',
                co2_tonnes DOUBLE COMMENT 'Carbon dioxide in tonnes',
                n2o_tonnes DOUBLE COMMENT 'Nitrous oxide in tonnes',
                nmvoc_tonnes DOUBLE COMMENT 'Non-methane volatile organic compounds in tonnes',
                nox_tonnes DOUBLE COMMENT 'Nitrogen oxides in tonnes',
                pm10_tonnes DOUBLE COMMENT 'Particulate matter (with a diameter of 10 microns or less) in tonnes',
                pm2_5_tonnes DOUBLE COMMENT 'Particulate matter (with a diameter of 2.5 microns or less) in tonnes',
                sox_tonnes DOUBLE COMMENT 'Sulfur oxides in tonnes',
                co2e_tonnes DOUBLE COMMENT 'CO2 equivalent in tonnes',
                delta_previous_point_seconds INT COMMENT 'Seconds from previous AIS point',
                distance_previous_point_meters FLOAT COMMENT 'Distance in meters from previous AIS point',
                bi_processing_time TIMESTAMP COMMENT 'Last processing time',
                version STRING COMMENT 'MarU version number'
            )
            CLUSTER BY(date_time_utc, mmsi, municipality_id, phase)
            COMMENT 'MarU raw data'
    """
    )
