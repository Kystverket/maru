from pyspark.sql import DataFrame


def select_maru_output_columns(df: DataFrame) -> DataFrame:
    """
    Selects a subset of columns from the input DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The DataFrame with the selected columns.
    """

    df = df.select(
        "id",
        "date_time_utc",
        "mmsi",
        "vessel_id",
        "phase",
        "sail_id",
        "geometry_wkt",
        "hex_14",
        "emission_control_area_id",
        "management_plan_marine_areas_area_id",
        "maritime_borders_norwegian_economic_zone_id",
        "maritime_borders_norwegian_continental_shelf_id",
        "shore_power_id",
        "municipality_id",
        "in_coast_and_sea_area",
        "electric_shore_power_at_berth_reduction_factor",
        "main_engine_load_factor",
        "main_engine_kwh",
        "aux_kwh",
        "boiler_kwh",
        "fuel_tonnes",
        "fuel_mdo_equivalent_tonnes",
        "bc_tonnes",
        "ch4_tonnes",
        "co_tonnes",
        "co2_tonnes",
        "n2o_tonnes",
        "nmvoc_tonnes",
        "nox_tonnes",
        "pm10_tonnes",
        "pm2_5_tonnes",
        "sox_tonnes",
        "co2e_tonnes",
        "delta_previous_point_seconds",
        "distance_previous_point_meters",
        "bi_processing_time",
        "version",
    )

    return df


def select_vessel_columns(df: DataFrame) -> DataFrame:
    """
    Selects a subset of columns from the input DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The DataFrame with the selected columns.
    """

    df = df.select(
        "vessel_id",
        "vessel_type_maru",
        "main_engine_fueltype",
        "main_engine_enginetype",
        "year_of_build",
        "main_engine_rpm",
        "speed_service",
        "stroketype",
        "degree_of_electrification",
        "main_engine_kw",
        "aux_berth_kw",
        "aux_anchor_kw",
        "aux_maneuver_kw",
        "aux_cruise_kw",
        "boiler_berth_kw",
        "boiler_anchor_kw",
        "boiler_maneuver_kw",
        "boiler_cruise_kw",
        "cf_eca",
        "cf_global",
        "sfc_main_engine_baseline",
        "sfc_aux_baseline",
        "sfc_boiler_baseline",
        "sfc_pilot_fuel",
        "fuel_sulfur_fraction_global",
        "fuel_sulfur_fraction_eca",
        "bc_aux_factor_global",
        "bc_aux_factor_eca",
        "bc_boiler_factor_global",
        "bc_boiler_factor_eca",
        "ch4_main_engine_factor",
        "ch4_aux_factor",
        "ch4_boiler_factor",
        "co_main_engine_factor_global",
        "co_main_engine_factor_eca",
        "co_aux_factor_global",
        "co_aux_factor_eca",
        "co_boiler_factor_global",
        "co_boiler_factor_eca",
        "n2o_main_engine_factor_global",
        "n2o_main_engine_factor_eca",
        "n2o_aux_factor_global",
        "n2o_aux_factor_eca",
        "n2o_boiler_factor_global",
        "n2o_boiler_factor_eca",
        "nmvoc_main_engine_factor",
        "nmvoc_aux_factor",
        "nmvoc_boiler_factor",
        "nox_main_engine_factor_global",
        "nox_main_engine_factor_eca",
        "nox_aux_factor_global",
        "nox_aux_factor_eca",
        "nox_boiler_factor_global",
        "nox_boiler_factor_eca",
        "sox_main_engine_global_factor",
        "sox_aux_global_factor",
        "sox_boiler_global_factor",
        "sox_main_engine_eca_factor",
        "sox_aux_eca_factor",
        "sox_boiler_eca_factor",
        "start_date",
        "end_date",
    )

    return df
