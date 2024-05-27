from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, row_number, when
from pyspark.sql.window import Window
from utilities.transformers.h3 import (
    H3_HEX_KRING_SHORE_POWER,
    H3_RESOLUTION_EMISSION_CONTROL_AREA,
    H3_RESOLUTION_MANAGEMENT_PLAN_MARINE_AREAS,
    H3_RESOLUTION_MARITIME_BORDERS,
    H3_RESOLUTION_MUNICIPALITY,
    H3_RESOLUTION_SHORE_POWER,
)


def get_areas():
    """
    Get a list of areas with their corresponding properties.

    Returns:
        list: A list of dictionaries representing the areas. Each dictionary contains the following keys:
            - area_name (str): The name of the area.
            - hex_resolution (int): The resolution of the hexagon grid for the area.
            - select_expression (list): A list of expressions to select from the area.
            - k_ring (bool): Indicates whether the area has k-rings.
            - window_id (str): The window ID for the area.
            - window_orderby_key (str): The window orderby key for the area.
    """
    areas = [
        {
            "area_name": "emission_control_area",
            "hex_resolution": H3_RESOLUTION_EMISSION_CONTROL_AREA,
            "select_expression": [
                "id",
                f"hex_{H3_RESOLUTION_EMISSION_CONTROL_AREA}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "emission_control_area_id",
        },
        {
            "area_name": "management_plan_marine_areas",
            "hex_resolution": H3_RESOLUTION_MANAGEMENT_PLAN_MARINE_AREAS,
            "select_expression": [
                "area_id",
                "area_name_norwegian",
                f"hex_{H3_RESOLUTION_MANAGEMENT_PLAN_MARINE_AREAS}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "management_plan_marine_areas_area_id",
        },
        {
            "area_name": "maritime_borders_norwegian_economic_zone",
            "hex_resolution": H3_RESOLUTION_MARITIME_BORDERS,
            "select_expression": [
                "id",
                "area_name",
                f"hex_{H3_RESOLUTION_MARITIME_BORDERS}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "maritime_borders_norwegian_economic_zone_id",
        },
        {
            "area_name": "maritime_borders_norwegian_continental_shelf",
            "hex_resolution": H3_RESOLUTION_MARITIME_BORDERS,
            "select_expression": [
                "id",
                f"hex_{H3_RESOLUTION_MARITIME_BORDERS}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "maritime_borders_norwegian_continental_shelf_id",
        },
        {
            "area_name": "municipality",
            "hex_resolution": H3_RESOLUTION_MUNICIPALITY,
            "select_expression": [
                "municipality_id as id",
                "municipality_name as name",
                f"hex_{H3_RESOLUTION_MUNICIPALITY}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "municipality_id",
        },
        {
            "area_name": "shore_power",
            "hex_resolution": H3_RESOLUTION_SHORE_POWER,
            "hex_krings": H3_HEX_KRING_SHORE_POWER,
            "select_expression": [
                f"hex_kring_{H3_RESOLUTION_SHORE_POWER}_{H3_HEX_KRING_SHORE_POWER}",
                f"hex_kring_distance_{H3_RESOLUTION_SHORE_POWER}_{H3_HEX_KRING_SHORE_POWER}",
            ],
            "k_ring": True,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": f"shore_power_hex_kring_distance_{H3_RESOLUTION_SHORE_POWER}_{H3_HEX_KRING_SHORE_POWER}",
        },
    ]
    return areas


def set_area_distance(df: DataFrame, close_to_power_cell: int) -> DataFrame:
    """
    Calculates distances between the AIS point and a set of areas based on H3 hexagonal grid coordinates.
    This function also creates columns to flag proximity or belonging to certain areas of interest.

    H3 resolution is given by the name of hex column, where the first digit refers to the H3 resolution and the second to the amount of k-rings. Ie. `coast_norway_hex_kring_distance_9_12` has H3 resolution 9 and 12 k-rings.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Input DataFrame with columns `coast_norway_hex_kring_distance_9_12`, `aquacultur_hex_kring_distance_9_2`,
        `oil_and_gas_hex_kring_distance_7_4`, `unlocode_hex_kring_distance_6_4`, and other related columns that contain
        environmental information such as area distances and landmarks.

    close_to_power_cell: int
        Maximum distance from a electric port for a vessel to be considered to be near one.

    Returns
    -------
    pyspark.sql.DataFrame
        A new DataFrame with additional columns:
        - `dist_to_XXX`: The distance between the input row and area `XXX` in H# hex cell's.
        - `close_to_XXX`: A flag indicating if the input row is close to area `XXX`.
        - `in_XXX`: A flag indicating if the input row belongs to area `XXX`.

    """
    df = (
        df.withColumn(
            "dist_to_power",
            coalesce(col("shore_power_hex_kring_distance_9_1"), lit(99)),
        )
        .withColumn(
            "close_to_power",
            when(col("dist_to_power") <= close_to_power_cell, True).otherwise(False),
        )
        .withColumn(
            "eca",
            when(col("emission_control_area_id").isNotNull(), True).otherwise(False),
        )
        .withColumn(
            "in_management_planning_area",
            when(
                col("management_plan_marine_areas_area_id").isNotNull(), True
            ).otherwise(False),
        )
        .withColumn(
            "in_norwegian_continental_shelf",
            when(
                col("maritime_borders_norwegian_continental_shelf_id").isNotNull(), True
            ).otherwise(False),
        )
    )

    return df


def join_ais_area(
    ais_df: DataFrame,
    area_df: DataFrame,
    join_key: str,
    window_id: str,
    window_orderby_key: str,
    h3_resolution: int,
) -> DataFrame:
    """
    Join AIS data with area data on a specified join key and window partitoning.
    """
    window_spec = Window.partitionBy(window_id).orderBy(window_orderby_key)

    df = (
        ais_df.join(
            area_df,
            on=(ais_df[f"hex_{h3_resolution}"] == area_df[join_key]),
            how="left",
        )
        .drop(area_df[join_key])
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    return df
