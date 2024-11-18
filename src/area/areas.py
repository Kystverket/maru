from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, row_number, when, year
from pyspark.sql.window import Window
from utilities.transformers.h3 import (
    H3_HEX_KRING_SHORE_POWER,
    H3_RESOLUTION_EMISSION_CONTROL_AREA,
    H3_RESOLUTION_MANAGEMENT_PLAN_MARINE_AREAS,
    H3_RESOLUTION_MARITIME_BORDERS,
    H3_RESOLUTION_MUNICIPALITY,
    H3_RESOLUTION_SHORE_POWER,
)
from utils.config import MUNICIPALITY_CURRENT_YEAR


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
            "table_name": "municipality_historical_h3",
            "hex_resolution": H3_RESOLUTION_MUNICIPALITY,
            "select_expression": [
                "municipality_id as id",
                "municipality_name_no as name",
                "year",
                f"hex_{H3_RESOLUTION_MUNICIPALITY}",
            ],
            "k_ring": False,
            "window_id": ["mmsi", "date_time_utc"],
            "window_orderby_key": "municipality_id",
            "filter_sql_expression": f"year = {MUNICIPALITY_CURRENT_YEAR}",
        },
        {
            "area_name": "shore_power",
            "hex_resolution": H3_RESOLUTION_SHORE_POWER,
            "hex_krings": H3_HEX_KRING_SHORE_POWER,
            "select_expression": [
                "bi_row_sha AS id",
                "year",
                "usage_percentage",
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
    name: str,
    window_id: str,
    window_orderby_key: str,
    h3_resolution: int,
    k_rings: int,
) -> DataFrame:
    """
    Join AIS data with area data based on H3 hexagonal grid coordinates and optional k-rings.

    This function performs a left join operation between an AIS DataFrame and an area DataFrame. The join is based on
    H3 hexagonal grid coordinates, with support for k-rings if specified. Additionally, it applies a window function
    to partition and order the data, ensuring that only the first row per partition is kept after the join.

    Parameters
    ----------
    ais_df : DataFrame
        The AIS DataFrame containing vessel locations and other relevant information.
    area_df : DataFrame
        The area DataFrame containing area information, including H3 hexagonal grid coordinates.
    name : str
        The name of the area, used to construct the join key and identify the correct H3 column in `area_df`.
    window_id : str
        The column name(s) used to partition the data for the window function.
    window_orderby_key : str
        The column name used to order data within each partition for the window function.
    h3_resolution : int
        The resolution of the H3 hexagonal grid used for the join.
    k_rings : int
        The number of k-rings to consider for the join. If `k_rings` is greater than 0, the join will use k-ring
        specific columns. If `k_rings` is 0, a direct H3 hexagonal grid coordinate join is performed.

    Returns
    -------
    DataFrame
        A DataFrame resulting from the left join of `ais_df` and `area_df`, with duplicates removed based on the
        window specification and only the first row per partition kept. The resulting DataFrame includes AIS data
        enriched with area information.
    """
    window_spec = Window.partitionBy(window_id).orderBy(window_orderby_key)

    # Set join key based on conditions

    if name == "shore_power":
        # Shore power joined with H3 and greater or equal to year to get only valid installations
        h3_join_column = f"{name}_hex_kring_{h3_resolution}_{k_rings}"
        join_key = (ais_df[f"hex_{h3_resolution}"] == area_df[h3_join_column]) & (
            year(ais_df["date_time_utc"]) >= area_df["shore_power_year"]
        )
    elif k_rings:
        # Join with H3 k-ring if exist
        h3_join_column = f"{name}_hex_kring_{h3_resolution}_{k_rings}"
        join_key = ais_df[f"hex_{h3_resolution}"] == area_df[h3_join_column]
    else:
        # Join with H3
        h3_join_column = f"{name}_hex_{h3_resolution}"
        join_key = ais_df[f"hex_{h3_resolution}"] == area_df[h3_join_column]

    df = (
        ais_df.join(
            area_df,
            on=join_key,
            how="left",
        )
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .drop(h3_join_column)
    )

    return df
