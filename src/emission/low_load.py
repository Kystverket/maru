from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when


def low_load(df: DataFrame, var: dict) -> DataFrame:
    """
    Assign a low load factor to emissions when maine engine load factor (main_engine_load_factor) is below 20%.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        The PySpark DataFrame containing the different emission columns (pm10_main_engine_ton, nox_main_engine_ton ect.)

    var: dict
        A dictionary containing input variables for different
        emission factors.

    Returns:
    --------
    pyspark.sql.DataFrame
        The updated PySpark DataFrame with low load factors (pm10_main_engine_low_load_factor, nox_main_engine_low_load_factor etc.) and adjusted emission columns (pm10_main_engine_ton, nox_main_engine_ton ect.)
    """

    emission_low_load_columns = [
        "pm10_main_engine",
        "nox_main_engine",
        "co_main_engine",
        "ch4_main_engine",
        "nmvoc_main_engine",
        "n2o_main_engine",
    ]

    # Assign low load factor to emission columns
    for column_name in emission_low_load_columns:
        df = df.withColumn(
            f"{column_name}_low_load_factor",
            when(
                (df["main_engine_load_factor"] > 0.0001)
                & (df["main_engine_load_factor"] <= 0.02),
                var[f"low_load_{column_name}_00001_002"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.02)
                & (df["main_engine_load_factor"] <= 0.03),
                var[f"low_load_{column_name}_002_003"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.03)
                & (df["main_engine_load_factor"] <= 0.04),
                var[f"low_load_{column_name}_003_004"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.04)
                & (df["main_engine_load_factor"] <= 0.05),
                var[f"low_load_{column_name}_004_005"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.05)
                & (df["main_engine_load_factor"] <= 0.06),
                var[f"low_load_{column_name}_005_006"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.06)
                & (df["main_engine_load_factor"] <= 0.07),
                var[f"low_load_{column_name}_006_007"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.07)
                & (df["main_engine_load_factor"] <= 0.08),
                var[f"low_load_{column_name}_007_008"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.08)
                & (df["main_engine_load_factor"] <= 0.09),
                var[f"low_load_{column_name}_008_009"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.09)
                & (df["main_engine_load_factor"] <= 0.1),
                var[f"low_load_{column_name}_009_01"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.10)
                & (df["main_engine_load_factor"] <= 0.11),
                var[f"low_load_{column_name}_01_011"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.11)
                & (df["main_engine_load_factor"] <= 0.12),
                var[f"low_load_{column_name}_011_012"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.12)
                & (df["main_engine_load_factor"] <= 0.13),
                var[f"low_load_{column_name}_012_013"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.13)
                & (df["main_engine_load_factor"] <= 0.14),
                var[f"low_load_{column_name}_013_014"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.14)
                & (df["main_engine_load_factor"] <= 0.15),
                var[f"low_load_{column_name}_014_015"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.15)
                & (df["main_engine_load_factor"] <= 0.16),
                var[f"low_load_{column_name}_015_016"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.16)
                & (df["main_engine_load_factor"] <= 0.17),
                var[f"low_load_{column_name}_016_017"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.17)
                & (df["main_engine_load_factor"] <= 0.18),
                var[f"low_load_{column_name}_017_018"],
            )
            .when(
                (df["main_engine_load_factor"] > 0.18)
                & (df["main_engine_load_factor"] <= 0.19),
                var[f"low_load_{column_name}_018_019"],
            )
            .otherwise(lit(1)),
        )

        # Apply low load factor to emission
        df = df.withColumn(
            f"{column_name}_ton",
            df[f"{column_name}_low_load_factor"] * df[f"{column_name}_ton"],
        )

    return df
