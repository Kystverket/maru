from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, lit


def total_emission(df: DataFrame, engines: list, emissions: list) -> DataFrame:
    """
    Sum main, aux, and boiler components into a total

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        input PySpark DataFrame with columns for each engine, component, and emission.

    engines: list[Str]
        list of strings with names of the engines, e.g. ['main_engine', 'aux', 'boiler']

    emissions: list[Str]
        list of strings with names of the emissions to calculate totals for, e.g. ['co2', 'nox', 'sox']

    Returns:
    --------
    pyspark.sql.DataFrame
        PySpark DataFrame with the same schema as the input DataFrame, with additional columns for total fuel usage and total emissions for the given engines and emissions.
    """

    for emission in emissions:
        columns_to_sum = [f"{emission}_{engine}_tonnes" for engine in engines]
        df = df.withColumn(
            f"{emission}_tonnes",
            sum(coalesce(df[col], lit(0)) for col in columns_to_sum),
        )

    df = df.withColumn(
        "fuel_tonnes",
        sum(coalesce(df[f"{engine}_fuel_tonnes"], lit(0)) for engine in engines),
    )

    return df
