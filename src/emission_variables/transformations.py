from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, to_date


def change_data_types(df: DataFrame) -> DataFrame:
    """
    Change data types from string to float and dates.
    Filter the dataframe for date between df.start_date and df_end_date, so only active variables to be shown.
    The dataframe is also filtered on unique values, to account for duplicated data.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        A DataFrame containing variables and value.

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame containing variables and value.
    """

    # Change data types
    df = df.withColumn("value", regexp_replace("value", ",", ".").astype("double"))
    df = df.withColumn("start_date", to_date(df["start_date"], "dd.MM.yyyy"))
    df = df.withColumn("end_date", to_date(df["end_date"], "dd.MM.yyyy"))

    return df
